import json
import math
import threading
import time
from collections import deque

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# BITCOIN RADAR LIVE - COINBASE
# Datos en vivo, velas de 1 minuto, EMA 9/21, RSI, VWAP,
# fuerza compradora/vendedora y prediccion separada a 15 minutos.
# La prediccion es una estimacion tecnica, no una garantia.
# ============================================================

PRODUCT_ID = "BTC-USD"
REST_URL = "https://api.exchange.coinbase.com"
WS_URL = "wss://advanced-trade-ws.coinbase.com"

MAX_CANDLES = 300
STALE_SECONDS = 20
PREDICTION_HORIZON = 15
lock = threading.RLock()

candles = deque(maxlen=MAX_CANDLES)
ticks = deque(maxlen=1200)
trade_flow = deque(maxlen=5000)
current_price = None
last_update = 0.0
last_error = ""
last_source = "NINGUNA"
ws_connected = False
services_started = False
session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarLive/4.0", "Accept": "application/json"})


def ema(values, period):
    values = [float(v) for v in values]
    if not values:
        return None
    alpha = 2.0 / (period + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def rsi(values, period=14):
    values = [float(v) for v in values]
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(len(values) - period, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def vwap_from_candles(history):
    rows = [c for c in history if float(c.get("volume", 0) or 0) > 0]
    if not rows:
        return None
    volume_sum = sum(float(c["volume"]) for c in rows)
    if volume_sum <= 0:
        return None
    weighted = sum(
        ((float(c["high"]) + float(c["low"]) + float(c["close"])) / 3.0)
        * float(c["volume"]) for c in rows
    )
    return weighted / volume_sum


def update_price(price, source="WEBSOCKET"):
    global current_price, last_update, last_source
    price = float(price)
    if not math.isfinite(price) or price <= 0:
        return
    now = time.time()
    bucket = int(now // 60) * 60
    with lock:
        current_price = price
        last_update = now
        last_source = source
        ticks.append({"time": now, "price": price})
        if candles and int(candles[-1]["time"]) == bucket:
            c = candles[-1]
            c["high"] = max(float(c["high"]), price)
            c["low"] = min(float(c["low"]), price)
            c["close"] = price
        else:
            candles.append({
                "time": bucket, "open": price, "high": price,
                "low": price, "close": price, "volume": 0.0
            })


def record_trade(side, size, price=None):
    now = time.time()
    side = str(side or "").upper()
    try:
        size = float(size)
    except (TypeError, ValueError):
        return
    if size <= 0 or side not in ("BUY", "SELL"):
        return
    with lock:
        trade_flow.append({"time": now, "side": side, "size": size})
        if price is not None and candles:
            bucket = int(now // 60) * 60
            if int(candles[-1]["time"]) == bucket:
                candles[-1]["volume"] = float(candles[-1].get("volume", 0)) + size


def load_history():
    global last_error
    try:
        response = session.get(
            f"{REST_URL}/products/{PRODUCT_ID}/candles",
            params={"granularity": 60},
            timeout=15
        )
        response.raise_for_status()
        history = []
        for item in response.json():
            if len(item) < 6:
                continue
            history.append({
                "time": int(item[0]), "low": float(item[1]),
                "high": float(item[2]), "open": float(item[3]),
                "close": float(item[4]), "volume": float(item[5])
            })
        history.sort(key=lambda x: x["time"])
        with lock:
            existing = {int(c["time"]): dict(c) for c in history}
            for c in candles:
                existing[int(c["time"])] = dict(c)
            merged = sorted(existing.values(), key=lambda x: x["time"])
            candles.clear()
            candles.extend(merged[-MAX_CANDLES:])
            last_error = ""
    except Exception as exc:
        with lock:
            last_error = "Historial Coinbase: " + str(exc)


def force_coinbase_update():
    global last_error
    try:
        response = session.get(f"{REST_URL}/products/{PRODUCT_ID}/ticker", timeout=10)
        response.raise_for_status()
        update_price(float(response.json()["price"]), "REST MANUAL")
        with lock:
            last_error = ""
        return True
    except Exception as exc:
        with lock:
            last_error = "Actualizacion manual: " + str(exc)
        return False


def on_open(ws):
    global ws_connected, last_error
    try:
        # Dos canales: ticker para precio y market_trades para flujo comprador/vendedor.
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channel": "ticker"
        }))
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channel": "market_trades"
        }))
        with lock:
            ws_connected = True
            last_error = ""
    except Exception as exc:
        with lock:
            ws_connected = False
            last_error = "Suscripcion Coinbase: " + str(exc)


def on_message(ws, message):
    global last_error
    try:
        data = json.loads(message)
        channel = data.get("channel")
        if channel == "ticker":
            for event in data.get("events", []):
                for ticker in event.get("tickers", []):
                    if ticker.get("product_id") == PRODUCT_ID and ticker.get("price") is not None:
                        update_price(ticker["price"], "WEBSOCKET")
        elif channel == "market_trades":
            for event in data.get("events", []):
                for trade in event.get("trades", []):
                    if trade.get("product_id") == PRODUCT_ID:
                        record_trade(
                            trade.get("side"),
                            trade.get("size"),
                            trade.get("price")
                        )
        with lock:
            last_error = ""
    except Exception as exc:
        with lock:
            last_error = "Mensaje WebSocket: " + str(exc)


def on_error(ws, error):
    global ws_connected, last_error
    with lock:
        ws_connected = False
        last_error = "WebSocket: " + str(error)


def on_close(ws, close_status_code, close_msg):
    global ws_connected
    with lock:
        ws_connected = False


def websocket_worker():
    global ws_connected, last_error
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            with lock:
                last_error = "Reconectando WebSocket: " + str(exc)
                ws_connected = False
        with lock:
            ws_connected = False
        time.sleep(5)


def rest_fallback_worker():
    global last_error
    while True:
        try:
            with lock:
                updated = last_update
            if time.time() - updated > 8:
                response = session.get(f"{REST_URL}/products/{PRODUCT_ID}/ticker", timeout=10)
                response.raise_for_status()
                update_price(float(response.json()["price"]), "REST RESPALDO")
        except Exception as exc:
            with lock:
                last_error = "Respaldo REST: " + str(exc)
        time.sleep(5)


def start_background_services():
    global services_started
    with lock:
        if services_started:
            return
        services_started = True
    threading.Thread(target=load_history, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    threading.Thread(target=rest_fallback_worker, daemon=True).start()


def momentum_pct(price, ticks_snapshot, seconds, now):
    target = now - seconds
    old = next((t for t in reversed(ticks_snapshot) if float(t["time"]) <= target), None)
    if old is None or target - float(old["time"]) > 4:
        return None
    old_price = float(old["price"])
    return ((price / old_price) - 1.0) * 100.0 if old_price > 0 else None


def calculate_indicators(price, history, ticks_snapshot, flow_snapshot):
    closes = [float(c["close"]) for c in history]
    ema9 = ema(closes[-60:], 9) if len(closes) >= 9 else None
    ema21 = ema(closes[-100:], 21) if len(closes) >= 21 else None
    rsi_value = rsi(closes, 14)
    vwap_value = vwap_from_candles(history)
    now = time.time()
    m5 = momentum_pct(price, ticks_snapshot, 5, now)
    m15 = momentum_pct(price, ticks_snapshot, 15, now)
    m30 = momentum_pct(price, ticks_snapshot, 30, now)

    flow_cutoff = now - 60
    recent_flow = [t for t in flow_snapshot if float(t["time"]) >= flow_cutoff]
    buy_volume = sum(float(t["size"]) for t in recent_flow if t["side"] == "BUY")
    sell_volume = sum(float(t["size"]) for t in recent_flow if t["side"] == "SELL")
    total_flow = buy_volume + sell_volume
    buy_pct = (buy_volume / total_flow * 100.0) if total_flow else None
    sell_pct = (sell_volume / total_flow * 100.0) if total_flow else None

    score = 0.0
    if ema9 is not None and ema21 is not None:
        score += 2 if ema9 > ema21 else -2 if ema9 < ema21 else 0
    if vwap_value is not None:
        score += 1 if price > vwap_value else -1 if price < vwap_value else 0
    if rsi_value is not None:
        score += 1 if rsi_value >= 55 else -1 if rsi_value <= 45 else 0
    for value, weight in ((m5, 1.5), (m15, 2.0), (m30, 1.0)):
        if value is not None:
            score += weight if value > 0 else -weight if value < 0 else 0
    if buy_pct is not None:
        score += 1 if buy_pct >= 55 else -1 if buy_pct <= 45 else 0

    if score >= 1.5:
        direction, signal = "ALCISTA", "SUBE"
    elif score <= -1.5:
        direction, signal = "BAJISTA", "BAJA"
    else:
        direction, signal = "NEUTRAL", "ESPERAR"

    # Proyeccion limitada y basada en momentum observado; no es garantia.
    usable = [v for v in (m5, m15, m30) if v is not None]
    short_change = (sum(usable) / len(usable) / 100.0) if usable else 0.0
    projected_change = max(-0.0025, min(0.0025, short_change * 3.0))
    projected_price = price * (1.0 + projected_change)

    # La confianza mostrada es fuerza relativa del puntaje, no probabilidad calibrada.
    confidence = min(90, int(50 + abs(score) * 6))
    return {
        "direction": direction, "signal": signal, "score": round(score, 2),
        "ema9": round(ema9, 2) if ema9 is not None else None,
        "ema21": round(ema21, 2) if ema21 is not None else None,
        
        "rsi": round(rsi_value, 2) if rsi_value is not None else None,
        "vwap": round(vwap_value, 2) if vwap_value is not None else None,
        "buy_pct": round(buy_pct, 1) if buy_pct is not None else None,
        "sell_pct": round(sell_pct, 1) if sell_pct is not None else None,
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "momentum_5s_pct": round(m5, 5) if m5 is not None else None,
        "momentum_15s_pct": round(m15, 5) if m15 is not None else None,
        "momentum_30s_pct": round(m30, 5) if m30 is not None else None,
        "prediction": round(projected_price, 2),
        "confidence": confidence
    }


def get_api_data():
    with lock:
        price = current_price
        history = [dict(c) for c in candles]
        tick_snapshot = [dict(t) for t in ticks]
        flow_snapshot = [dict(t) for t in trade_flow]
        updated = last_update
        error = last_error
        source = last_source
        socket_status = ws_connected

    now = time.time()
    live = bool(price is not None and updated > 0 and now - updated <= STALE_SECONDS)
    countdown = 900 - (int(now) % 900)
    if price is None:
        return {
            "ok": False, "connected": False, "status": "ESPERANDO COINBASE",
            "price": None, "candles": history, "countdown": countdown, "error": error
        }

    indicators = calculate_indicators(price, history, tick_snapshot, flow_snapshot)
    return {
        "ok": True, "connected": live, "websocket_connected": socket_status,
        "status": "EN VIVO" if live else "SIN DATOS RECIENTES",
        "price": round(price, 2), "candles": history[-MAX_CANDLES:],
        "countdown": countdown, "last_update": updated, "source": source,
        "error": error, **indicators
    }


@app.route("/")
def home():
    start_background_services()
    return render_template_string(HTML)


@app.route("/api")
def api():
    start_background_services()
    return jsonify(get_api_data())


@app.route("/refresh")
def refresh():
    start_background_services()
    success = force_coinbase_update()
    data = get_api_data()
    data["manual_refresh"] = success
    return jsonify(data)


@app.route("/health")
def health():
    with lock:
        price, updated, error = current_price, last_update, last_error
        source, count, socket_status = last_source, len(candles), ws_connected
    live = bool(price is not None and updated > 0 and time.time() - updated <= STALE_SECONDS)
    return jsonify({
        "ok": True, "coinbase_live": live, "websocket_connected": socket_status,
        "price": price, "last_update": updated, "source": source,
        "candles": count, "error": error
    })


# ============================================================
# INTERFAZ
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar Live</title>
<style>
*{box-sizing:border-box}
body{margin:0;padding:14px;background:#080d18;color:#edf2ff;font-family:Arial,sans-serif}
main{max-width:1000px;margin:auto}
.header{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:14px}
h1{font-size:20px;margin:0;letter-spacing:.5px}
.status{font-size:11px;color:#ffbd59;font-weight:bold}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.card{background:#111a2b;border:1px solid #263650;border-radius:13px;padding:14px;margin-bottom:10px;min-width:0}
.label{font-size:11px;letter-spacing:.8px;color:#91a2c1}
.price{font-size:clamp(27px,6vw,38px);font-weight:700;margin:7px 0}
.value{font-size:18px;font-weight:700;margin-top:6px;overflow-wrap:anywhere}
.signal{font-size:28px;font-weight:800;margin:8px 0}
.up{color:#22d69a}.down{color:#ff6475}.neutral{color:#f4c76b}
.small{font-size:11px;line-height:1.6;color:#91a2c1}
.row{display:flex;justify-content:space-between;align-items:center;gap:10px}
.metric{padding:10px;background:#0b1322;border-radius:9px;min-width:0}
.metric .value{font-size:15px}
button{border:0;border-radius:8px;padding:9px 12px;color:#fff;background:#294a83;font-weight:bold}
button:disabled{opacity:.5}
.controls{display:flex;gap:7px;flex-wrap:wrap;margin:12px 0}
.controls button{min-width:44px}
canvas{width:100%;height:330px;display:block;touch-action:none}
.bar{height:9px;border-radius:8px;background:#26334e;overflow:hidden;display:flex;margin:9px 0}
.buybar{background:#22d69a}.sellbar{background:#ff6475}
.prediction{font-size:25px;font-weight:800;margin:7px 0}
.tag{display:inline-block;padding:4px 7px;border-radius:6px;background:#202d45;color:#bdcbea;font-size:10px}
@media(max-width:600px){.grid{grid-template-columns:1fr}.header{align-items:flex-start}.card{padding:12px}canvas{height:280px}}
</style>
</head>
<body>
<main>
<div class="header">
  <h1>BITCOIN RADAR LIVE</h1>
  <div id="status" class="status">CONECTANDO...</div>
</div>

<section class="card">
  <div class="label">BITCOIN / USD · COINBASE</div>
  <div id="price" class="price">Cargando...</div>
  <div class="row"><span class="small">Precio recibido de Coinbase</span><span id="source" class="tag">SIN FUENTE</span></div>
  <div id="tickMomentum" class="small">Impulso: esperando datos</div>
</section>

<div class="grid">
  <section class="card">
    <div class="label">RADAR ACTUAL · ANÁLISIS TÉCNICO</div>
    <div id="direction" class="signal neutral">ESPERANDO</div>
    <div class="row">
      <div><div class="label">SEÑAL</div><div id="signal" class="value">ESPERAR</div></div>
      <div><div class="label">FUERZA TÉCNICA</div><div id="confidence" class="value">--</div></div>
    </div>
    <div class="small">Describe las condiciones actuales; no garantiza el próximo movimiento.</div>
  </section>

  <section class="card">
    <div class="label">PREDICCIÓN AL CIERRE · 15 MIN</div>
    <div id="predDirection" class="signal neutral">ESPERANDO</div>
    <div class="prediction" id="prediction">--</div>
    <div class="row"><span class="label">ESTIMACIÓN TÉCNICA</span><span id="predStrength" class="value">--</span></div>
    <div class="small">Proyección experimental basada en indicadores y momentum. No es una probabilidad estadística validada.</div>
  </section>
</div>

<section class="card">
  <div class="label">INDICADORES EN VIVO</div>
  <div class="grid" style="margin-top:10px">
    <div class="metric"><div class="label">EMA 9</div><div id="ema9" class="value">--</div></div>
    <div class="metric"><div class="label">EMA 21</div><div id="ema21" class="value">--</div></div>
    <div class="metric"><div class="label">RSI 14</div><div id="rsi" class="value">--</div></div>
    <div class="metric"><div class="label">VWAP</div><div id="vwap" class="value">--</div></div>
  </div>
</section>

<section class="card">
  <div class="label">FUERZA COMPRADORA / VENDEDORA · ÚLTIMOS 60 SEGUNDOS</div>
  <div class="bar"><div id="buyBar" class="buybar" style="width:50%"></div><div id="sellBar" class="sellbar" style="width:50%"></div></div>
  <div class="row">
    <div><div class="label">COMPRADORES</div><div id="buyPct" class="value up">--</div></div>
    <div style="text-align:right"><div class="label">VENDEDORES</div><div id="sellPct" class="value down">--</div></div>
  </div>
  <div id="flowNote" class="small">Esperando operaciones de Coinbase.</div>
</section>

<section class="card">
  <div class="row"><div class="label">VELAS DE 1 MINUTO · EMA 9 / EMA 21</div><button id="refreshButton" onclick="manualRefresh()">ACTUALIZAR</button></div>
  <div class="controls">
    <button onclick="zoomIn()">+</button><button onclick="zoomOut()">−</button>
    <button onclick="resetZoom()">AJUSTAR</button><button onclick="goLive()">EN VIVO</button>
  </div>
  <canvas id="chart" width="1000" height="430"></canvas>
  <div class="small">Desliza para revisar velas anteriores. Usa dos dedos para acercar o alejar.</div>
</section>

<section class="card">
  <div class="row"><div><div class="label">CIERRE DEL BLOQUE ACTUAL</div><div id="countdown" class="prediction">15:00</div></div>
    <div><div class="label">ESTADO WEBSOCKET</div><div id="socket" class="value">COMPROBANDO</div></div>
  </div>
  <div id="updated" class="small">Esperando actualización...</div>
  <div id="error" class="small"></div>
</section>
</main>

<script>
const $ = id => document.getElementById(id);
const money = v => v === null || v === undefined ? "--" :
  Number(v).toLocaleString("en-US",{style:"currency",currency:"USD",maximumFractionDigits:2});
const pct = v => v === null || v === undefined ? "--" : Number(v).toFixed(1)+"%";
const fmtMomentum = v => v === null || v === undefined ? "--" :
  (Number(v)>0?"+":"")+Number(v).toFixed(5)+"%";
function formatTime(seconds){
  const m=Math.floor(seconds/60),s=seconds%60;
  return String(m).padStart(2,"0")+":"+String(s).padStart(2,"0");
}

let chartData=[],zoomLevel=1,chartOffset=0;
function zoomIn(){zoomLevel=Math.min(8,zoomLevel*1.3);drawChart(chartData)}
function zoomOut(){zoomLevel=Math.max(.5,zoomLevel/1.3);drawChart(chartData)}
function resetZoom(){zoomLevel=1;chartOffset=0;drawChart(chartData)}
function goLive(){chartOffset=0;drawChart(chartData)}

function calcEmaSeries(values,period){
  if(!values.length)return [];
  const a=2/(period+1);let v=values[0];const out=[v];
  for(let i=1;i<values.length;i++){v=a*values[i]+(1-a)*v;out.push(v)}
  return out;
}
function drawChart(data){
  chartData=data||[];
  const canvas=$("chart"),ctx=canvas.getContext("2d");
  const W=canvas.width,H=canvas.height;
  ctx.clearRect(0,0,W,H);
  if(!chartData.length){ctx.fillStyle="#91a2c1";ctx.fillText("Esperando velas...",25,35);return}
  const count=Math.max(12,Math.min(chartData.length,Math.floor(100/zoomLevel)));
  const maxOffset=Math.max(0,chartData.length-count);
  chartOffset=Math.max(0,Math.min(chartOffset,maxOffset));
  const end=chartData.length-chartOffset,start=Math.max(0,end-count);
  const visible=chartData.slice(start,end);
  const closes=visible.map(c=>Number(c.close));
  const e9=calcEmaSeries(closes,9),e21=calcEmaSeries(closes,21);
  const vals=visible.flatMap(c=>[Number(c.high),Number(c.low)]).concat(e9,e21);
  let min=Math.min(...vals),max=Math.max(...vals);
  let pad=Math.max((max-min)*.12,2);min-=pad;max+=pad;
  const left=82,right=W-12,top=18,bottom=H-30,usable=bottom-top;
  const y=p=>bottom-((p-min)/(max-min))*usable;
  ctx.font="13px Arial";ctx.strokeStyle="#263650";ctx.lineWidth=1;
  for(let i=0;i<5;i++){
    const yy=top+usable*i/4;
    ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();
    const val=max-(max-min)*i/4;ctx.fillStyle="#91a2c1";
    ctx.fillText(money(val),4,yy-3);
  }
  const step=(right-left)/visible.length,cw=Math.max(3,Math.min(18,step*.62));
  visible.forEach((c,i)=>{
    const x=left+i*step+step/2,up=Number(c.close)>=Number(c.open);
    const color=up?"#22d69a":"#ff6475";
    ctx.strokeStyle=color;ctx.fillStyle=color;ctx.lineWidth=2;
    ctx.beginPath();ctx.moveTo(x,y(Number(c.high)));ctx.lineTo(x,y(Number(c.low)));ctx.stroke();
    const oy=y(Number(c.open)),cy=y(Number(c.close));
    ctx.fillRect(x-cw/2,Math.min(oy,cy),cw,Math.max(2,Math.abs(cy-oy)));
  });
  function line(series,color){
    if(!series.length)return;
    ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=2;
    series.forEach((v,i)=>{const x=left+i*step+step/2;if(i===0)ctx.moveTo(x,y(v));else ctx.lineTo(x,y(v))});
    ctx.stroke();
  }
  line(e9,"#f4c76b");line(e21,"#69a8ff");
  ctx.fillStyle="#f4c76b";ctx.fillText("EMA 9",left+5,14);
  ctx.fillStyle="#69a8ff";ctx.fillText("EMA 21",left+65,14);
}

let pointers=new Map(),initialDistance=0,initialZoom=1,previousX=0;
const canvas=$("chart");
function distance(a,b){return Math.hypot(a.x-b.x,a.y-b.y)}
canvas.addEventListener("pointerdown",e=>{
  canvas.setPointerCapture(e.pointerId);pointers.set(e.pointerId,{x:e.clientX,y:e.clientY});
  if(pointers.size===1)previousX=e.clientX;
  if(pointers.size===2){const p=[...pointers.values()];initialDistance=distance(p[0],p[1]);initialZoom=zoomLevel}
});
canvas.addEventListener("pointermove",e=>{
  if(!pointers.has(e.pointerId))return;
  pointers.set(e.pointerId,{x:e.clientX,y:e.clientY});
  if(pointers.size===2){
    const p=[...pointers.values()],d=distance(p[0],p[1]);
    if(initialDistance>0){zoomLevel=Math.max(.5,Math.min(8,initialZoom*d/initialDistance));drawChart(chartData)}
  }else if(pointers.size===1){
    const delta=e.clientX-previousX;previousX=e.clientX;
    const visible=Math.max(12,Math.floor(100/zoomLevel));
    chartOffset+=Math.round(-delta/Math.max(1,canvas.clientWidth/visible));drawChart(chartData);
  }
});
function releasePointer(e){pointers.delete(e.pointerId);if(pointers.size<2)initialDistance=0}
canvas.addEventListener("pointerup",releasePointer);
canvas.addEventListener("pointercancel",releasePointer);

let requestInProgress=false;
async function refreshData(manual=false){
  if(requestInProgress)return;requestInProgress=true;
  const button=$("refreshButton");
  if(manual){button.disabled=true;button.textContent="ACTUALIZANDO..."}
  try{
    const response=await fetch((manual?"/refresh":"/api")+"?_="+Date.now(),{cache:"no-store"});
    if(!response.ok)throw new Error("HTTP "+response.status);
    const d=await response.json();
    if(d.price!==null&&d.price!==undefined){
      $("price").textContent=money(d.price);
      $("direction").textContent=d.direction||"ESPERANDO";
      $("signal").textContent=d.signal||"ESPERAR";
      $("confidence").textContent=(d.confidence??"--")+"%";
      $("prediction").textContent=money(d.prediction);
      $("predDirection").textContent=d.signal||"ESPERAR";
      $("predStrength").textContent=(d.confidence??"--")+"% fuerza";
      const cls=d.direction==="ALCISTA"?"up":d.direction==="BAJISTA"?"down":"neutral";
      $("direction").className="signal "+cls;
      $("predDirection").className="signal "+(d.signal==="SUBE"?"up":d.signal==="BAJA"?"down":"neutral");
      $("ema9").textContent=money(d.ema9);$("ema21").textContent=money(d.ema21);
      $("rsi").textContent=d.rsi===null?"--":Number(d.rsi).toFixed(1);
      $("vwap").textContent=money(d.vwap);
      $("buyPct").textContent=pct(d.buy_pct);$("sellPct").textContent=pct(d.sell_pct);
      if(d.buy_pct!==null&&d.buy_pct!==undefined){
        $("buyBar").style.width=d.buy_pct+"%";$("sellBar").style.width=d.sell_pct+"%";
        $("flowNote").textContent="Volumen comprador: "+d.buy_volume+" BTC · vendedor: "+d.sell_volume+" BTC";
      }else{$("flowNote").textContent="Coinbase aún no ha entregado suficiente flujo de operaciones."}
      $("tickMomentum").textContent="Impulso 5s: "+fmtMomentum(d.momentum_5s_pct)+" | 15s: "+fmtMomentum(d.momentum_15s_pct)+" | 30s: "+fmtMomentum(d.momentum_30s_pct);
      drawChart(d.candles);
    }
    $("countdown").textContent=formatTime(d.countdown||0);
    $("status").textContent=d.connected?"● EN VIVO":"● SIN DATOS";
    $("status").style.color=d.connected?"#22d69a":"#ff6475";
    $("source").textContent=d.source||"NINGUNA";
    $("socket").textContent=d.websocket_connected?"CONECTADO":"DESCONECTADO";
    if(d.last_update)$("updated").textContent="Último dato: "+new Date(d.last_update*1000).toLocaleTimeString();
    $("error").textContent=manual?(d.manual_refresh?"Actualización completada.":(d.error||"No se pudo actualizar.")):(d.error||"");
  }catch(err){$("status").textContent="ERROR DE CONEXIÓN";$("updated").textContent=String(err)}
  finally{requestInProgress=false;if(manual){button.disabled=false;button.textContent="ACTUALIZAR"}}
}
function manualRefresh(){refreshData(true)}
refreshData();
setInterval(()=>refreshData(false),1000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    start_background_services()
    app.run(host="0.0.0.0", port=10000, threaded=True)
