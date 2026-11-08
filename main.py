import os
import json
import time
import base64
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

app = Flask(__name__)

# =========================================================
# BITCOIN RADAR PRO 3.0
# BRTI EN VIVO DESDE KALSHI
# SOLO LECTURA: NO ENVIA ORDENES
# =========================================================

KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n")

lock = threading.Lock()
ticks = deque(maxlen=12000)
candles = {}
latest = {
    "price": None,
    "avg60": None,
    "avg15": None,
    "timestamp": None,
    "connected": False,
    "error": None
}

market_cache = {
    "data": None,
    "updated": 0
}


def make_signature(timestamp):
    if not API_KEY_ID or not PRIVATE_KEY_PEM:
        raise RuntimeError("Faltan las variables RSA de Kalshi")

    private_key = serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode("utf-8"),
        password=None
    )

    message = (
        str(timestamp) + "GET/trade-api/ws/v2"
    ).encode("utf-8")

    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=hashes.SHA256().digest_size
        ),
        hashes.SHA256()
    )

    return base64.b64encode(signature).decode("utf-8")


def ws_headers():
    timestamp = str(int(time.time() * 1000))

    return [
        "KALSHI-ACCESS-KEY: " + API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP: " + timestamp,
        "KALSHI-ACCESS-SIGNATURE: " + make_signature(timestamp)
    ]


def add_brtI_tick(price, timestamp_ms):
    price = float(price)
    timestamp_ms = int(timestamp_ms)
    second = timestamp_ms // 1000
    minute = second - (second % 60)

    with lock:
        ticks.append({
            "time": second,
            "price": price
        })

        candle = candles.get(minute)

        if candle is None:
            candles[minute] = {
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0
            }
        else:
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += 1

        # Mantener solo las 250 velas más recientes
        old_keys = sorted(candles.keys())[:-250]
        for key in old_keys:
            candles.pop(key, None)

        latest["price"] = price
        latest["timestamp"] = second
        latest["connected"] = True
        latest["error"] = None


def on_message(ws, message):
    try:
        packet = json.loads(message)
        if packet.get("type") != "cfbenchmarks_value":
            return

        msg = packet.get("msg", {})

        if msg.get("index_id") != "BRTI":
            return

        raw = json.loads(msg.get("data", "{}"))

        price = raw.get("value")
        timestamp_ms = raw.get("time") or msg.get("received_at")

        if price is None or timestamp_ms is None:
            return

        add_brtI_tick(price, timestamp_ms)

        avg60 = msg.get("avg_60s_data") or {}
        avg15 = msg.get("last_60s_windowed_average_15min") or {}

        with lock:
            latest["avg60"] = float(avg60["value"]) if avg60.get("value") else None
            latest["avg15"] = float(avg15["value"]) if avg15.get("value") else None

    except Exception as exc:
        app.logger.warning("Mensaje BRTI no procesado: %s", exc)


def on_open(ws):
    with lock:
        latest["connected"] = True
        latest["error"] = None

    ws.send(json.dumps({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["cfbenchmarks_value"],
            "index_ids": ["BRTI"]
        }
    }))


def on_error(ws, error):
    with lock:
        latest["connected"] = False
        latest["error"] = str(error)


def on_close(ws, status, message):
    with lock:
        latest["connected"] = False


def brti_worker():
    while True:
        try:
            socket = websocket.WebSocketApp(
                KALSHI_WS,
                header=ws_headers(),
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            socket.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            with lock:
                latest["connected"] = False
                latest["error"] = str(exc)

        time.sleep(5)


def start_brtI():
    thread = threading.Thread(
        target=brti_worker,
        daemon=True
    )
    thread.start()


def calculate_signal():
    with lock:
        values = [x["price"] for x in ticks]

    if len(values) < 30:
        return "ESPERAR", "Recopilando datos BRTI en vivo"

    recent = values[-100:]

    fast = sum(recent[-9:]) / min(9, len(recent))
    slow = sum(recent[-21:]) / min(21, len(recent))

    changes = [
        recent[i] - recent[i - 1]
        for i in range(1, len(recent))
    ]

    gains = [x for x in changes[-14:] if x > 0]
    losses = [-x for x in changes[-14:] if x < 0]

    avg_gain = sum(gains) / 14
    avg_loss = sum(losses) / 14

    if avg_loss == 0:
        rsi = 100 if avg_gain > 0 else 50
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    momentum = recent[-1] - recent[-15]

    if fast > slow and rsi > 52 and momentum > 0:
        return "SUBE", "Tendencia y momentum positivos"

    if fast < slow and rsi < 48 and momentum < 0:
        return "BAJA", "Tendencia y momentum negativos"

    return "ESPERAR", "No existe confirmación suficiente"


def get_kalshi_market():
    now = time.time()

    if market_cache["data"] and now - market_cache["updated"] < 3:
        return market_cache["data"]

    response = requests.get(
        f"{KALSHI_API}/markets",
        params={
            "series_ticker": SERIES,
            "status": "open",
            "limit": 100
        },
        timeout=10
    )
    response.raise_for_status()

    markets = response.json().get("markets", [])
    candidates = []

    for market in markets:
        close_text = market.get("close_time") or market.get(
            "expected_expiration_time"
        )

        if not close_text:
            continue

        try:
            close_dt = datetime.fromisoformat(
                close_text.replace("Z", "+00:00")
            )
            if close_dt.timestamp() > now:
                candidates.append((close_dt.timestamp(), market))
        except ValueError:
            continue

    if not candidates:
        result = {
            "available": False,
            "message": "No se encontró un contrato BTC 15M abierto."
        }
    else:
        candidates.sort(key=lambda x: x[0])
        close_ts, m = candidates[0]

        result = {
            "available": True,
            "ticker": m.get("ticker"),
            "title": m.get("title") or m.get("subtitle"),
            "close_ts": int(close_ts),
            "yes_bid": m.get("yes_bid_dollars"),
            "yes_ask": m.get("yes_ask_dollars"),
            "no_bid": m.get("no_bid_dollars"),
            "no_ask": m.get("no_ask_dollars"),
            "last_price": m.get("last_price_dollars")
        }

    market_cache["data"] = result
    market_cache["updated"] = now

    return result
HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar PRO by Jady</title>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>
*{box-sizing:border-box}
body{
 margin:0;padding:14px;background:#070b16;color:#eef4ff;
 font-family:Arial,sans-serif
}
main{max-width:1100px;margin:auto}
header{
 display:flex;justify-content:space-between;align-items:center;
 flex-wrap:wrap;gap:10px;margin-bottom:14px
}
h1{font-size:20px;margin:0;letter-spacing:1px}
.sub{font-size:11px;color:#00d69a;margin-top:5px}
.panel{
 background:#111a2a;border:1px solid #293953;
 border-radius:13px;padding:14px;margin-bottom:12px
}
.grid{
 display:grid;grid-template-columns:repeat(2,1fr);gap:9px
}
.metric{background:#19263a;padding:13px;border-radius:9px}
.metric small{color:#9eafc5;font-size:10px}
.metric strong{display:block;font-size:21px;margin-top:8px}
.section{font-size:11px;color:#aebed2;margin-bottom:12px}
.chart{height:350px;width:100%}
.green{color:#00d69a}
.red{color:#ff5268}
.yellow{color:#ffc857}
.signals{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.signal{
 padding:13px 4px;text-align:center;background:#19263a;
 border:1px solid #344963;border-radius:8px;font-weight:bold
}
.active-up{color:#00d69a;border-color:#00d69a}
.active-down{color:#ff5268;border-color:#ff5268}
.active-wait{color:#ffc857;border-color:#ffc857}
.two{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.quote{background:#19263a;padding:13px;border-radius:9px}
.quote small{color:#9eafc5}
.quote strong{display:block;font-size:25px;margin-top:8px}
.note{font-size:10px;color:#8799b1;line-height:1.5}
button{
 background:#1b2c45;color:white;border:1px solid #3a5272;
 border-radius:8px;padding:10px 14px;font-weight:bold
}
@media(min-width:750px){
 .grid{grid-template-columns:repeat(4,1fr)}
 .layout{display:grid;grid-template-columns:1.5fr 1fr;gap:12px}
 .chart{height:470px}
}
</style>
</head>

<body>
<main>

<header>
 <div>
  <h1>₿ BITCOIN RADAR PRO</h1>
  <div class="sub">BY JADY ♥ · KALSHI BRTI · BTC 15 MIN</div>
 </div>
 <button onclick="updateAll()">ACTUALIZAR</button>
</header>

<div class="grid">
 <div class="metric">
  <small>ÍNDICE BRTI EN VIVO</small>
  <strong id="price">--</strong>
 </div>
 <div class="metric">
  <small>SEÑAL DEL RADAR</small>
  <strong id="signal" class="yellow">ESPERAR</strong>
 </div>
 <div class="metric">
  <small>PROMEDIO BRTI 60S</small>
  <strong id="avg60">--</strong>
 </div>
 <div class="metric">
  <small>CIERRE DEL CONTRATO</small>
  <strong id="timer">--:--</strong>
 </div>
</div>

<div class="layout">
<div>

<section class="panel">
 <div class="section">ALERTA DE DIRECCIÓN</div>
 <div class="signals">
  <div class="signal" id="up">▲ SUBE</div>
  <div class="signal" id="down">▼ BAJA</div>
  <div class="signal active-wait" id="wait">Ⅱ ESPERAR</div>
 </div>
 <p class="note" id="reason">Esperando datos de Kalshi.</p>
</section>

<section class="panel">
 <div class="section">BRTI · VELAS DE 1 MINUTO</div>
 <div id="chart" class="chart"></div>
</section>

<section class="panel">
 <div class="section">PROMEDIO FINAL DE 60 SEGUNDOS</div>
 <strong id="avg15" class="green">--</strong>
 <p class="note">
 Este dato aparece durante el último minuto del cuarto de hora
 cuando Kalshi lo transmite.
 </p>
</section>

</div>

<div>

<section class="panel">
 <div class="section">KALSHI · CONTRATO BTC 15M</div>
 <strong id="marketTitle">Buscando mercado...</strong>
 <p class="note" id="ticker">Ticker: --</p>

 <div class="two">
  <div class="quote">
   <small>YES ASK</small>
   <strong class="green" id="yesAsk">--</strong>
   <small id="yesBid">Bid: --</small>
  </div>
  <div class="quote">
   <small>NO ASK</small>
   <strong class="red" id="noAsk">--</strong>
   <small id="noBid">Bid: --</small>
  </div>
 </div>
 <p class="note" id="marketStatus">Esperando cotizaciones.</p>
</section>

<section class="panel">
 <div class="section">ESTADO DE CONEXIÓN</div>
 <strong id="connection" class="yellow">CONECTANDO...</strong>
 <p class="note" id="updated">Sin actualización todavía.</p>
 <p class="note">
 El precio principal procede del índice BRTI. YES y NO son
 precios de contratos, no el precio del Bitcoin.
 </p>
</section>

<section class="panel">
 <div class="section">CONFIGURACIÓN DEL RADAR</div>
 <p class="note">
 Actualización automática de datos. Señales técnicas basadas
 en tendencia y momentum. No ejecuta operaciones ni garantiza
 resultados.
 </p>
 <button onclick="location.reload()">RECARGAR RADAR</button>
</section>

</div>
</div>

</main>

<script>
const $ = id => document.getElementById(id);

const money = n => n == null
 ? "--"
 : "$" + Number(n).toLocaleString("en-US",{
     minimumFractionDigits:2,
     maximumFractionDigits:2
   });

const cents = n => n == null
 ? "--¢"
 : (Number(n)*100).toFixed(1)+"¢";

let chart = null;
let series = null;
let lastSignal = "";
let closeTime = null;

function setupChart(){
 const el = $("chart");

 if(typeof LightweightCharts === "undefined"){
  $("connection").textContent="ERROR: No cargó la gráfica";
  return;
 }

 chart = LightweightCharts.createChart(el,{
  width:el.clientWidth,
  height:470,
  layout:{
   background:{color:"#111a2a"},
   textColor:"#aebed2"
  },
  grid:{
   vertLines:{color:"#1d2b40"},
   horzLines:{color:"#1d2b40"}
  },
  timeScale:{
   timeVisible:true,
   secondsVisible:false,
   borderColor:"#34445a"
  },
  rightPriceScale:{borderColor:"#34445a"}
 });

 series = chart.addCandlestickSeries({
  upColor:"#00d69a",
  downColor:"#ff5268",
  borderUpColor:"#00d69a",
  borderDownColor:"#ff5268",
  wickUpColor:"#00d69a",
  wickDownColor:"#ff5268"
 });
}

function renderCandles(rows){
 if(!series || !rows) return;

 const clean = rows.filter(x =>
  Number.isFinite(x.time) &&
  Number.isFinite(x.open) &&
  Number.isFinite(x.high) &&
  Number.isFinite(x.low) &&
  Number.isFinite(x.close)
 );

 series.setData(clean);

 if(clean.length){
  chart.timeScale().fitContent();
 }
}

function renderSignal(s, reason){
 $("signal").textContent=s;
 $("reason").textContent=reason;

 $("signal").className =
  s==="SUBE" ? "green" :
  s==="BAJA" ? "red" : "yellow";

 $("up").className="signal"+(s==="SUBE"?" active-up":"");
 $("down").className="signal"+(s==="BAJA"?" active-down":"");
 $("wait").className="signal"+(s==="ESPERAR"?" active-wait":"");

 if(lastSignal && lastSignal!==s && "Notification" in window &&
    Notification.permission==="granted"){
  new Notification("Bitcoin Radar: "+s);
 }

 lastSignal=s;
}

async function getJSON(url){
 const response=await fetch(url,{cache:"no-store"});
 const data=await response.json();

 if(!response.ok){
  throw new Error(data.error || "Error consultando "+url);
 }

 return data;
}

async function updateAll(){
 try{
  const [btc,market]=await Promise.all([
   getJSON("/api/data"),
   getJSON("/api/kalshi")
  ]);

  $("price").textContent=money(btc.price);
  $("avg60").textContent=money(btc.avg60);
  $("avg15").textContent=money(btc.avg15);

  renderSignal(btc.signal,btc.reason);
  renderCandles(btc.candles);

  if(market.available){
   $("marketTitle").textContent=market.title || "Bitcoin 15 minutos";
   $("ticker").textContent="Ticker: "+market.ticker;
   $("yesAsk").textContent=cents(market.yes_ask);
   $("noAsk").textContent=cents(market.no_ask);
   $("yesBid").textContent="Bid: "+cents(market.yes_bid);
   $("noBid").textContent="Bid: "+cents(market.no_bid);
   $("marketStatus").textContent="Cotizaciones recibidas de Kalshi.";
   closeTime=market.close_ts;
  }else{
   $("marketTitle").textContent="Contrato no disponible";
   $("marketStatus").textContent=market.message || "Sin mercado abierto.";
  }

  $("connection").textContent=btc.connected
   ? "● BRTI CONECTADO"
   : "● SIN CONEXIÓN BRTI";

  $("connection").className=btc.connected?"green":"red";

  $("updated").textContent="Última actualización: "+
   new Date().toLocaleTimeString();

 }catch(error){
  $("connection").textContent="ERROR: "+error.message;
  $("connection").className="red";
 }
}

function updateTimer(){
 if(!closeTime) return;

 const seconds=Math.max(
  0,
  closeTime-Math.floor(Date.now()/1000)
 );

 $("timer").textContent=
  String(Math.floor(seconds/60)).padStart(2,"0")+":"+
  String(seconds%60).padStart(2,"0");
}

window.addEventListener("resize",()=>{
 if(chart){
  chart.applyOptions({
   width:$("chart").clientWidth
  });
 }
});

setupChart();
updateAll();

setInterval(updateAll,2000);
setInterval(updateTimer,1000);
</script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():
    with lock:
        price = latest["price"]
        avg60 = latest["avg60"]
        avg15 = latest["avg15"]
        connected = latest["connected"]
        error = latest["error"]
        rows = list(candles.values())

    rows.sort(key=lambda x: x["time"])

    signal, reason = calculate_signal()

    return jsonify({
        "price": price,
        "avg60": avg60,
        "avg15": avg15,
        "connected": connected,
        "error": error,
        "signal": signal,
        "reason": reason,
        "candles": rows
    })


@app.route("/api/kalshi")
def api_kalshi():
    try:
        return jsonify(get_kalshi_market())
    except Exception as exc:
        app.logger.exception("Error consultando mercado Kalshi")
        return jsonify({
            "available": False,
            "message": str(exc)
        }), 502

start_brtI()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
