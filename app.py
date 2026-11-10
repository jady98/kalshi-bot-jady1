import os
import json
import time
import threading
import logging
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("btc-radar")

PRODUCT = "BTC-USD"
REST_URL = f"https://api.exchange.coinbase.com/products/{PRODUCT}/candles"
WS_URL = "wss://advanced-trade-ws.coinbase.com"
lock = threading.RLock()
candles = deque(maxlen=1200)
current_price = 0.0
last_update = 0.0
connection_status = False
ws_connected = False
last_error = ""


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def minute_timestamp(ts=None):
    ts = time.time() if ts is None else ts
    return int(ts // 60) * 60


def load_historical_candles():
    global current_price
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=300)
    params = {"granularity": 60, "start": start.isoformat(), "end": end.isoformat()}
    try:
        response = requests.get(REST_URL, params=params,
                                headers={"User-Agent": "BTC-Radar-PRO/2.0"}, timeout=15)
        response.raise_for_status()
        rows = response.json()
        parsed = []
        for row in rows:
            if len(row) < 6:
                continue
            parsed.append({"time": int(row[0]), "low": safe_float(row[1]),
                           "high": safe_float(row[2]), "open": safe_float(row[3]),
                           "close": safe_float(row[4]), "volume": safe_float(row[5])})
        parsed.sort(key=lambda x: x["time"])
        with lock:
            candles.clear()
            candles.extend(parsed)
            if candles:
                current_price = candles[-1]["close"]
                # REST candles may be delayed; use newest candle close only as initial value.
                global last_update
                last_update = time.time()
        log.info("Loaded %s Coinbase candles", len(parsed))
        return True
    except Exception as exc:
        log.exception("Historical candle load failed: %s", exc)
        return False


def update_live_candle(price, size=0.0, trade_time=None):
    global current_price, last_update
    if price <= 0:
        return
    ts = minute_timestamp(trade_time)
    with lock:
        current_price = price
        last_update = time.time()
        if not candles:
            candles.append({"time": ts, "open": price, "high": price,
                            "low": price, "close": price, "volume": size})
            return
        last = candles[-1]
        if ts == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
            last["volume"] += max(0.0, size)
        elif ts > last["time"]:
            # Fill missing minutes with flat candles so chart time stays continuous.
            next_ts = last["time"] + 60
            while next_ts < ts:
                prev = candles[-1]["close"]
                candles.append({"time": next_ts, "open": prev, "high": prev,
                                "low": prev, "close": prev, "volume": 0.0})
                next_ts += 60
            candles.append({"time": ts, "open": price, "high": price,
                            "low": price, "close": price, "volume": max(0.0, size)})


def websocket_message(ws, message):
    global connection_status, ws_connected, last_error
    try:
        payload = json.loads(message)
        if payload.get("type") == "error":
            last_error = str(payload)
            log.error("Coinbase websocket message error: %s", payload)
            return
        if payload.get("channel") == "subscriptions":
            ws_connected = True
            connection_status = True
        if payload.get("channel") != "market_trades":
            return
        for event in payload.get("events", []):
            for trade in event.get("trades", []):
                price = safe_float(trade.get("price"))
                size = safe_float(trade.get("size"))
                # Coinbase trade time is ISO-8601; use local receipt time if parsing fails.
                trade_ts = None
                stamp = trade.get("time")
                if stamp:
                    try:
                        trade_ts = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        pass
                if price > 0:
                    update_live_candle(price, size, trade_ts)
    except Exception as exc:
        log.exception("Websocket message parse failed: %s", exc)


def websocket_open(ws):
    global ws_connected
    ws_connected = False
    ws.send(json.dumps({"type": "subscribe", "channel": "market_trades", "product_ids": [PRODUCT]}))
    log.info("Coinbase Advanced Trade websocket connected")


def websocket_close(ws, code, message):
    global ws_connected
    ws_connected = False
    log.warning("Coinbase websocket closed (%s): %s", code, message)


def websocket_error(ws, error):
    global ws_connected, last_error
    ws_connected = False
    last_error = str(error)
    log.error("Coinbase websocket error: %s", error)


def websocket_worker():
    global ws_connected
    while True:
        try:
            client = websocket.WebSocketApp(WS_URL, on_open=websocket_open,
                                            on_message=websocket_message,
                                            on_close=websocket_close,
                                            on_error=websocket_error)
            client.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            log.exception("Websocket worker exception: %s", exc)
        ws_connected = False
        time.sleep(3)


def rest_price_worker():
    """Keeps live price flowing when the Coinbase websocket is unavailable."""
    global connection_status, last_error
    ticker_url = f"https://api.exchange.coinbase.com/products/{PRODUCT}/ticker"
    backup_url = f"https://api.coinbase.com/v2/prices/{PRODUCT}/spot"
    session = requests.Session()
    session.headers.update({"User-Agent": "BTC-Radar-PRO/2.1", "Accept": "application/json"})
    while True:
        try:
            response = session.get(ticker_url, timeout=8)
            response.raise_for_status()
            payload = response.json()
            price = safe_float(payload.get("price"))
            if price <= 0:
                raise ValueError("Coinbase ticker devolvió un precio inválido")
            update_live_candle(price, 0.0)
            connection_status = True
            last_error = ""
        except Exception as primary_exc:
            try:
                response = session.get(backup_url, timeout=8)
                response.raise_for_status()
                payload = response.json()
                price = safe_float((payload.get("data") or {}).get("amount"))
                if price <= 0:
                    raise ValueError("Coinbase spot devolvió un precio inválido")
                update_live_candle(price, 0.0)
                connection_status = True
                last_error = ""
            except Exception as backup_exc:
                with lock:
                    age = time.time() - last_update if last_update else 999999
                    connection_status = age < 20
                last_error = f"Ticker: {primary_exc}; respaldo: {backup_exc}"
                log.warning("REST price polling failed: %s", last_error)
        time.sleep(3)


def ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    result = [values[0]]
    for value in values[1:]:
        result.append(value * alpha + result[-1] * (1.0 - alpha))
    return result


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes]
    losses = [max(-x, 0.0) for x in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def vwap_series(data):
    total_volume = 0.0
    total_value = 0.0
    result = []
    for candle in data:
        typical = (candle["high"] + candle["low"] + candle["close"]) / 3.0
        volume = max(0.0, candle["volume"])
        total_volume += volume
        total_value += typical * volume
        result.append(total_value / total_volume if total_volume else candle["close"])
    return result


def macd_values(values):
    if len(values) < 26:
        return 0.0, 0.0, 0.0
    fast = ema(values, 12)
    slow = ema(values, 26)
    line = [a - b for a, b in zip(fast, slow)]
    signal_line = ema(line, 9)
    return line[-1], signal_line[-1], line[-1] - signal_line[-1]


def aggregate_candles(data, minutes):
    if minutes == 1:
        return list(data)
    grouped = {}
    span = minutes * 60
    for c in data:
        bucket = (c["time"] // span) * span
        if bucket not in grouped:
            grouped[bucket] = {"time": bucket, "open": c["open"], "high": c["high"],
                               "low": c["low"], "close": c["close"], "volume": c["volume"]}
        else:
            g = grouped[bucket]
            g["high"] = max(g["high"], c["high"])
            g["low"] = min(g["low"], c["low"])
            g["close"] = c["close"]
            g["volume"] += c["volume"]
    return sorted(grouped.values(), key=lambda x: x["time"])


def analyze(data):
    closes = [c["close"] for c in data]
    if len(closes) < 30:
        return {"bull": 50.0, "bear": 50.0, "neutral": 0.0,
                "direction": "REUNIENDO DATOS", "score": 0.0,
                "rsi": 50.0, "ema9": None, "ema21": None,
                "vwap": None, "macd": 0.0, "histogram": 0.0}
    e9, e21 = ema(closes, 9), ema(closes, 21)
    vwap = vwap_series(data)
    rv = rsi(closes)
    macd, signal, hist = macd_values(closes)
    score = 0.0
    score += 2.0 if e9[-1] > e21[-1] else -2.0
    score += 1.5 if closes[-1] > vwap[-1] else -1.5
    score += 1.5 if hist > 0 else -1.5
    momentum = (closes[-1] - closes[-6]) / closes[-6] * 100 if closes[-6] else 0.0
    score += max(-2.0, min(2.0, momentum * 8.0))
    if 50 < rv < 70:
        score += 1.0
    elif 30 < rv < 50:
        score -= 1.0
    elif rv >= 75:
        score -= 1.0
    elif rv <= 25:
        score += 1.0
    score = max(-8.0, min(8.0, score))
    strength = min(1.0, abs(score) / 8.0)
    bull_raw = 50.0 + score * 5.0
    bear_raw = 50.0 - score * 5.0
    neutral_raw = max(5.0, 100.0 - strength * 100.0)
    total = bull_raw + bear_raw + neutral_raw
    bull = bull_raw / total * 100.0
    bear = bear_raw / total * 100.0
    neutral = neutral_raw / total * 100.0
    direction = "ALCISTA" if bull > bear and bull > neutral else "BAJISTA" if bear > bull and bear > neutral else "INDECISO"
    return {"bull": round(bull, 1), "bear": round(bear, 1), "neutral": round(neutral, 1),
            "direction": direction, "score": round(score, 2), "rsi": round(rv, 2),
            "ema9": round(e9[-1], 2), "ema21": round(e21[-1], 2),
            "vwap": round(vwap[-1], 2), "macd": round(macd, 4),
            "signal": round(signal, 4), "histogram": round(hist, 4)}


def forecast_15m(data, price, prediction):
    """Heuristic scenario estimate, not a guaranteed or calibrated probability."""
    closes = [c["close"] for c in data if c["close"] > 0]
    if len(closes) < 30 or price <= 0:
        return {"target": None, "change_pct": None, "direction": "ESPERANDO DATOS",
                "horizon_minutes": 15, "method": "insufficient_data"}
    recent = closes[-30:]
    # One-minute log returns, trimmed to reduce the impact of a single spike.
    returns = []
    for i in range(1, len(recent)):
        if recent[i - 1] > 0:
            returns.append((recent[i] / recent[i - 1]) - 1.0)
    returns = sorted(returns)
    if len(returns) > 4:
        returns = returns[2:-2]
    avg_return = sum(returns) / max(1, len(returns))
    volatility = (sum((x - avg_return) ** 2 for x in returns) / max(1, len(returns))) ** 0.5
    score = safe_float(prediction.get("score"))
    # Blend short-term drift with technical score. Cap projection to 1.5x recent 15m sigma.
    raw_pct = (avg_return * 15.0 * 100.0 * 0.45) + (score / 8.0 * volatility * 15.0 * 100.0 * 0.55)
    cap_pct = max(0.04, min(1.20, volatility * (15 ** 0.5) * 100.0 * 1.5))
    projected_pct = max(-cap_pct, min(cap_pct, raw_pct))
    target = price * (1.0 + projected_pct / 100.0)
    direction = "SUBE" if projected_pct > 0.015 else "BAJA" if projected_pct < -0.015 else "LATERAL"
    return {"target": round(target, 2), "change_pct": round(projected_pct, 3),
            "direction": direction, "horizon_minutes": 15,
            "range_pct": round(cap_pct, 3), "method": "technical_drift_volatility_estimate"}


PAGE = r'''<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#050610;color:#f5f4ff;font-family:Arial,sans-serif}header{display:flex;align-items:center;gap:12px;padding:15px;border-bottom:1px solid #29263f}.coin{width:48px;height:48px;border-radius:50%;background:#f7931a;display:grid;place-items:center;font-size:32px;font-weight:bold}.brand{font-size:24px;font-weight:900}.brand b{color:#b12cff}.sub{font-size:12px;color:#aaa7c5;margin-top:4px}.live{margin-left:auto;color:#00e88b;font-size:12px}.wrap{max-width:1000px;margin:auto;padding:10px}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.card,.tile{background:#0b0d1c;border:1px solid #34314f;border-radius:15px;padding:12px;text-align:center;min-width:0}.card .label{font-size:12px;color:#c5c1dd}.pct{font-size:clamp(24px,6vw,34px);font-weight:bold;margin:9px 0}.green{color:#00e58b}.red{color:#ff4166}.purple{color:#bd55ff}.direction{font-size:22px;font-weight:900;margin:9px 0}.bar{height:8px;border-radius:20px;background:#28283a;overflow:hidden}.bar span{display:block;height:100%;width:0;background:currentColor}.tabs{display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin:14px 0}.tf{background:#17172a;color:#ddd;border:1px solid #393650;border-radius:9px;padding:9px 15px}.tf.active{background:#8500ff;border-color:#a84bff;color:#fff}.chartbox{height:390px;border:1px solid #302d49;border-radius:14px;overflow:hidden;background:#080a16}.indicators{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px}.tile{text-align:left}.tile small{color:#aaa}.tile strong{display:block;font-size:20px;margin-top:8px}.bottom{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-top:10px}.bottom .tile strong{font-size:clamp(22px,5vw,32px)}.status{display:flex;justify-content:space-around;gap:8px;flex-wrap:wrap;background:#111326;border:1px solid #34314f;border-radius:30px;padding:13px;margin-top:14px;font-size:12px;color:#c9c5df}.note{text-align:center;color:#918da9;font-size:11px;padding:14px}.prediction-note{font-size:11px;color:#aaa;text-align:center;margin-top:7px}@media(max-width:420px){.brand{font-size:20px}.chartbox{height:340px}.tile{padding:10px 8px}.tile strong{font-size:17px}}
</style></head><body>
<header><div class="coin">₿</div><div><div class="brand">BTC Radar <b>PRO</b></div><div class="sub">Bitcoin / USD · Coinbase · análisis técnico</div></div><div class="live" id="live">● INICIANDO</div></header>
<div class="wrap">
<section class="cards">
<div class="card"><div class="label">↗ Probabilidad alcista*</div><div class="pct green" id="bull">--%</div><div class="bar green"><span id="bullbar"></span></div></div>
<div class="card"><div class="label">Señal técnica</div><div class="direction purple" id="direction">ESPERANDO</div><div class="label" id="score">Calculando...</div></div>
<div class="card"><div class="label">↘ Probabilidad bajista*</div><div class="pct red" id="bear">--%</div><div class="bar red"><span id="bearbar"></span></div></div>
</section>
<div class="tabs"><button class="tf active" data-tf="1">1m</button><button class="tf" data-tf="3">3m</button><button class="tf" data-tf="5">5m</button><button class="tf" data-tf="15">15m</button></div>
<div class="chartbox" id="chart"></div>
<section class="indicators"><div class="tile"><small>RSI (14)</small><strong id="rsi">--</strong></div><div class="tile"><small>MACD histograma</small><strong id="macd">--</strong></div><div class="tile"><small>EMA 9 / EMA 21</small><strong id="emas">--</strong></div></section>
<section class="bottom"><div class="tile"><small>⏱ Tiempo restante del bloque 15m</small><strong class="purple" id="countdown">15:00</strong><div class="bar purple" style="margin-top:9px"><span id="timebar" style="width:100%"></span></div></div><div class="tile"><small>Precio BTC/USD en vivo</small><strong class="green" id="price">$--</strong><small id="updated">Esperando actualización</small></div><div class="tile"><small>Estimación de precio en 15 minutos</small><strong class="purple" id="target">$--</strong><small id="targetchange">Sin estimación todavía</small></div><div class="tile"><small>Dirección estimada 15m</small><strong id="forecastdir">--</strong><small>Escenario técnico, no garantía</small></div></section>
<div class="prediction-note">*Los porcentajes son una puntuación técnica normalizada, no probabilidades estadísticas calibradas.</div>
<div class="status"><span id="connection">● Conectando Coinbase</span><span id="age">Último dato: --</span></div>
<div class="note">BTC Radar PRO · Fuente spot: Coinbase BTC-USD. Puede diferir del índice de liquidación BRTI de Kalshi. No ejecuta operaciones ni garantiza resultados.</div>
</div>
<script>
const $=id=>document.getElementById(id);let tf=1,lastState=null,chart=null,candleSeries=null,ema9Series=null,ema21Series=null,vwapSeries=null,volumeSeries=null;
function money(v){return Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}
function initChart(){if(!window.LightweightCharts){$('chart').textContent='No se pudo cargar la librería de gráficas. Comprueba la conexión.';return}chart=LightweightCharts.createChart($('chart'),{width:$('chart').clientWidth,height:$('chart').clientHeight,layout:{background:{color:'#080a16'},textColor:'#c7c4dc'},grid:{vertLines:{color:'#202238'},horzLines:{color:'#202238'}},rightPriceScale:{borderColor:'#34314f'},timeScale:{borderColor:'#34314f',timeVisible:true,secondsVisible:false},crosshair:{mode:LightweightCharts.CrosshairMode.Normal}});candleSeries=chart.addCandlestickSeries({upColor:'#00d98a',downColor:'#ff4166',borderUpColor:'#00d98a',borderDownColor:'#ff4166',wickUpColor:'#00d98a',wickDownColor:'#ff4166'});ema9Series=chart.addLineSeries({color:'#00aaff',lineWidth:2,priceLineVisible:false});ema21Series=chart.addLineSeries({color:'#ff9d2e',lineWidth:2,priceLineVisible:false});vwapSeries=chart.addLineSeries({color:'#b34cff',lineWidth:2,priceLineVisible:false});volumeSeries=chart.addHistogramSeries({priceFormat:{type:'volume'},priceScaleId:'',scaleMargins:{top:.82,bottom:0}});chart.timeScale().fitContent()}
function ema(values,n){if(!values.length)return[];let a=2/(n+1),out=[values[0]];for(let i=1;i<values.length;i++)out.push(values[i]*a+out[i-1]*(1-a));return out}
function vwap(data){let vol=0,val=0,out=[];for(const c of data){let typical=(c.high+c.low+c.close)/3;vol+=c.volume;val+=typical*c.volume;out.push(vol?val/vol:c.close)}return out}
function updateChart(){if(!chart||!lastState)return;let data=lastState.timeframes[String(tf)]||[];if(!data.length)return;candleSeries.setData(data.map(c=>({time:c.time,open:c.open,high:c.high,low:c.low,close:c.close})));let closes=data.map(c=>c.close);ema9Series.setData(ema(closes,9).map((v,i)=>({time:data[i].time,value:v})));ema21Series.setData(ema(closes,21).map((v,i)=>({time:data[i].time,value:v})));vwapSeries.setData(vwap(data).map((v,i)=>({time:data[i].time,value:v})));volumeSeries.setData(data.map(c=>({time:c.time,value:c.volume,color:c.close>=c.open?'#00d98a66':'#ff416666'})));chart.timeScale().scrollToRealTime()}
function render(s){lastState=s;let p=s.prediction||{},f=s.forecast||{};$('bull').textContent=(p.bull??'--')+'%';$('bear').textContent=(p.bear??'--')+'%';$('bullbar').style.width=(p.bull||0)+'%';$('bearbar').style.width=(p.bear||0)+'%';$('direction').textContent=p.direction||'--';$('direction').className='direction '+(p.direction==='ALCISTA'?'green':p.direction==='BAJISTA'?'red':'purple');$('score').textContent='Fuerza técnica: '+(p.score??'--');$('price').textContent='$'+money(s.price);$('rsi').textContent=p.rsi??'--';$('macd').textContent=p.histogram??'--';$('emas').textContent=p.ema9&&p.ema21?money(p.ema9)+' / '+money(p.ema21):'--';$('target').textContent=f.target?'$'+money(f.target):'$--';$('targetchange').textContent=f.change_pct!==null&&f.change_pct!==undefined?((f.change_pct>0?'+':'')+f.change_pct+'% estimado'): 'Esperando datos';$('forecastdir').textContent=f.direction||'--';$('forecastdir').className=f.direction==='SUBE'?'green':f.direction==='BAJA'?'red':'purple';$('connection').textContent=s.connected?'● Coinbase conectado':'● Coinbase desconectado';$('live').textContent=s.connected?'● EN VIVO':'● SIN CONEXIÓN';$('age').textContent='Último dato: '+s.update_age+' s';$('updated').textContent='Cambio sesión: '+(s.change>=0?'+':'')+s.change+'%';updateChart()}
async function refresh(){try{let r=await fetch('/api/state?t='+Date.now(),{cache:'no-store'});if(!r.ok)throw Error('API '+r.status);render(await r.json())}catch(e){$('connection').textContent='● Esperando datos / reconectando';$('live').textContent='● SIN DATOS'}}
function countdown(){let rem=900-Math.floor(Date.now()/1000)%900;$('countdown').textContent=String(Math.floor(rem/60)).padStart(2,'0')+':'+String(rem%60).padStart(2,'0');$('timebar').style.width=(rem/900*100)+'%'}
document.querySelectorAll('.tf').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.tf').forEach(x=>x.classList.remove('active'));b.classList.add('active');tf=Number(b.dataset.tf);updateChart()}));
initChart();window.addEventListener('resize',()=>{if(chart)chart.applyOptions({width:$('chart').clientWidth,height:$('chart').clientHeight})});refresh();countdown();setInterval(refresh,2000);setInterval(countdown,250);
</script></body></html>'''


@app.route("/")
def home():
    return render_template_string(PAGE)


@app.route("/health")
def health():
    with lock:
        count = len(candles)
    return jsonify({"ok": True, "candles": count, "connected": connection_status, "websocket_connected": ws_connected,
                    "last_error": last_error})


@app.route("/api/state")
def api_state():
    with lock:
        raw = list(candles)
        price = current_price
        updated = last_update
        connected = connection_status and (time.time() - updated < 20 if updated else False)
    if not raw or price <= 0:
        return jsonify({"error": "Esperando datos de Coinbase", "connected": connected,
                        "last_error": last_error}), 503
    frames = {str(n): aggregate_candles(raw, n)[-180:] for n in (1, 3, 5, 15)}
    prediction = analyze(raw)
    forecast = forecast_15m(raw, price, prediction)
    first = raw[0]["open"] or price
    change = (price - first) / first * 100 if first else 0.0
    return jsonify({"price": round(price, 2), "change": round(change, 3),
                    "connected": connected, "update_age": round(time.time() - updated, 1) if updated else 999,
                    "prediction": prediction, "forecast": forecast, "timeframes": frames})


_started = False
_start_lock = threading.Lock()


def start_background():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
    load_historical_candles()
    threading.Thread(target=websocket_worker, daemon=True, name="coinbase-ws").start()
    threading.Thread(target=rest_price_worker, daemon=True, name="coinbase-rest").start()


start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), threaded=True)
