import os
import json
import time
import threading
import logging
from collections import deque

import requests
import websocket

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

# CONFIGURACION
PRODUCT = "BTC-USD"
WS_URL = "wss://advanced-trade-ws.coinbase.com"
REST_URL = "https://api.exchange.coinbase.com"

PORT = int(os.environ.get("PORT", 10000))

# DATOS
candles = deque(maxlen=500)
current_price = 0.0
connection_status = "CONECTANDO"
last_update = 0
last_error = ""

lock = threading.RLock()
started = False


def load_history():
    global current_price

    try:
        url = f"{REST_URL}/products/{PRODUCT}/candles"

        response = requests.get(
            url,
            params={"granularity": 60},
            timeout=15,
            headers={"User-Agent": "BTC-Radar"}
        )

        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list):
            return False

        with lock:
            candles.clear()

            for item in reversed(data):
                candles.append({
                    "time": int(item[0]),
                    "low": float(item[1]),
                    "high": float(item[2]),
                    "open": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5])
                })

            if candles:
                current_price = candles[-1]["close"]

        logging.info("Historial Coinbase cargado: %s", len(data))
        return True

    except Exception:
        logging.exception("Error cargando historial")
        return False


def calculate_ema(values, period):
    if not values:
        return []

    multiplier = 2 / (period + 1)
    result = [float(values[0])]

    for value in values[1:]:
        result.append(
            (float(value) - result[-1]) * multiplier
            + result[-1]
        )

    return result


def calculate_rsi(values, period=14):
    if len(values) <= period:
        return 50.0

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0) for x in changes]
    losses = [max(-x, 0) for x in changes]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(changes)):
        avg_gain = (
            avg_gain * (period - 1) + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def calculate_vwap(data):
    if not data:
        return 0.0

    total = 0.0
    volume_total = 0.0

    for candle in data[-100:]:
        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3

        volume = candle["volume"]

        total += typical * volume
        volume_total += volume

    if volume_total == 0:
        return data[-1]["close"]

    return total / volume_total


def update_candle(price, size=0):
    global current_price, last_update

    now = int(time.time())
    minute = now - now % 60

    with lock:
        current_price = price
        last_update = now

        if candles and candles[-1]["time"] == minute:
            candle = candles[-1]

            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] += size

        else:
            candles.append({
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size
            })


def get_market_data():
    with lock:
        data = [dict(c) for c in candles]
        price = current_price

    return data, price
    def on_message(ws, message):
    global connection_status, last_error

    try:
        data = json.loads(message)
        channel = data.get("channel")

        if channel == "subscriptions":
            connection_status = "CONECTADO A COINBASE"
            return

        if channel not in ("ticker", "market_trades"):
            return

        for event in data.get("events", []):
            for ticker in event.get("tickers", []):
                if ticker.get("product_id") != PRODUCT:
                    continue

                price = float(ticker["price"])
                update_candle(price)

            for trade in event.get("trades", []):
                if trade.get("product_id") != PRODUCT:
                    continue

                price = float(trade["price"])
                size = float(trade.get("size", 0))
                update_candle(price, size)

    except Exception as error:
        last_error = str(error)
        logging.exception("Error procesando mensaje")


def on_open(ws):
    global connection_status

    connection_status = "CONECTADO A COINBASE"

    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": [PRODUCT],
        "channel": "ticker"
    }))

    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": [PRODUCT],
        "channel": "market_trades"
    }))


def on_error(ws, error):
    global connection_status

    connection_status = "ERROR DE CONEXION"
    logging.error("Coinbase WebSocket: %s", error)


def on_close(ws, *args):
    global connection_status
    connection_status = "RECONECTANDO"


def coinbase_worker():
    global connection_status

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:
            logging.exception("Error WebSocket")

        connection_status = "RECONECTANDO"
        time.sleep(5)


def calculate_signal():
    data, price = get_market_data()

    if len(data) < 25 or price <= 0:
        return {
            "bullish": 0,
            "bearish": 0,
            "neutral": 100,
            "signal": "INDECISO",
            "reason": "Esperando datos suficientes",
            "rsi": 50,
            "ema9": 0,
            "ema21": 0,
            "vwap": 0
        }

    closes = [c["close"] for c in data]

    ema9 = calculate_ema(closes, 9)[-1]
    ema21 = calculate_ema(closes, 21)[-1]
    rsi = calculate_rsi(closes)
    vwap = calculate_vwap(data)

    score = 0

    if ema9 > ema21:
        score += 2
    elif ema9 < ema21:
        score -= 2

    if price > vwap:
        score += 1
    elif price < vwap:
        score -= 1

    if rsi >= 55:
        score += 1
    elif rsi <= 45:
        score -= 1

    if len(closes) >= 6:
        change = closes[-1] - closes[-6]

        if change > 0:
            score += 1
        elif change < 0:
            score -= 1

    bullish = max(5, min(90, 50 + score * 8))
    bearish = max(5, min(90, 50 - score * 8))
    neutral = max(10, 100 - bullish - bearish)

    total = bullish + bearish + neutral

    bullish = round(bullish / total * 100)
    bearish = round(bearish / total * 100)
    neutral = 100 - bullish - bearish

    signal = "INDECISO"

    if bullish >= 60 and bullish > bearish:
        signal = "ALCISTA"
    elif bearish >= 60 and bearish > bullish:
        signal = "BAJISTA"

    return {
        "bullish": bullish,
        "bearish": bearish,
        "neutral": neutral,
        "signal": signal,
        "rsi": round(rsi, 2),
        "ema9": round(ema9, 2),
        "ema21": round(ema21, 2),
        "vwap": round(vwap, 2)
    }


HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC RADAR 15M</title>

<script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>

<style>
*{box-sizing:border-box}

body{
margin:0;
padding:14px;
background:#080e19;
color:white;
font-family:Arial,sans-serif;
}

header{text-align:center}

h1{
color:#00dcff;
font-size:25px;
letter-spacing:3px;
}

#status{
color:#00d9a0;
font-size:12px;
}

.price{
text-align:center;
font-size:32px;
font-weight:bold;
margin:20px 0;
}

.panel{
background:#111c2e;
border:1px solid #253650;
border-radius:15px;
padding:14px;
margin-bottom:14px;
}

.signals{
display:grid;
grid-template-columns:repeat(3,1fr);
gap:7px;
}

.signal{
text-align:center;
padding:13px 3px;
background:#1b2a40;
border-radius:12px;
}

.signal strong{
font-size:11px;
display:block;
}

.signal span{
font-size:25px;
font-weight:bold;
}

.green{color:#00e6a0}
.red{color:#ff5268}
.yellow{color:#ffc857}

#mainSignal{
text-align:center;
font-size:25px;
font-weight:bold;
margin-top:17px;
}

#chart{
height:300px;
width:100%;
}

.metrics{
display:grid;
grid-template-columns:1fr 1fr;
gap:9px;
}

.metric{
padding:13px;
background:#1b2a40;
border-radius:10px;
font-size:12px;
color:#aebbd0;
}

.metric b{
display:block;
color:white;
font-size:17px;
margin-top:6px;
}

#timer{
font-size:35px;
text-align:center;
font-weight:bold;
color:#00dcff;
}

.small{
font-size:11px;
color:#8d9bb0;
line-height:1.5;
}
</style>
</head>

<body>

<header>
<h1>BTC RADAR</h1>
<div id="status">CONECTANDO CON COINBASE</div>
</header>

<div class="price" id="price">$0.00</div>

<div class="panel">
<div class="signals">

<div class="signal">
<strong class="green">ALCISTA</strong>
<span class="green" id="bull">0%</span>
</div>

<div class="signal">
<strong class="red">BAJISTA</strong>
<span class="red" id="bear">0%</span>
</div>

<div class="signal">
<strong class="yellow">INDECISO</strong>
<span class="yellow" id="neutral">100%</span>
</div>

</div>

<div id="mainSignal" class="yellow">INDECISO</div>
</div>

<div class="panel">
<h3>GRÁFICA BTC/USD · 1 MIN</h3>
<div id="chart"></div>
</div>

<div class="panel">
<h3>INDICADORES TÉCNICOS</h3>

<div class="metrics">
<div class="metric">RSI 14<b id="rsi">--</b></div>
<div class="metric">EMA 9<b id="ema9">--</b></div>
<div class="metric">EMA 21<b id="ema21">--</b></div>
<div class="metric">VWAP<b id="vwap">--</b></div>
</div>
</div>

<div class="panel">
<h3>CUENTA REGRESIVA 15 MIN</h3>
<div id="timer">15:00</div>
</div>

<div class="panel">
<div class="small">
Fuente de datos: Coinbase BTC-USD.<br>
Las señales son estimaciones técnicas y no garantías de resultados.
El precio de referencia de Kalshi puede diferir del de Coinbase.
</div>
</div>

<script>

const chartElement=document.getElementById("chart");

const chart=LightweightCharts.createChart(chartElement,{
width:chartElement.clientWidth,
height:300,
layout:{
background:{color:"#111c2e"},
textColor:"#dce5f2"
},
grid:{
vertLines:{color:"#253650"},
horzLines:{color:"#253650"}
},
timeScale:{
timeVisible:true,
secondsVisible:false
},
rightPriceScale:{
borderColor:"#253650"
}
});

const series=chart.addSeries(
LightweightCharts.CandlestickSeries,{
upColor:"#00e6a0",
downColor:"#ff5268",
borderUpColor:"#00e6a0",
borderDownColor:"#ff5268",
wickUpColor:"#00e6a0",
wickDownColor:"#ff5268"
});

let lastCandleTime=0;

function money(value){
return Number(value).toLocaleString("en-US",{
minimumFractionDigits:2,
maximumFractionDigits:2
});
}

async function refresh(){

try{

const response=await fetch("/api/data");

if(!response.ok){
throw new Error("Error HTTP "+response.status);
}

const d=await response.json();

document.getElementById("status").textContent=d.status;

document.getElementById("price").textContent=
"$"+money(d.price);

const s=d.signal;

document.getElementById("bull").textContent=
s.bullish+"%";

document.getElementById("bear").textContent=
s.bearish+"%";

document.getElementById("neutral").textContent=
s.neutral+"%";

const main=document.getElementById("mainSignal");

main.textContent=s.signal;

main.className=
s.signal==="ALCISTA"?"green":
s.signal==="BAJISTA"?"red":"yellow";

document.getElementById("rsi").textContent=s.rsi;
document.getElementById("ema9").textContent=
s.ema9?money(s.ema9):"--";

document.getElementById("ema21").textContent=
s.ema21?money(s.ema21):"--";

document.getElementById("vwap").textContent=
s.vwap?money(s.vwap):"--";

if(d.candles && d.candles.length){

const formatted=d.candles.map(c=>({
time:c.time,
open:c.open,
high:c.high,
low:c.low,
close:c.close
}));

series.setData(formatted);

}

}catch(error){
document.getElementById("status").textContent=
"ERROR ACTUALIZANDO DATOS";

console.error(error);

}

}

function timer(){

const now=Math.floor(Date.now()/1000);

const remaining=900-(now%900);

const m=Math.floor(remaining/60);
const s=remaining%60;

document.getElementById("timer").textContent=
String(m).padStart(2,"0")+":"+
String(s).padStart(2,"0");

}

setInterval(refresh,2000);
setInterval(timer,1000);

refresh();
timer();

window.addEventListener("resize",()=>{
chart.applyOptions({
width:chartElement.clientWidth
});
});

</script>

</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():

    data, price = get_market_data()

    return jsonify({
        "price": price,
        "status": connection_status,
        "last_update": last_update,
        "candles": data[-150:],
        "signal": calculate_signal()
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


def start_background():
    global started

    if started:
        return

    started = True

    load_history()

    threading.Thread(
        target=coinbase_worker,
        daemon=True
    ).start()


start_background()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False
    )
