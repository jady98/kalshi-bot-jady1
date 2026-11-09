import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

PRODUCT = "BTC-USD"
WS_URL = "wss://advanced-trade-ws.coinbase.com"

candles = deque(maxlen=300)
current_price = 0.0
connection_status = "Conectando..."
last_update = 0

lock = threading.Lock()


def get_history():
    global current_price

    try:
        url = (
            "https://api.exchange.coinbase.com/products/"
            + PRODUCT
            + "/candles?granularity=60"
        )

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        with lock:
            candles.clear()

            for item in reversed(data):
                candles.append({
                    "time": item[0],
                    "low": item[1],
                    "high": item[2],
                    "open": item[3],
                    "close": item[4],
                    "volume": item[5]
                })

            if candles:
                current_price = candles[-1]["close"]

        return True

    except Exception as error:
        print("Error histórico:", error)
        return False


def calculate_ema(values, period):
    if not values:
        return []

    multiplier = 2 / (period + 1)
    ema = [values[0]]

    for price in values[1:]:
        ema.append(
            (price - ema[-1]) * multiplier + ema[-1]
        )

    return ema


def calculate_rsi(values, period=14):
    if len(values) <= period:
        return 50

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def calculate_vwap(data):
    if not data:
        return 0

    total_price_volume = 0
    total_volume = 0

    for candle in data[-100:]:
        typical_price = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3

        volume = candle["volume"]

        total_price_volume += typical_price * volume
        total_volume += volume

    if total_volume == 0:
        return data[-1]["close"]

    return total_price_volume / total_volume
    @app.route("/api/data")
def api_data():
    with lock:
        data = list(candles)
        price = current_price
        status = connection_status
        updated = last_update

    return jsonify({
        "price": price,
        "status": status,
        "last_update": updated,
        "candles": data[-150:],
        "signal": get_signal()
    })


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
margin:0;background:#080d18;color:#fff;
font-family:Arial,sans-serif;padding:14px
}
.header{text-align:center;margin-bottom:15px}
.header h1{color:#00d9ff;font-size:24px;letter-spacing:2px}
.status{color:#aaa;font-size:12px}
.price{
font-size:32px;font-weight:bold;
text-align:center;margin:18px 0
}
.panel{
background:#111b2c;border:1px solid #243650;
border-radius:15px;padding:14px;margin-bottom:13px
}
.signals{
display:grid;grid-template-columns:repeat(3,1fr);
gap:8px
}
.signal{
text-align:center;padding:13px 4px;
border-radius:12px;background:#18263a
}
.signal strong{display:block;font-size:13px}
.signal span{font-size:25px;font-weight:bold}
.bull{color:#00e69a}
.bear{color:#ff5268}
.neutral{color:#ffc857}
#chart{height:330px;width:100%}
.main-signal{
text-align:center;font-size:25px;
font-weight:bold;margin:12px
}
.metrics{
display:grid;grid-template-columns:1fr 1fr;
gap:10px
}
.metric{
background:#18263a;border-radius:10px;
padding:12px;font-size:12px;color:#aaa
}
.metric b{display:block;color:white;font-size:17px;margin-top:5px}
button{
background:#00d9ff;border:0;border-radius:9px;
padding:12px;color:#07111d;font-weight:bold;width:100%
}
.warning{font-size:11px;color:#8995a8;line-height:1.5}
</style>
</head>

<body>

<div class="header">
<h1>BTC RADAR 15M</h1>
<div class="status" id="status">Conectando con Coinbase...</div>
</div>

<div class="price" id="price">$0.00</div>

<div class="panel">
<div class="signals">
<div class="signal">
<strong class="bull">ALCISTA</strong>
<span class="bull" id="bull">0%</span>
</div>
<div class="signal">
<strong class="bear">BAJISTA</strong>
<span class="bear" id="bear">0%</span>
</div>
<div class="signal">
<strong class="neutral">INDECISO</strong>
<span class="neutral" id="neutral">100%</span>
</div>
</div>
<div class="main-signal" id="mainSignal">INDECISO</div>
</div>

<div class="panel">
<h3>GRÁFICA BTC/USD · 1 MIN</h3>
<div id="chart"></div>
</div>

<div class="panel">
<h3>INDICADORES</h3>
<div class="metrics">
<div class="metric">RSI 14<b id="rsi">--</b></div>
<div class="metric">EMA 9<b id="ema9">--</b></div>
<div class="metric">EMA 21<b id="ema21">--</b></div>
<div class="metric">VWAP<b id="vwap">--</b></div>
</div>
</div>

<div class="panel">
<h3>CUENTA REGRESIVA 15 MIN</h3>
<div class="price" id="timer">15:00</div>
</div>

<div class="panel">
<p class="warning">
Fuente de precios: Coinbase BTC-USD.
Las probabilidades son estimaciones técnicas basadas en indicadores,
no probabilidades estadísticas verificadas ni garantías de resultado.
Kalshi puede utilizar un índice de referencia diferente para resolver
sus contratos.
</p>
<button onclick="refreshData()">ACTUALIZAR RADAR</button>
</div>

<script>
const chartElement=document.getElementById('chart');

const chart=LightweightCharts.createChart(chartElement,{
width:chartElement.clientWidth,
height:330,
layout:{
background:{color:'#111b2c'},
textColor:'#d5dfec'
},
grid:{
vertLines:{color:'#1d2b40'},
horzLines:{color:'#1d2b40'}
},
rightPriceScale:{borderColor:'#33445e'},
timeScale:{borderColor:'#33445e',timeVisible:true}
});

const series=chart.addSeries(
LightweightCharts.CandlestickSeries,{
upColor:'#00d9a0',
downColor:'#ff5268',
borderUpColor:'#00d9a0',
borderDownColor:'#ff5268',
wickUpColor:'#00d9a0',
wickDownColor:'#ff5268'
});

async function refreshData(){
try{
const response=await fetch('/api/data');
const d=await response.json();

document.getElementById('status').textContent=d.status;

document.getElementById('price').textContent=
'$'+Number(d.price).toLocaleString('en-US',{
minimumFractionDigits:2,
maximumFractionDigits:2
});

document.getElementById('bull').textContent=
d.signal.bullish+'%';

document.getElementById('bear').textContent=
d.signal.bearish+'%';

document.getElementById('neutral').textContent=
d.signal.neutral+'%';

const main=document.getElementById('mainSignal');
main.textContent=d.signal.signal;

main.style.color=
d.signal.signal==='ALCISTA'?'#00e69a':
d.signal.signal==='BAJISTA'?'#ff5268':'#ffc857';

document.getElementById('rsi').textContent=
d.signal.rsi??'--';

document.getElementById('ema9').textContent=
d.signal.ema9??'--';

document.getElementById('ema21').textContent=
d.signal.ema21??'--';

document.getElementById('vwap').textContent=
d.signal.vwap??'--';

if(d.candles.length){
const formatted=d.candles.map(c=>({
time:c.time,
open:c.open,
high:c.high,
low:c.low,
close:c.close
}));

series.setData(formatted);
chart.timeScale().fitContent();
}

}catch(e){
document.getElementById('status').textContent=
'Error actualizando datos';
console.error(e);
}
}

function updateTimer(){
const now=Math.floor(Date.now()/1000);
const remaining=900-(now%900);
const minutes=Math.floor(remaining/60);
const seconds=remaining%60;

document.getElementById('timer').textContent=
String(minutes).padStart(2,'0')+':'+
String(seconds).padStart(2,'0');
}

setInterval(refreshData,2000);
setInterval(updateTimer,1000);
refreshData();
updateTimer();

window.addEventListener('resize',()=>{
chart.applyOptions({
width:chartElement.clientWidth
});
});
</script>

</body>
</html>
"""


if __name__ == "__main__":
    get_history()

    threading.Thread(
        target=coinbase_worker,
        daemon=True
    ).start()

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
