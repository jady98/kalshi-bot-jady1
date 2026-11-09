
import os
import json
import time
import threading
import logging
from datetime import datetime, timezone, timedelta
from collections import deque

import requests
import websocket

from flask import Flask, jsonify, render_template_string

# ==========================================
# BTC RADAR PRO
# CONFIGURACION PRINCIPAL
# ==========================================

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

PRODUCT = "BTC-USD"

REST_URL = (
    "https://api.exchange.coinbase.com"
    "/products/BTC-USD/candles"
)

WS_URL = "wss://advanced-trade-ws.coinbase.com"

lock = threading.RLock()

candles = deque(maxlen=500)

current_price = 0.0
previous_price = 0.0
last_update = 0

connection_status = False
last_error = ""

# ==========================================
# UTILIDADES
# ==========================================

def utc_now():
    return datetime.now(timezone.utc)


def minute_timestamp(timestamp=None):
    if timestamp is None:
        timestamp = time.time()

    return int(timestamp // 60) * 60


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ==========================================
# DESCARGAR VELAS DE COINBASE
# ==========================================

def load_historical_candles():

    global current_price

    end = utc_now()
    start = end - timedelta(minutes=300)

    params = {
        "granularity": 60,
        "start": start.isoformat(),
        "end": end.isoformat()
    }

    headers = {
        "User-Agent": "BTC-Radar-PRO/1.0"
    }

    try:

        response = requests.get(
            REST_URL,
            params=params,
            headers=headers,
            timeout=15
        )

        response.raise_for_status()

        data = response.json()

        formatted = []

        for item in data:

            if len(item) < 6:
                continue

            formatted.append({
                "time": int(item[0]),
                "low": safe_float(item[1]),
                "high": safe_float(item[2]),
                "open": safe_float(item[3]),
                "close": safe_float(item[4]),
                "volume": safe_float(item[5])
            })

        formatted.sort(key=lambda x: x["time"])

        with lock:

            candles.clear()

            for candle in formatted:
                candles.append(candle)

            if candles:
                current_price = candles[-1]["close"]

        logging.info(
            "Velas cargadas: %s",
            len(formatted)
        )

        return True

    except Exception as error:

        logging.error(
            "Error descargando velas: %s",
            error
        )

        return False


# ==========================================
# ACTUALIZAR VELA CON PRECIO EN VIVO
# ==========================================

def update_live_candle(price, size=0):

    global current_price
    global previous_price
    global last_update

    now = time.time()
    timestamp = minute_timestamp(now)

    with lock:

        previous_price = current_price
        current_price = price
        last_update = now

        if not candles:

            candles.append({
                "time": timestamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size
            })

            return

        last = candles[-1]

        if last["time"] == timestamp:

            last["high"] = max(
                last["high"],
                price
            )

            last["low"] = min(
                last["low"],
                price
            )

            last["close"] = price

            last["volume"] += size

        elif timestamp > last["time"]:

            candles.append({
                "time": timestamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size
            })


# ==========================================
# COINBASE WEBSOCKET
# ==========================================

def websocket_message(ws, message):

    global connection_status
    global last_error

    try:

        data = json.loads(message)

        if data.get("channel") == "subscriptions":
            connection_status = True

        if data.get("channel") == "market_trades":

            events = data.get("events", [])

            for event in events:

                trades = event.get("trades", [])

                for trade in trades:

                    price = safe_float(
                        trade.get("price")
                    )

                    size = safe_float(
                        trade.get("size")
                    )

                    if price > 0:
                        update_live_candle(
                            price,
                            size
                        )

        if data.get("type") == "error":

            last_error = str(data)

            logging.error(
                "Coinbase WS: %s",
                data
            )

    except Exception as error:

        logging.error(
            "Error procesando WS: %s",
            error
        )


def websocket_open(ws):

    global connection_status

    connection_status = True

    subscribe = {
        "type": "subscribe",
        "channel": "market_trades",
        "product_ids": [PRODUCT]
    }

    ws.send(json.dumps(subscribe))

    logging.info(
        "Conectado a Coinbase Advanced Trade"
    )


def websocket_close(ws, code, message):

    global connection_status

    connection_status = False

    logging.warning(
        "WebSocket desconectado: %s",
        message
    )


def websocket_error(ws, error):

    global connection_status
    global last_error

    connection_status = False
    last_error = str(error)

    logging.error(
        "WebSocket error: %s",
        error
    )


def websocket_worker():

    while True:

        try:

            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=websocket_open,
                on_message=websocket_message,
                on_close=websocket_close,
                on_error=websocket_error
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as error:

            logging.error(
                "Reconectando: %s",
                error
            )

        connection_status = False

        time.sleep(3)


# ==========================================
# INDICADOR EMA
# ==========================================

def calculate_ema(values, period):

    if not values:
        return []

    multiplier = 2 / (period + 1)

    ema = values[0]

    result = [ema]

    for value in values[1:]:

        ema = (
            value * multiplier
            + ema * (1 - multiplier)
        )

        result.append(ema)

    return result


# ==========================================
# INDICADOR RSI
# ==========================================

def calculate_rsi(values, period=14):

    if len(values) < period + 1:
        return 50.0

    gains = []
    losses = []

    for i in range(1, len(values)):

        difference = values[i] - values[i - 1]

        gains.append(max(difference, 0))
        losses.append(max(-difference, 0))

    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):

        average_gain = (
            average_gain * (period - 1)
            + gains[i]
        ) / period

        average_loss = (
            average_loss * (period - 1)
            + losses[i]
        ) / period

    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0

    rs = average_gain / average_loss

    return 100 - (100 / (1 + rs))


# ==========================================
# INDICADOR VWAP
# ==========================================

def calculate_vwap(data):

    if not data:
        return []

    cumulative_volume = 0.0
    cumulative_value = 0.0

    result = []

    for candle in data:

        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3

        volume = candle["volume"]

        cumulative_value += typical * volume
        cumulative_volume += volume

        if cumulative_volume > 0:

            result.append(
                cumulative_value / cumulative_volume
            )

        else:

            result.append(candle["close"])

    return result


# ==========================================
# INDICADOR MACD
# ==========================================

def calculate_macd(values):

    if len(values) < 26:
        return 0.0, 0.0, 0.0

    ema12 = calculate_ema(values, 12)
    ema26 = calculate_ema(values, 26)

    macd_line = [
        a - b
        for a, b in zip(ema12, ema26)
    ]

    signal_line = calculate_ema(
        macd_line,
        9
    )

    macd = macd_line[-1]
    signal = signal_line[-1]

    histogram = macd - signal

    return macd, signal, histogram


# ==========================================
# AGREGAR TEMPORALIDADES
# ==========================================

def aggregate_candles(data, minutes):

    if minutes == 1:
        return list(data)

    grouped = {}

    for candle in data:

        bucket = (
            candle["time"]
            // (minutes * 60)
        ) * (minutes * 60)

        if bucket not in grouped:

            grouped[bucket] = {
                "time": bucket,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"]
            }

        else:

            item = grouped[bucket]

            item["high"] = max(
                item["high"],
                candle["high"]
            )

            item["low"] = min(
                item["low"],
                candle["low"]
            )

            item["close"] = candle["close"]

            item["volume"] += candle["volume"]

    return sorted(
        grouped.values(),
        key=lambda x: x["time"]
    )


# ==========================================
# ANALISIS Y PREDICCION
# ==========================================

def calculate_prediction(data):

    closes = [
        c["close"]
        for c in data
    ]

    if len(closes) < 30:

        return {
            "bull": 33.3,
            "bear": 33.3,
            "neutral": 33.4,
            "direction": "INDECISO",
            "score": 0
        }

    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)

    rsi = calculate_rsi(closes)

    vwap = calculate_vwap(data)

    macd, signal, histogram = calculate_macd(
        closes
    )

    score = 0.0

    # Tendencia EMA

    if ema9[-1] > ema21[-1]:
        score += 2.0
    else:
        score -= 2.0

    # Direccion de VWAP

    if closes[-1] > vwap[-1]:
        score += 1.5
    else:
        score -= 1.5

    # MACD

    if histogram > 0:
        score += 1.5
    else:
        score -= 1.5

    # Momentum reciente

    momentum = (
        closes[-1] - closes[-6]
    ) / closes[-6] * 100

    score += max(
        -2,
        min(2, momentum * 8)
    )

    # RSI

    if 50 < rsi < 70:
        score += 1.0

    elif 30 < rsi < 50:
        score -= 1.0

    elif rsi >= 75:
        score -= 1.0

    elif rsi <= 25:
        score += 1.0

    # Volatilidad y confianza

    score = max(-8, min(8, score))

    confidence = min(
        0.95,
        abs(score) / 8
    )

    bull = 50 + score * 5
    bear = 50 - score * 5

    bull = max(5, min(95, bull))
    bear = max(5, min(95, bear))

    neutral = max(
        5,
        100 - confidence * 100
    )

    total = bull + bear + neutral

    bull = bull / total * 100
    bear = bear / total * 100
    neutral = neutral / total * 100

    if bull >= bear and bull > neutral:
        direction = "ALCISTA"

    elif bear > bull and bear > neutral:
        direction = "BAJISTA"

    else:
        direction = "INDECISO"

    return {
        "bull": round(bull, 1),
        "bear": round(bear, 1),
        "neutral": round(neutral, 1),
        "direction": direction,
        "score": round(score, 2),
        "rsi": round(rsi, 2),
        "macd": round(macd, 4),
        "signal": round(signal, 4),
        "histogram": round(histogram, 4),
        "ema9": round(ema9[-1], 2),
        "ema21": round(ema21[-1], 2),
        "vwap": round(vwap[-1], 2)
    }


# ==========================================
# A PARTIR DE AQUI CONTINUA LA PARTE 2
# ==========================================

PAGE = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC Radar PRO</title>

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #03040b;
    color: #f5f5ff;
    font-family: Arial, sans-serif;
}

header {
    padding: 17px 12px;
    display: flex;
    align-items: center;
    gap: 12px;
    border-bottom: 1px solid #25233d;
}

.logo {
    background: #f7931a;
    color: white;
    border-radius: 50%;
    width: 53px;
    height: 53px;
    display: grid;
    place-items: center;
    font-size: 36px;
    font-weight: bold;
}

.brand {
    font-size: 25px;
    font-weight: 900;
    font-style: italic;
}

.brand span {
    color: #a000ff;
}

.subtitle {
    font-size: 12px;
    color: #b6b4d3;
    margin-top: 5px;
}

.live {
    margin-left: auto;
    color: #00ed82;
    font-size: 12px;
    white-space: nowrap;
}

.container {
    padding: 10px;
    max-width: 850px;
    margin: auto;
}

.panels {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 8px;
    margin-bottom: 14px;
}

.panel {
    background: #080b18;
    border: 1px solid #353451;
    border-radius: 17px;
    padding: 12px 5px;
    text-align: center;
    box-shadow: 0 0 10px #18132b;
    min-width: 0;
}

.panel.green {
    border-color: #00e88a;
    box-shadow: 0 0 12px #00a86b55;
}

.panel.red {
    border-color: #ff3158;
    box-shadow: 0 0 12px #ff315855;
}

.panel .label {
    font-size: 12px;
    color: #dedcf8;
}

.panel .percent {
    font-size: clamp(23px, 6vw, 34px);
    font-weight: 900;
    margin: 10px 0;
}

.green-text {
    color: #00ed89;
}

.red-text {
    color: #ff3158;
}

.purple-text {
    color: #bd35ff;
}

.bar {
    background: #25263a;
    height: 11px;
    border-radius: 20px;
    overflow: hidden;
}

.bar div {
    height: 100%;
    border-radius: 20px;
}

.green-fill {
    background: #00ed89;
}

.red-fill {
    background: #ff3158;
}

.purple-fill {
    background: #a000ff;
}

.prediction {
    font-size: 25px;
    font-weight: bold;
    margin: 8px 0;
    white-space: nowrap;
}

.timeframes {
    display: flex;
    align-items: center;
    justify-content: space-around;
    gap: 5px;
    padding: 10px 0;
    border-bottom: 1px solid #302c49;
    flex-wrap: wrap;
}

.tf {
    padding: 9px 13px;
    border-radius: 10px;
    font-size: 15px;
    color: #dedcf8;
    cursor: pointer;
}

.tf.active {
    background: #8500ff;
    color: white;
    box-shadow: 0 0 12px #8500ff;
}

.legend {
    display: flex;
    justify-content: center;
    gap: 10px;
    flex-wrap: wrap;
    font-size: 11px;
}

.dot {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    margin-right: 3px;
}

.chart-container {
    width: 100%;
    height: 380px;
    position: relative;
    margin-top: 10px;
}

canvas {
    width: 100%;
    height: 100%;
    display: block;
}

.indicators {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 8px;
    margin-top: 12px;
}

.indicator {
    background: #080b18;
    border: 1px solid #34334e;
    border-radius: 14px;
    padding: 12px 8px;
    min-width: 0;
}

.indicator h3 {
    font-size: 14px;
    margin: 0 0 9px;
}

.indicator .value {
    font-size: 22px;
    font-weight: bold;
}

.mini-chart {
    width: 100%;
    height: 45px;
    margin-top: 8px;
}

.bottom {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-top: 18px;
}

.bottom-card {
    background: #080b18;
    border: 1px solid #6533a2;
    border-radius: 19px;
    padding: 16px 12px;
    min-width: 0;
}

.bottom-card h3 {
    font-size: 14px;
    margin: 0;
}

.big {
    font-size: clamp(23px, 6vw, 34px);
    font-weight: bold;
    margin-top: 10px;
}

.buttons {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    margin-top: 20px;
}

.trade {
    border: 0;
    border-radius: 22px;
    height: 90px;
    font-size: 25px;
    font-weight: 900;
    color: white;
    cursor: pointer;
}

.up {
    background: linear-gradient(130deg,#00df80,#00a95c);
    box-shadow: 0 0 15px #00e87a66;
}

.down {
    background: linear-gradient(130deg,#ff3158,#c90035);
    box-shadow: 0 0 15px #ff315866;
}

.status {
    background: #101326;
    border: 1px solid #353451;
    border-radius: 30px;
    margin-top: 20px;
    padding: 15px 12px;
    display: flex;
    justify-content: space-around;
    gap: 8px;
    font-size: 12px;
    color: #c8c5e9;
}

.notice {
    font-size: 11px;
    color: #9b98b9;
    text-align: center;
    padding: 14px 5px;
}

@media(max-width:390px) {
    .brand {
        font-size: 20px;
    }

    .indicators {
        gap: 5px;
    }

    .indicator .value {
        font-size: 18px;
    }

    .legend {
        gap: 5px;
    }
}
</style>
</head>

<body>

<header>
    <div class="logo">₿</div>
    <div>
        <div class="brand">
            BTC Radar <span>PRO 〽</span>
        </div>
        <div class="subtitle">
            Análisis en Tiempo Real · Coinbase
        </div>
    </div>
    <div class="live">● En Vivo</div>
</header>

<div class="container">

<section class="panels">

    <div class="panel green">
        <div class="label">↗ Alcista</div>
        <div class="percent green-text" id="bull">--%</div>
        <div class="bar">
            <div class="green-fill" id="bullbar"
                 style="width:0%"></div>
        </div>
    </div>

    <div class="panel">
        <div class="label">Predicción 15 min</div>
        <div class="prediction green-text" id="direction">
            ESPERANDO
        </div>
        <div class="label" id="change">--</div>
    </div>

    <div class="panel red">
        <div class="label">↘ Bajista</div>
        <div class="percent red-text" id="bear">--%</div>
        <div class="bar">
            <div class="red-fill" id="bearbar"
                 style="width:0%"></div>
        </div>
    </div>

</section>

<div class="timeframes">

    <div class="tf active" data-tf="1">1m</div>
    <div class="tf" data-tf="3">3m</div>
    <div class="tf" data-tf="5">5m</div>
    <div class="tf" data-tf="15">15m</div>

    <div class="legend">
        <span><i class="dot" style="background:#00aaff"></i>EMA 9</span>
        <span><i class="dot" style="background:#ff941a"></i>EMA 21</span>
        <span><i class="dot" style="background:#a000ff"></i>VWAP</span>
    </div>

</div>

<div class="chart-container">
    <canvas id="chart"></canvas>
</div>

<section class="indicators">

    <div class="indicator">
        <h3>RSI (14)</h3>
        <div class="value" id="rsi"
             style="color:#00dfff">--</div>
        <canvas class="mini-chart" id="rsiChart"></canvas>
    </div>

    <div class="indicator">
        <h3>MACD</h3>
        <div class="value green-text" id="macd">--</div>
        <canvas class="mini-chart" id="macdChart"></canvas>
    </div>

    <div class="indicator">
        <h3>Volumen</h3>
        <div class="value purple-text" id="volume">--</div>
        <canvas class="mini-chart" id="volumeChart"></canvas>
    </div>

</section>

<section class="bottom">

    <div class="bottom-card">
        <h3>◴ Tiempo Restante</h3>
        <div class="big purple-text" id="countdown">
            15:00
        </div>
        <div class="bar" style="margin-top:10px">
            <div class="purple-fill" id="timebar"
                 style="width:100%"></div>
        </div>
    </div>

    <div class="bottom-card">
        <h3>⚑ Precio Actual</h3>
        <div class="big green-text" id="price">
            $--
        </div>
        <div class="green-text" id="dailychange">
            --
        </div>
    </div>

</section>

<div class="buttons">
    <button class="trade up"
            onclick="selectDirection('SUBE')">
        ↗ SUBE
    </button>

    <button class="trade down"
            onclick="selectDirection('BAJA')">
        ↘ BAJA
    </button>
</div>

<div id="selection"
     style="text-align:center;margin-top:10px;color:#aaa">
    Selección manual · No ejecuta operaciones
</div>

<div class="status">
    <span id="connection">● Conectando Coinbase</span>
    <span id="updates">● Actualización: --</span>
</div>

<div class="notice">
    BTC Radar PRO · Análisis técnico informativo.
    Las probabilidades no garantizan resultados.
</div>

</div>

<script>

let timeframe = 1;
let state = null;

const $ = id => document.getElementById(id);

function formatPrice(value) {

    return Number(value).toLocaleString(
        "en-US",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    );
}

function calculateEMA(values, period) {

    if (!values.length) return [];

    let result = [values[0]];

    let multiplier = 2 / (period + 1);

    for (let i = 1; i < values.length; i++) {

        result.push(
            values[i] * multiplier
            + result[i-1] * (1-multiplier)
        );
    }

    return result;
}

function calculateRSI(values, period=14) {

    if (values.length < period+1) return [];

    let gains = [];
    let losses = [];

    for (let i=1;i<values.length;i++) {

        let change = values[i]-values[i-1];

        gains.push(Math.max(change,0));
        losses.push(Math.max(-change,0));
    }

    let gain = gains.slice(0,period)
                    .reduce((a,b)=>a+b,0)/period;

    let loss = losses.slice(0,period)
                     .reduce((a,b)=>a+b,0)/period;

    let result = Array(period).fill(null);

    result.push(
        loss === 0 ? 100 :
        100-(100/(1+gain/loss))
    );

    for (let i=period;i<gains.length;i++) {

        gain = (
            gain*(period-1)+gains[i]
        )/period;

        loss = (
            loss*(period-1)+losses[i]
        )/period;

        result.push(
            loss === 0 ? 100 :
            100-(100/(1+gain/loss))
        );
    }

    return result;
}

function drawChart() {

    if (!state) return;

    const canvas = $("chart");
    const rect = canvas.getBoundingClientRect();

    const dpr = window.devicePixelRatio || 1;

    canvas.width = rect.width*dpr;
    canvas.height = rect.height*dpr;

    const ctx = canvas.getContext("2d");

    ctx.scale(dpr,dpr);

    const W = rect.width;
    const H = rect.height;

    ctx.clearRect(0,0,W,H);

    const data = state.timeframes[String(timeframe)] || [];

    if (data.length < 2) return;

    const closes = data.map(c=>c.close);

    const ema9 = calculateEMA(closes,9);
    const ema21 = calculateEMA(closes,21);

    const min = Math.min(...data.map(c=>c.low));
    const max = Math.max(...data.map(c=>c.high));

    const padding = (max-min)*0.12 || 1;

    const low = min-padding;
    const high = max+padding;

    const chartHeight = H-55;
    const chartWidth = W-60;

    const x = i => i*chartWidth/(data.length-1);

    const y = price =>
        chartHeight-
        ((price-low)/(high-low))*chartHeight;

    // GRID

    ctx.strokeStyle="#20253a";
    ctx.lineWidth=1;

    for(let i=0;i<=5;i++) {

        let yy=i*chartHeight/5;

        ctx.beginPath();
        ctx.moveTo(0,yy);
        ctx.lineTo(chartWidth,yy);
        ctx.stroke();

        let value = high-(high-low)*i/5;

        ctx.fillStyle="#c4c4d5";
        ctx.font="11px Arial";

        ctx.fillText(
            formatPrice(value),
            chartWidth+5,
            yy+4
        );
    }

    // EMA 9

    function line(values,color,width) {

        ctx.beginPath();
        ctx.strokeStyle=color;
        ctx.lineWidth=width;

        values.forEach((v,i)=>{

            if(i===0) ctx.moveTo(x(i),y(v));
            else ctx.lineTo(x(i),y(v));

        });

        ctx.stroke();
    }

    // VELAS

    let candleWidth=Math.max(
        2,
        Math.min(10,chartWidth/data.length*0.65)
    );

    data.forEach((c,i)=>{

        const rising=c.close>=c.open;

        const color=rising?"#00d987":"#ff3458";

        let xx=x(i);

        ctx.strokeStyle=color;
        ctx.fillStyle=color;
        ctx.lineWidth=1;

        ctx.beginPath();
        ctx.moveTo(xx,y(c.high));
        ctx.lineTo(xx,y(c.low));
        ctx.stroke();

        let top=y(Math.max(c.open,c.close));
        let bottom=y(Math.min(c.open,c.close));

        ctx.fillRect(
            xx-candleWidth/2,
            top,
            candleWidth,
            Math.max(1,bottom-top)
        );

    });

    line(ema9,"#00aaff",2);
    line(ema21,"#ff941a",2);

    // VWAP

    const vwap=calculateEMA(
        data.map(c=>c.close),1
    );

    // VWAP calculado acumulando volumen

    let totalVolume=0;
    let totalValue=0;
    let vwapValues=[];

    data.forEach(c=>{

        let typical=(c.high+c.low+c.close)/3;

        totalVolume+=c.volume;
        totalValue+=typical*c.volume;

        vwapValues.push(
            totalVolume>0 ?
            totalValue/totalVolume :
            c.close
        );

    });

    line(vwapValues,"#a000ff",2);

    // VOLUMEN

    let maxVolume=Math.max(
        ...data.map(c=>c.volume),1
    );

    data.forEach((c,i)=>{

        let height=(c.volume/maxVolume)*40;

        ctx.fillStyle=
            c.close>=c.open ?
            "#00d98799":"#ff345899";

        ctx.fillRect(
            x(i)-candleWidth/2,
            H-height,
            candleWidth,
            height
        );

    });

    // PRECIO ACTUAL

    let last=data[data.length-1].close;

    ctx.fillStyle="#00e889";
    ctx.font="bold 12px Arial";

    ctx.fillText(
        formatPrice(last),
        chartWidth+5,
        y(last)-8
    );

    drawMiniCharts(data);
}

function drawMiniCharts(data) {

    let closes=data.map(c=>c.close);

    drawMini(
        $("rsiChart"),
        calculateRSI(closes),
        "#00dfff",
        0,
        100
    );

    let volumes=data.map(c=>c.volume);

    drawMini(
        $("volumeChart"),
        volumes,
        "#a000ff"
    );

    let ema12=calculateEMA(closes,12);
    let ema26=calculateEMA(closes,26);

    drawMini(
        $("macdChart"),
        ema12.map((v,i)=>v-ema26[i]),
        "#00aaff"
    );
}

function drawMini(canvas,values,color,min=null,max=null) {

    if(!canvas || !values.length) return;

    const rect=canvas.getBoundingClientRect();

    const dpr=window.devicePixelRatio||1;

    canvas.width=rect.width*dpr;
    canvas.height=rect.height*dpr;

    const ctx=canvas.getContext("2d");

    ctx.scale(dpr,dpr);

    let valid=values.filter(v=>v!==null);

    if(!valid.length)return;

    min=min===null?Math.min(...valid):min;
    max=max===null?Math.max(...valid):max;

    if(max===min)max=min+1;

    ctx.beginPath();
    ctx.strokeStyle=color;
    ctx.lineWidth=2;

    valid.forEach((v,i)=>{

        let x=i*rect.width/Math.max(1,valid.length-1);

        let y=rect.height-
            ((v-min)/(max-min))*rect.height;

        if(i===0)ctx.moveTo(x,y);
        else ctx.lineTo(x,y);

    });

    ctx.stroke();
}

async function refresh() {

    try {

        const response=await fetch(
            "/api/state?t="+Date.now(),
            {cache:"no-store"}
        );

        if(!response.ok)throw Error("API");

        state=await response.json();

        updateDashboard();

    } catch(error) {

        $("connection").textContent=
            "● Reconectando...";

    }
}

function updateDashboard() {

    if(!state)return;

    let p=state.prediction;

    $("bull").textContent=p.bull+"%";
    $("bear").textContent=p.bear+"%";

    $("bullbar").style.width=p.bull+"%";
    $("bearbar").style.width=p.bear+"%";

    $("direction").textContent=p.direction;

    $("direction").className=
        "prediction "+
        (
            p.direction==="ALCISTA" ?
            "green-text":
            p.direction==="BAJISTA" ?
            "red-text":"purple-text"
        );

    $("change").textContent=
        "Señal técnica: "+p.score;

    $("price").textContent=
        "$"+formatPrice(state.price);

    $("dailychange").textContent=
        (state.change>=0?"+":"")+
        state.change.toFixed(2)+"%";

    $("rsi").textContent=
        p.rsi!==undefined?p.rsi:"--";

    $("macd").textContent=
        p.macd!==undefined?p.macd:"--";

    $("volume").textContent=
        state.volume.toFixed(2);

    $("connection").textContent=
        state.connected ?
        "● Conectado: Coinbase":
        "● Reconectando Coinbase";

    $("updates").textContent=
        "● Actualización: "+
        state.update_age+"s";

    drawChart();
}

function updateCountdown() {

    let now=Math.floor(Date.now()/1000);

    let remaining=900-(now%900);

    let minutes=String(
        Math.floor(remaining/60)
    ).padStart(2,"0");

    let seconds=String(
        remaining%60
    ).padStart(2,"0");

    $("countdown").textContent=
        minutes+":"+seconds;

    $("timebar").style.width=
        (remaining/900*100)+"%";
}

function selectDirection(direction) {

    $("selection").textContent=
        "Selección manual: "+direction+
        " · No ejecuta operaciones";
}

document.querySelectorAll(".tf").forEach(button=>{

    button.addEventListener("click",()=>{

        document.querySelectorAll(".tf")
            .forEach(b=>b.classList.remove("active"));

        button.classList.add("active");

        timeframe=Number(button.dataset.tf);

        drawChart();
    });

});

window.addEventListener("resize",drawChart);

setInterval(refresh,1000);

setInterval(updateCountdown,250);

refresh();
updateCountdown();

</script>

</body>
</html>
"""


# ==========================================
# API PRINCIPAL
# ==========================================

@app.route("/")
def home():

    return render_template_string(PAGE)


@app.route("/api/state")
def api_state():

    with lock:

        raw = list(candles)

        price = current_price

        connected = connection_status

        updated = last_update

    if not raw:

        return jsonify({
            "error": "Esperando datos de Coinbase"
        }), 503

    timeframes = {}

    for minutes in [1, 3, 5, 15]:

        grouped = aggregate_candles(
            raw,
            minutes
        )

        timeframes[str(minutes)] = grouped[-120:]

    data = timeframes["1"]

    prediction = calculate_prediction(data)

    first_price = raw[0]["open"]

    change = (
        (price-first_price)/first_price*100
        if first_price else 0
    )

    volume = raw[-1]["volume"]

    return jsonify({

        "price": price,

        "change": round(change, 3),

        "volume": round(volume, 3),

        "connected": connected,

        "update_age": round(
            time.time()-updated, 1
        ) if updated else 999,

        "prediction": prediction,

        "timeframes": timeframes

    })


# ==========================================
# INICIO DE SERVIDORES
# ==========================================

def start_background():

    load_historical_candles()

    thread = threading.Thread(
        target=websocket_worker,
        daemon=True
    )

    thread.start()


start_background()


if __name__ == "__main__":

    port = int(os.environ.get("PORT",5000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True
    )
