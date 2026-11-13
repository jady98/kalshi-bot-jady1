import os
import json
import time
import math
import base64
import threading
from collections import deque

import requests
import websocket
from flask import Flask, jsonify, render_template_string
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

app = Flask(__name__)

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"
KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

PRODUCT = "BTC-USD"
INDEX_ID = "BRTI"
MAX_CANDLES = 300
STALE_SECONDS = 15

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n")

lock = threading.RLock()

brti_candles = deque(maxlen=MAX_CANDLES)
coinbase_candles = deque(maxlen=MAX_CANDLES)
brti_ticks = deque(maxlen=120)

brti_price = None
brti_time = 0
coinbase_price = None
coinbase_time = 0

kalshi_connected = False
coinbase_connected = False
kalshi_error = ""
coinbase_error = ""

started = False
session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadar/6.0"})


def kalshi_headers():
    if not API_KEY_ID or not PRIVATE_KEY_PEM:
        raise RuntimeError(
            "Faltan KALSHI_API_KEY_ID o KALSHI_PRIVATE_KEY en Render"
        )

    private_key = serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode("utf-8"),
        password=None
    )

    timestamp = str(int(time.time() * 1000))
    message = (
        timestamp + "GET" + KALSHI_WS_PATH
    ).encode("utf-8")

    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=32
        ),
        hashes.SHA256()
    )

    return [
        "KALSHI-ACCESS-KEY: " + API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP: " + timestamp,
        "KALSHI-ACCESS-SIGNATURE: " +
        base64.b64encode(signature).decode("utf-8")
    ]


def update_candle(target, price, timestamp=None, volume=0):
    if timestamp is None:
        timestamp = time.time()

    price = float(price)

    if not math.isfinite(price) or price <= 0:
        return

    bucket = int(timestamp // 60) * 60

    if target and int(target[-1]["time"]) == bucket:
        candle = target[-1]
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] += float(volume or 0)
    else:
        target.append({
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": float(volume or 0)
        })


def set_brti(price, timestamp=None):
    global brti_price, brti_time

    price = float(price)

    if not math.isfinite(price) or price <= 0:
        return

    if timestamp is None:
        timestamp = time.time()

    with lock:
        brti_price = price
        brti_time = float(timestamp)
        brti_ticks.append((float(timestamp), price))
        update_candle(brti_candles, price, timestamp)


def set_coinbase(price, timestamp=None):
    global coinbase_price, coinbase_time

    price = float(price)

    if not math.isfinite(price) or price <= 0:
        return

    if timestamp is None:
        timestamp = time.time()

    with lock:
        coinbase_price = price
        coinbase_time = float(timestamp)
        update_candle(coinbase_candles, price, timestamp)


def calculate_ema(values, period):
    if len(values) < period:
        return None

    alpha = 2 / (period + 1)
    result = float(values[0])

    for value in values[1:]:
        result = alpha * float(value) + (1 - alpha) * result

    return result


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None

    changes = [
        float(values[i]) - float(values[i - 1])
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
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_vwap(history):
    rows = [
        c for c in history
        if float(c.get("volume", 0)) > 0
    ]

    total = sum(float(c["volume"]) for c in rows)

    if total <= 0:
        return None

    weighted = sum(
        (
            float(c["high"]) +
            float(c["low"]) +
            float(c["close"])
        ) / 3 * float(c["volume"])
        for c in rows
    )

    return weighted / total


def get_indicators(price, history, tick_history, seconds_to_close=900):
    closes = [float(c["close"]) for c in history]

    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes)
    vwap = calculate_vwap(history)

    ticks = list(tick_history)
    tick_momentum = None
    elapsed_minutes = 0.0

    if len(ticks) >= 2:
        first_time, first_price = ticks[0]
        last_time, last_price = ticks[-1]

        elapsed_minutes = max(
            (last_time - first_time) / 60.0,
            0.25
        )

        if first_price > 0:
            tick_momentum = (
                (last_price / first_price) - 1
            ) * 100

    score = 0.0
    possible = 0.0
    confirmations = []

    if tick_momentum is not None:
        possible += 3

        if tick_momentum > 0.00001:
            score += 3
            confirmations.append("BRTI EN MOVIMIENTO ALCISTA")

        elif tick_momentum < -0.00001:
            score -= 3
            confirmations.append("BRTI EN MOVIMIENTO BAJISTA")

        else:
            confirmations.append("BRTI SIN CAMBIO NETO")

    if len(ticks) >= 3:
        recent = [float(item[1]) for item in ticks[-3:]]
        possible += 1

        if recent[-1] > recent[0]:
            score += 1
            confirmations.append("ÚLTIMOS TICKS ALCISTAS")

        elif recent[-1] < recent[0]:
            score -= 1
            confirmations.append("ÚLTIMOS TICKS BAJISTAS")

    if ema9 is not None and ema21 is not None:
        possible += 1

        if ema9 > ema21:
            score += 1
            confirmations.append("EMA ALCISTA")

        elif ema9 < ema21:
            score -= 1
            confirmations.append("EMA BAJISTA")

    if rsi is not None:
        possible += 1

        if rsi >= 55:
            score += 1
            confirmations.append("RSI ALCISTA")

        elif rsi <= 45:
            score -= 1
            confirmations.append("RSI BAJISTA")

    if vwap is not None:
        possible += 1

        if price is not None and price > vwap:
            score += 1
            confirmations.append("SOBRE VWAP")

        elif price is not None and price < vwap:
            score -= 1
            confirmations.append("BAJO VWAP")

    normalized = score / possible if possible else 0
    enough_data = len(closes) >= 22

    if price is None or price <= 0:
        direction = "ESPERANDO BRTI"
        stage = "ESPERANDO EL PRIMER PRECIO"

    elif len(ticks) < 2:
        direction = "INDECISO"
        stage = "LECTURA INICIAL · ESPERANDO MOVIMIENTO"

    else:
        direction = (
            "ALCISTA" if normalized > 0.03 else
            "BAJISTA" if normalized < -0.03 else
            "INDECISO"
        )

        stage = (
            "LECTURA EN VIVO · HISTORIAL AMPLIADO"
            if enough_data else
            "LECTURA EN VIVO · ACTUALIZACIÓN POR TICK"
        )

    bullish_score = max(
        0,
        min(100, round(50 + normalized * 50))
    )

    bearish_score = 100 - bullish_score

    predicted_price = (
        price if price is not None and price > 0 else None
    )

    if predicted_price is not None and tick_momentum is not None:
        remaining_minutes = max(
            0,
            min(15, float(seconds_to_close) / 60)
        )

        rate_per_minute = (
            tick_momentum / max(elapsed_minutes, 0.25)
        )

        projected_change = max(
            -0.5,
            min(
                0.5,
                rate_per_minute * remaining_minutes * 0.15
            )
        )

        predicted_price = price * (
            1 + projected_change / 100
        )

    return {
        "direction": direction,
        "signal_stage": stage,
        "predicted_price": (
            round(predicted_price, 2)
            if predicted_price is not None else None
        ),
        "bullish_score": bullish_score,
        "bearish_score": bearish_score,
        "technical_strength": round(abs(normalized) * 100),
        "ema9": round(ema9, 2) if ema9 is not None else None,
        "ema21": round(ema21, 2) if ema21 is not None else None,
        "rsi": round(rsi, 2) if rsi is not None else None,
        "vwap": round(vwap, 2) if vwap is not None else None,
        "momentum_pct": (
            round(tick_momentum, 5)
            if tick_momentum is not None else None
        ),
        "confirmations": confirmations,
        "enough_data": enough_data,
        "brti_tick_count": len(ticks)
    }


def load_coinbase_history():
    global coinbase_error

    try:
        response = session.get(
            f"{COINBASE_REST}/products/{PRODUCT}/candles",
            params={"granularity": 60},
            timeout=15
        )

        response.raise_for_status()

        rows = []

        for item in response.json():
            if len(item) < 6:
                continue

            rows.append({
                "time": int(item[0]),
                "low": float(item[1]),
                "high": float(item[2]),
                "open": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5])
            })

        rows.sort(key=lambda x: x["time"])

        with lock:
            coinbase_candles.clear()
            coinbase_candles.extend(rows[-MAX_CANDLES:])
            coinbase_error = ""

    except Exception as exc:
        with lock:
            coinbase_error = "Historial Coinbase: " + str(exc)


def coinbase_open(ws):
    global coinbase_connected

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

    with lock:
        coinbase_connected = True


def coinbase_message(ws, message):
    global coinbase_error

    try:
        data = json.loads(message)

        if data.get("channel") == "ticker":
            for event in data.get("events", []):
                for item in event.get("tickers", []):
                    if item.get("product_id") == PRODUCT:
                        set_coinbase(item["price"])

        with lock:
            coinbase_error = ""

    except Exception as exc:
        with lock:
            coinbase_error = str(exc)


def set_coinbase_error(error):
    global coinbase_connected, coinbase_error

    with lock:
        coinbase_connected = False
        coinbase_error = str(error)


def set_coinbase_status(value):
    global coinbase_connected

    with lock:
        coinbase_connected = value


def coinbase_worker():
    global coinbase_connected, coinbase_error

    while True:
        try:
            ws = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=coinbase_open,
                on_message=coinbase_message,
                on_error=lambda ws, err: set_coinbase_error(err),
                on_close=lambda ws, code, msg: set_coinbase_status(False)
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            with lock:
                coinbase_connected = False
                coinbase_error = str(exc)

        time.sleep(5)


def kalshi_open(ws):
    global kalshi_connected, kalshi_error

    ws.send(json.dumps({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["cfbenchmarks_value"],
            "index_ids": [INDEX_ID]
        }
    }))

    with lock:
        kalshi_connected = True
        kalshi_error = ""


def find_brti_value(data):
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (ValueError, TypeError):
            return None

    if isinstance(data, list):
        for item in data:
            found = find_brti_value(item)

            if found is not None:
                return found

        return None

    if not isinstance(data, dict):
        return None

    index_id = data.get("index_id") or data.get("ticker")

    if index_id == INDEX_ID:
        raw = data.get("data")

        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                raw = None

        if isinstance(raw, dict):
            value = raw.get("value")

            timestamp = (
                raw.get("ts_ms") or
                raw.get("time") or
                data.get("received_at")
            )

            if value is not None:
                return value, timestamp

        average = data.get("avg_60s_data")

        if isinstance(average, dict):
            value = average.get("value")

            if value is not None:
                return value, data.get("received_at")

        value = data.get("value")

        if value is not None:
            timestamp = (
                data.get("ts_ms") or
                data.get("time") or
                data.get("timestamp") or
                data.get("received_at")
            )

            return value, timestamp

    for key in ("msg", "data", "payload"):
        nested = data.get(key)

        if isinstance(nested, (dict, list, str)):
            found = find_brti_value(nested)

            if found is not None:
                return found

    return None


def kalshi_message(ws, message):
    global kalshi_error

    try:
        data = json.loads(message)

        print(
            "KALSHI MENSAJE:",
            json.dumps(data)[:2000],
            flush=True
        )

        if isinstance(data, dict):
            if data.get("type") == "error":
                with lock:
                    kalshi_error = str(data.get("msg", data))

                print(
                    "KALSHI ERROR:",
                    kalshi_error,
                    flush=True
                )

            if data.get("type") in ("subscribed", "subscription"):
                print(
                    "KALSHI SUSCRIPCIÓN:",
                    data,
                    flush=True
                )

        found = find_brti_value(data)

        if found is not None:
            value, timestamp = found

            timestamp = (
                float(timestamp)
                if timestamp else time.time()
            )

            if timestamp > 10_000_000_000:
                timestamp /= 1000

            set_brti(value, timestamp)

            with lock:
                kalshi_error = ""

            print("BRTI RECIBIDO:", value, flush=True)

    except Exception as exc:
        with lock:
            kalshi_error = str(exc)

        print(
            "ERROR LEYENDO KALSHI:",
            str(exc),
            flush=True
        )


def set_kalshi_error(error):
    global kalshi_connected, kalshi_error

    with lock:
        kalshi_connected = False
        kalshi_error = str(error)


def set_kalshi_status(value):
    global kalshi_connected

    with lock:
        kalshi_connected = value


def kalshi_worker():
    global kalshi_connected, kalshi_error

    while True:
        try:
            headers = kalshi_headers()

            ws = websocket.WebSocketApp(
                KALSHI_WS,
                header=headers,
                on_open=kalshi_open,
                on_message=kalshi_message,
                on_error=lambda ws, err: set_kalshi_error(err),
                on_close=lambda ws, code, msg: set_kalshi_status(False)
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            with lock:
                kalshi_connected = False
                kalshi_error = str(exc)

        time.sleep(7)


def start_services():
    global started

    with lock:
        if started:
            return

        started = True

    threading.Thread(
        target=load_coinbase_history,
        daemon=True
    ).start()

    threading.Thread(
        target=coinbase_worker,
        daemon=True
    ).start()

    threading.Thread(
        target=kalshi_worker,
        daemon=True
    ).start()


def get_data():
    with lock:
        bp = brti_price
        bt = brti_time
        cp = coinbase_price
        ct = coinbase_time

        bh = [dict(c) for c in brti_candles]
        ch = [dict(c) for c in coinbase_candles]
        ticks = list(brti_ticks)

        kc = kalshi_connected
        cc = coinbase_connected
        ke = kalshi_error
        ce = coinbase_error

    now = time.time()
    countdown = 900 - (int(now) % 900)

    brti_live = bool(
        bp is not None and now - bt <= STALE_SECONDS
    )

    coinbase_live = bool(
        cp is not None and now - ct <= STALE_SECONDS
    )

    indicators = get_indicators(
        bp,
        bh,
        ticks,
        countdown
    )

    difference = (
        round(cp - bp, 2)
        if cp is not None and bp is not None
        else None
    )

    return {
        "ok": True,
        "brti_price": round(bp, 2) if bp is not None else None,
        "brti_live": brti_live,
        "kalshi_connected": kc,
        "brti_age": round(now - bt, 1) if bt else None,
        "coinbase_price": round(cp, 2) if cp is not None else None,
        "coinbase_live": coinbase_live,
        "coinbase_connected": cc,
        "price_difference": difference,
        "brti_candles": bh,
        "coinbase_candles": ch,
        "countdown": countdown,
        "kalshi_error": ke,
        "coinbase_error": ce,
        **indicators
    }


@app.route("/")
def home():
    start_services()
    return render_template_string(HTML)


@app.route("/api")
def api():
    start_services()
    return jsonify(get_data())


@app.route("/health")
def health():
    data = get_data()

    return jsonify({
        "ok": True,
        "brti_live": data["brti_live"],
        "kalshi_connected": data["kalshi_connected"],
        "coinbase_live": data["coinbase_live"],
        "coinbase_connected": data["coinbase_connected"],
        "brti_price": data["brti_price"],
        "coinbase_price": data["coinbase_price"],
        "kalshi_error": data["kalshi_error"],
        "coinbase_error": data["coinbase_error"]
    })

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar</title>

<style>
*{box-sizing:border-box}

body{
    margin:0;
    padding:12px;
    background:#080d18;
    color:#edf2ff;
    font-family:Arial,sans-serif
}

main{
    max-width:900px;
    margin:auto
}

.header,.row{
    display:flex;
    justify-content:space-between;
    align-items:center;
    gap:10px
}

.header{margin:5px 0 14px}

h1{
    font-size:20px;
    margin:0
}

.card{
    background:#111a2b;
    border:1px solid #263650;
    border-radius:14px;
    padding:14px;
    margin-bottom:11px;
    min-width:0
}

.label{
    font-size:11px;
    color:#91a2c1;
    letter-spacing:.5px
}

.price{
    font-size:clamp(25px,6vw,36px);
    font-weight:bold;
    margin:9px 0
}

.value{
    font-size:20px;
    font-weight:bold;
    margin-top:6px
}

.small{
    font-size:11px;
    color:#91a2c1;
    line-height:1.6
}

.green{color:#22d69a}
.red{color:#ff6475}
.yellow{color:#f4c76b}

.grid{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:10px
}

.signal{
    font-size:27px;
    font-weight:800;
    margin:9px 0
}

.bar{
    height:12px;
    background:#263650;
    border-radius:9px;
    overflow:hidden;
    display:flex;
    margin:12px 0
}

.bull{background:#22d69a}
.bear{background:#ff6475}

button{
    border:0;
    border-radius:9px;
    padding:11px 14px;
    background:#294a83;
    color:white;
    font-weight:bold
}

button:disabled{opacity:.5}

canvas{
    width:100%;
    height:300px;
    display:block;
    touch-action:none
}

.tag{
    display:inline-block;
    background:#202d45;
    border-radius:6px;
    padding:4px 7px;
    font-size:10px;
    color:#c5d2ec
}

.indicators{
    display:grid;
    grid-template-columns:repeat(2,1fr);
    gap:9px
}

.indicator{
    background:#0c1423;
    border:1px solid #263650;
    border-radius:9px;
    padding:10px;
    min-width:0
}

.indicator strong{
    display:block;
    font-size:17px;
    margin-top:6px
}

#status{
    font-size:11px;
    font-weight:bold
}

#confirmations{margin-top:8px}

@media(max-width:500px){
    body{padding:8px}
    .card{padding:11px}
    canvas{height:260px}
    h1{font-size:18px}
}
</style>
</head>

<body>
<main>

<div class="header">
    <h1>BITCOIN RADAR LIVE</h1>
    <div id="status" class="yellow">CONECTANDO...</div>
</div>

<section class="card">
    <div class="row">
        <div class="label">RADAR PRINCIPAL · ÍNDICE BRTI</div>
        <div class="row">
            <span id="brtiStatus" class="tag">ESPERANDO BRTI</span>
            <span id="brtiAge" class="small">--</span>
        </div>
    </div>
    <div class="small">
        El análisis y la estimación usan el índice BRTI en vivo.
    </div>
</section>

<div class="grid">
    <section class="card">
        <div class="label">PRECIO ACTUAL · BRTI</div>
        <div id="brti" class="price">--</div>
        <div class="small green">ÍNDICE EN VIVO</div>
    </section>

    <section class="card">
        <div class="label">ESTIMACIÓN AL CIERRE · 15 MIN</div>
        <div id="predictedPrice" class="price">--</div>
        <div id="signalStage" class="small yellow">
            ESPERANDO DATOS
        </div>
    </section>
</div>

<section class="card">
    <div class="label">LECTURA TÉCNICA DEL ÍNDICE BRTI</div>

    <div id="direction" class="signal yellow">
        ESPERANDO DATOS
    </div>

    <div class="row">
        <div>
            <div class="label">PUNTUACIÓN ALCISTA</div>
            <div id="bull" class="value green">--</div>
        </div>

        <div style="text-align:right">
            <div class="label">PUNTUACIÓN BAJISTA</div>
            <div id="bear" class="value red">--</div>
        </div>
    </div>

    <div class="bar">
        <div id="bullBar" class="bull" style="width:50%"></div>
        <div id="bearBar" class="bear" style="width:50%"></div>
    </div>

    <div class="small">
        Son puntuaciones de indicadores, no probabilidades
        reales de ganar una apuesta.
    </div>

    <div id="strength" class="small">FUERZA TÉCNICA: --</div>

    <div id="confirmations" class="small">
        Esperando confirmaciones...
    </div>
</section>

<section class="card">
    <div class="label">INDICADORES CALCULADOS SOBRE BRTI</div>

    <div class="indicators">
        <div class="indicator">
            <div class="label">EMA 9</div>
            <strong id="ema9">--</strong>
        </div>

        <div class="indicator">
            <div class="label">EMA 21</div>
            <strong id="ema21">--</strong>
        </div>

        <div class="indicator">
            <div class="label">RSI 14</div>
            <strong id="rsi">--</strong>
        </div>

        <div class="indicator">
            <div class="label">VWAP</div>
            <strong id="vwap">--</strong>
        </div>

        <div class="indicator">
            <div class="label">MOMENTUM</div>
            <strong id="momentum">--</strong>
        </div>

        <div class="indicator">
            <div class="label">HISTORIAL BRTI</div>
            <strong id="candleCount">0</strong>
        </div>
    </div>
</section>

<section class="card">
    <div class="row">
        <div>
            <div class="label">GRÁFICA EN VIVO · COINBASE</div>
            <div class="small">
                BTC/USD · Velas de 1 minuto · EMA 9 y EMA 21
            </div>
        </div>

        <button id="refreshButton" onclick="refreshData(true)">
            ACTUALIZAR
        </button>
    </div>

    <div style="height:9px"></div>

    <canvas id="chart" width="1000" height="430"></canvas>

    <div class="row">
        <span class="small">Arrastra para mover</span>
        <button onclick="goLive()">VOLVER A EN VIVO</button>
    </div>

    <div class="small">
        También puedes usar dos dedos para acercar o alejar.
        La gráfica es de Coinbase; las señales se calculan con BRTI.
    </div>
</section>

<section class="card">
    <div class="row">
        <div>
            <div class="label">CIERRE DEL BLOQUE DE 15 MINUTOS</div>
            <div id="countdown" class="signal">15:00</div>
        </div>

        <div style="text-align:right">
            <div class="label">DATOS KALSHI</div>
            <div id="kalshiConnection" class="value yellow">--</div>
        </div>
    </div>

    <div class="small" id="coinbaseConnection"></div>
    <div class="small" id="errors"></div>
</section>

</main>

<script>
const $ = id => document.getElementById(id);

let chartData = [];
let zoom = 1;
let offset = 0;
let pointers = new Map();
let initialDistance = 0;
let initialZoom = 1;
let previousX = 0;
let busy = false;

function money(value){
    if(value === null || value === undefined) return "--";

    return Number(value).toLocaleString("en-US",{
        style:"currency",
        currency:"USD",
        maximumFractionDigits:2
    });
}

function percent(value){
    if(value === null || value === undefined) return "--";
    return Number(value).toFixed(1) + "%";
}

function clock(seconds){
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;

    return String(m).padStart(2,"0") + ":" +
           String(s).padStart(2,"0");
}

function emaSeries(values, period){
    if(!values.length) return [];

    const alpha = 2 / (period + 1);
    let result = values[0];
    const output = [result];

    for(let i=1;i<values.length;i++){
        result = alpha * values[i] + (1-alpha) * result;
        output.push(result);
    }

    return output;
}

function drawChart(data){
    chartData = data || [];

    const canvas = $("chart");
    const ctx = canvas.getContext("2d");
    const W = canvas.width;
    const H = canvas.height;

    ctx.clearRect(0,0,W,H);

    if(!chartData.length){
        ctx.fillStyle = "#91a2c1";
        ctx.font = "18px Arial";
        ctx.fillText("ESPERANDO VELAS DE COINBASE...",25,45);
        return;
    }

    const count = Math.max(
        12,
        Math.min(100,Math.floor(90/zoom))
    );

    const center = Math.floor(count * 0.55);
    const visibleCount = Math.min(
        chartData.length,
        center+1
    );

    offset = Math.max(
        0,
        Math.min(offset,chartData.length-1)
    );

    const end = chartData.length - offset;
    const start = Math.max(0,end-visibleCount);
    const visible = chartData.slice(start,end);

    const closes = chartData.map(c => Number(c.close));
    const e9 = emaSeries(closes,9).slice(start,end);
    const e21 = emaSeries(closes,21).slice(start,end);

    const values = visible.flatMap(c => [
        Number(c.high),Number(c.low)
    ]).concat(e9,e21);

    let min = Math.min(...values);
    let max = Math.max(...values);

    const padding = Math.max((max-min)*0.13,2);
    min -= padding;
    max += padding;

    const left = 90;
    const right = W - 12;
    const top = 28;
    const bottom = H - 28;
    const usable = bottom-top;

    const y = p => bottom - ((p-min)/(max-min))*usable;
    const step = (right-left)/count;
    const candleWidth = Math.max(
        3,
        Math.min(18,step*.62)
    );

    ctx.font = "13px Arial";
    ctx.strokeStyle = "#263650";
    ctx.lineWidth = 1;

    for(let i=0;i<5;i++){
        const yy = top + usable*i/4;

        ctx.beginPath();
        ctx.moveTo(left,yy);
        ctx.lineTo(right,yy);
        ctx.stroke();

        const p = max - (max-min)*i/4;

        ctx.fillStyle = "#91a2c1";
        ctx.fillText(money(p),3,yy-3);
    }

    function candleX(i){
        return left + (
            center-(visible.length-1-i)+.5
        )*step;
    }

    visible.forEach((c,i)=>{
        const x = candleX(i);

        const open = Number(c.open);
        const close = Number(c.close);
        const high = Number(c.high);
        const low = Number(c.low);

        const color = close >= open ? "#22d69a" : "#ff6475";

        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        ctx.lineWidth = 2;

        ctx.beginPath();
        ctx.moveTo(x,y(high));
        ctx.lineTo(x,y(low));
        ctx.stroke();

        ctx.fillRect(
            x-candleWidth/2,
            Math.min(y(open),y(close)),
            candleWidth,
            Math.max(2,Math.abs(y(open)-y(close)))
        );
    });

    function line(series,color){
        if(!series.length) return;

        ctx.beginPath();
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;

        series.forEach((v,i)=>{
            const x = candleX(i);

            if(i===0) ctx.moveTo(x,y(v));
            else ctx.lineTo(x,y(v));
        });

        ctx.stroke();
    }

    line(e9,"#69a8ff");
    line(e21,"#f4a43b");

    ctx.fillStyle = "#69a8ff";
    ctx.fillText("EMA 9",left+5,17);

    ctx.fillStyle = "#f4a43b";
    ctx.fillText("EMA 21",left+65,17);

    if(offset===0){
        const last = chartData[chartData.length-1];
        const yy = y(Number(last.close));

        ctx.setLineDash([5,5]);
        ctx.strokeStyle = "#d6e2ff";

        ctx.beginPath();
        ctx.moveTo(left,yy);
        ctx.lineTo(right,yy);
        ctx.stroke();

        ctx.setLineDash([]);
    }
}

function goLive(){
    offset = 0;
    zoom = 1;
    drawChart(chartData);
}

function distance(a,b){
    return Math.hypot(a.x-b.x,a.y-b.y);
}

const canvas = $("chart");

canvas.addEventListener("pointerdown",e=>{
    canvas.setPointerCapture(e.pointerId);

    pointers.set(e.pointerId,{
        x:e.clientX,
        y:e.clientY
    });

    if(pointers.size===1){
        previousX=e.clientX;
    }

    if(pointers.size===2){
        const p=[...pointers.values()];
        initialDistance=distance(p[0],p[1]);
        initialZoom=zoom;
    }
});

canvas.addEventListener("pointermove",e=>{
    if(!pointers.has(e.pointerId)) return;

    pointers.set(e.pointerId,{
        x:e.clientX,
        y:e.clientY
    });

    if(pointers.size===2){
        const p=[...pointers.values()];
        const d=distance(p[0],p[1]);

        if(initialDistance>0){
            zoom=Math.max(
                .5,
                Math.min(8,initialZoom*d/initialDistance)
            );

            drawChart(chartData);
        }

    }else if(pointers.size===1){
        const delta=e.clientX-previousX;
        previousX=e.clientX;

        const count=Math.max(
            12,
            Math.min(100,Math.floor(90/zoom))
        );

        const step=canvas.clientWidth/count;
        offset += -delta/Math.max(1,step);

        offset=Math.max(
            0,
            Math.min(offset,chartData.length-1)
        );

        drawChart(chartData);
    }
});

function releasePointer(e){
    pointers.delete(e.pointerId);

    if(pointers.size<2){
        initialDistance=0;
    }
}

canvas.addEventListener("pointerup",releasePointer);
canvas.addEventListener("pointercancel",releasePointer);


async function refreshData(manual=false){
    if(busy) return;

    busy=true;

    const button=$("refreshButton");

    if(manual){
        button.disabled=true;
        button.textContent="ACTUALIZANDO";
    }

    try{
        const response=await fetch(
            "/api?_="+Date.now(),
            {cache:"no-store"}
        );

        if(!response.ok){
            throw new Error("HTTP "+response.status);
        }

        const d=await response.json();

        $("brti").textContent=money(d.brti_price);
        $("predictedPrice").textContent=money(d.predicted_price);

        $("signalStage").textContent=
            d.signal_stage || "ESPERANDO DATOS";

        $("brtiStatus").textContent=
            d.brti_live ? "BRTI EN VIVO" : "BRTI SIN DATOS";

        $("brtiStatus").className=
            d.brti_live ? "tag green" : "tag red";

        $("brtiAge").textContent=
            d.brti_age==null
            ? "--"
            : "Dato hace "+d.brti_age+" s";

        $("direction").textContent=
            d.direction || "INDECISO";

        const directionClass=
            d.direction==="ALCISTA" ? "green" :
            d.direction==="BAJISTA" ? "red" : "yellow";

        $("direction").className="signal "+directionClass;

        $("bull").textContent=percent(d.bullish_score);
        $("bear").textContent=percent(d.bearish_score);

        $("bullBar").style.width=(d.bullish_score||0)+"%";
        $("bearBar").style.width=(d.bearish_score||0)+"%";

        $("strength").textContent=
            "FUERZA TÉCNICA: "+
            (d.technical_strength ?? "--")+"%";

        $("ema9").textContent=money(d.ema9);
        $("ema21").textContent=money(d.ema21);

        $("rsi").textContent=
            d.rsi==null ? "--" : d.rsi;

        $("vwap").textContent=money(d.vwap);

        $("momentum").textContent=
            d.momentum_pct==null
            ? "--"
            : Number(d.momentum_pct).toFixed(5)+"%";

        $("candleCount").textContent=
            (d.brti_candles || []).length;

        $("confirmations").textContent=
            d.confirmations && d.confirmations.length
            ? d.confirmations.join(" · ")
            : "Esperando suficientes confirmaciones de BRTI.";

        $("countdown").textContent=clock(d.countdown||0);

        $("kalshiConnection").textContent=
            d.kalshi_connected ? "CONECTADO" : "DESCONECTADO";

        $("kalshiConnection").className=
            d.kalshi_connected ? "value green" : "value red";

        $("coinbaseConnection").textContent=
            "Coinbase WebSocket: "+
            (d.coinbase_connected ? "CONECTADO" : "DESCONECTADO");

        $("errors").textContent=
            [d.kalshi_error,d.coinbase_error]
            .filter(Boolean).join(" | ");

        const allLive=d.brti_live && d.coinbase_live;

        $("status").textContent=
            allLive ? "● FUENTES EN VIVO" :
            d.brti_live ? "● BRTI EN VIVO" :
            "● ESPERANDO DATOS";

        $("status").className=
            d.brti_live ? "green" : "yellow";

        drawChart(d.coinbase_candles);

    }catch(err){
        $("status").textContent="ERROR DE CONEXIÓN";
        $("errors").textContent=String(err);

    }finally{
        busy=false;

        if(manual){
            button.disabled=false;
            button.textContent="ACTUALIZAR";
        }
    }
}

refreshData();
setInterval(()=>refreshData(false),1000);

</script>
</body>
</html>
"""


if __name__ == "__main__":
    start_services()
    app.run(
        host="0.0.0.0",
        port=10000,
        threaded=True
    )
