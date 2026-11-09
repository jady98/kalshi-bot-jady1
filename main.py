import os
import json
import time
import threading
import base64
import logging
import math
from collections import deque
from datetime import datetime

import requests
import websocket

from flask import Flask, jsonify, render_template_string

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ============================================================
# BITCOIN RADAR PRO BY JADY
# BRTI REAL + KALSHI BTC 15M
# ============================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY = os.getenv(
    "KALSHI_PRIVATE_KEY", ""
).replace("\\n", "\n").strip()

KALSHI_REST = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

SERIES_TICKER = "KXBTC15M"
INDEX_ID = "BRTI"

MAX_TICKS = 12000
MAX_CANDLES = 2000

lock = threading.RLock()

ticks = deque(maxlen=MAX_TICKS)
candles = deque(maxlen=MAX_CANDLES)

state = {
    "connected": False,
    "subscribed": False,
    "last_price": None,
    "last_tick_time": None,
    "error": None,
    "warning": None,
}

ws_thread_started = False

market_cache = {
    "data": None,
    "updated": 0
}


# ============================================================
# UTILIDADES
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def safe_float(value):
    try:
        if value is None:
            return None

        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    except (TypeError, ValueError):
        return None


def parse_timestamp(value):
    if value is None:
        return now_ms()

    try:
        if isinstance(value, (int, float)):
            number = int(value)
            return number if number > 10**11 else number * 1000

        if isinstance(value, str):
            value = value.strip()

            if value.isdigit():
                number = int(value)
                return number if number > 10**11 else number * 1000

            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

            return int(parsed.timestamp() * 1000)

    except Exception:
        pass

    return now_ms()


def update_state(**values):
    with lock:
        state.update(values)


# ============================================================
# VELAS REALES DE 1 MINUTO
# ============================================================

def update_candle(price, timestamp_ms):

    minute = (timestamp_ms // 60000) * 60

    with lock:

        if candles and candles[-1]["time"] == minute:

            candle = candles[-1]

            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price

        else:

            candles.append({
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price
            })


def add_brti_tick(price, timestamp):

    price = safe_float(price)

    if price is None or price <= 0:
        return

    timestamp_ms = parse_timestamp(timestamp)

    with lock:

        if ticks and timestamp_ms <= ticks[-1]["time"]:
            return

        ticks.append({
            "time": timestamp_ms,
            "price": price
        })

        state["last_price"] = price
        state["last_tick_time"] = timestamp_ms
        state["error"] = None

    update_candle(price, timestamp_ms)


# ============================================================
# AUTENTICACIÓN KALSHI WEBSOCKET
# ============================================================

def create_ws_headers():

    if not KALSHI_API_KEY_ID or not KALSHI_PRIVATE_KEY:
        raise RuntimeError(
            "Faltan KALSHI_API_KEY_ID o KALSHI_PRIVATE_KEY"
        )

    private_key = serialization.load_pem_private_key(
        KALSHI_PRIVATE_KEY.encode("utf-8"),
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
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )

    signature_b64 = base64.b64encode(
        signature
    ).decode("utf-8")

    return [
        "KALSHI-ACCESS-KEY: " + KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP: " + timestamp,
        "KALSHI-ACCESS-SIGNATURE: " + signature_b64
    ]


# ============================================================
# PROCESAMIENTO BRTI
# ============================================================

def process_brti_message(packet):

    if not isinstance(packet, dict):
        return

    msg = packet.get("msg", {})
    msg_type = packet.get("type")

    if msg_type == "subscribed":
        update_state(subscribed=True)
        return

    if msg_type == "error":
        update_state(
            error="Kalshi: " + str(
                msg.get("message") or msg
            )
        )
        return

    if msg_type != "cfbenchmarks_value":
        return

    if msg.get("index_id") != INDEX_ID:
        return

    raw_data = msg.get("data", {})

    if isinstance(raw_data, str):

        try:
            raw_data = json.loads(raw_data)

        except Exception:
            raw_data = {}

    if not isinstance(raw_data, dict):
        return

    price = safe_float(raw_data.get("value"))

    timestamp = (
        raw_data.get("time")
        or raw_data.get("timestamp")
        or msg.get("received_at")
        or packet.get("sending_ts_ms")
    )

    if price is not None:
        add_brti_tick(price, timestamp)


def on_ws_open(ws):

    update_state(
        connected=True,
        subscribed=False,
        error=None
    )

    ws.send(json.dumps({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["cfbenchmarks_value"],
            "index_ids": ["BRTI"]
        }
    }))


def on_ws_message(ws, message):

    try:

        packet = json.loads(message)

        process_brti_message(packet)

    except Exception as error:

        update_state(warning=str(error))


def on_ws_error(ws, error):

    update_state(
        connected=False,
        error=str(error)
    )


def on_ws_close(ws, code, message):

    update_state(
        connected=False,
        subscribed=False
    )


def websocket_worker():

    while True:

        try:

            headers = create_ws_headers()

            ws = websocket.WebSocketApp(
                KALSHI_WS,
                header=headers,
                on_open=on_ws_open,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as error:

            update_state(
                connected=False,
                error=str(error)
            )

            logging.exception("Error WebSocket BRTI")

        time.sleep(5)


def start_brti():

    global ws_thread_started

    if ws_thread_started:
        return

    ws_thread_started = True

    threading.Thread(
        target=websocket_worker,
        daemon=True
    ).start()


# ============================================================
# INDICADORES TÉCNICOS REALES
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return []

    multiplier = 2 / (period + 1)

    ema = sum(values[:period]) / period

    result = [None] * (period - 1)
    result.append(ema)

    for value in values[period:]:

        ema = (
            value * multiplier
            + ema * (1 - multiplier)
        )

        result.append(ema)

    return result


def calculate_rsi(values, period=14):

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        difference = values[i] - values[i - 1]

        gains.append(max(difference, 0))
        losses.append(max(-difference, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):

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


def calculate_macd(values):

    ema12 = calculate_ema(values, 12)
    ema26 = calculate_ema(values, 26)

    if not ema26:
        return None, None, None

    macd_values = []

    for a, b in zip(ema12, ema26):

        if a is None or b is None:
            continue

        macd_values.append(a - b)

    if len(macd_values) < 9:
        return None, None, None

    signal_values = calculate_ema(macd_values, 9)

    macd = macd_values[-1]
    signal = signal_values[-1]

    histogram = macd - signal

    return macd, signal, histogram


def calculate_vwap_proxy():

    with lock:
        recent_ticks = list(ticks)

    if len(recent_ticks) < 2:
        return None

    cutoff = now_ms() - 15 * 60 * 1000

    recent_ticks = [
        item for item in recent_ticks
        if item["time"] >= cutoff
    ]

    if not recent_ticks:
        return None

    # Aproximación ponderada por frecuencia de ticks.
    # No es un VWAP de volumen negociado.

    return sum(
        item["price"] for item in recent_ticks
    ) / len(recent_ticks)


# ============================================================
# MOTOR DE SEÑALES
# ============================================================

def calculate_analysis():

    with lock:

        candle_data = list(candles)
        current_price = state["last_price"]
        last_tick_time = state["last_tick_time"]
        connected = state["connected"]

    if not candle_data or current_price is None:

        return {
            "signal": "INDECISO",
            "sube": 0,
            "baja": 0,
            "indeciso": 100,
            "reason": "Esperando datos reales de BRTI",
            "strength": 0,
            "rsi": None,
            "ema9": None,
            "ema21": None,
            "macd": None,
            "macd_signal": None,
            "macd_histogram": None,
            "vwap_proxy": None,
            "prediction": None,
            "difference": None
        }

    closes = [
        candle["close"] for candle in candle_data
    ]

    if len(closes) < 26:

        return {
            "signal": "INDECISO",
            "sube": 0,
            "baja": 0,
            "indeciso": 100,
            "reason": "Reuniendo velas reales para analizar",
            "strength": 0,
            "rsi": None,
            "ema9": None,
            "ema21": None,
            "macd": None,
            "macd_signal": None,
            "macd_histogram": None,
            "vwap_proxy": calculate_vwap_proxy(),
            "prediction": None,
            "difference": None
        }

    ema9_values = calculate_ema(closes, 9)
    ema21_values = calculate_ema(closes, 21)

    ema9 = ema9_values[-1]
    ema21 = ema21_values[-1]

    rsi = calculate_rsi(closes, 14)

    macd, macd_signal, macd_histogram = (
        calculate_macd(closes)
    )

    vwap = calculate_vwap_proxy()

    if (
        ema9 is None
        or ema21 is None
        or rsi is None
        or macd_histogram is None
        or vwap is None
    ):

        return {
            "signal": "INDECISO",
            "sube": 0,
            "baja": 0,
            "indeciso": 100,
            "reason": "Indicadores todavía incompletos",
            "strength": 0,
            "rsi": rsi,
            "ema9": ema9,
            "ema21": ema21,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_histogram": macd_histogram,
            "vwap_proxy": vwap,
            "prediction": None,
            "difference": None
        }

    # Cada indicador aporta puntos a la dirección.
    score = 0.0

    score += 25 if ema9 > ema21 else -25
    score += 20 if current_price > ema9 else -20
    score += 15 if current_price > vwap else -15
    score += 20 if macd_histogram > 0 else -20

    if rsi > 55:
        score += 20
    elif rsi < 45:
        score -= 20

    score = max(-100, min(100, score))

    # No mostrar señales fuertes con datos atrasados.
    age_seconds = (
        (now_ms() - last_tick_time) / 1000
        if last_tick_time else 999999
    )

    if not connected or age_seconds > 15:

        return {
            "signal": "INDECISO",
            "sube": 0,
            "baja": 0,
            "indeciso": 100,
            "reason": "Datos BRTI atrasados o desconectados",
            "strength": 0,
            "rsi": rsi,
            "ema9": ema9,
            "ema21": ema21,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_histogram": macd_histogram,
            "vwap_proxy": vwap,
            "prediction": None,
            "difference": None
        }

    strength = abs(score) / 100

    indeciso = max(
        15,
        min(65, 45 - strength * 30)
    )

    directional = 100 - indeciso

    sube = directional * (
        0.5 + (score / 200)
    )

    baja = directional - sube

    signal = "INDECISO"

    if score >= 25:
        signal = "SUBE"

    elif score <= -25:
        signal = "BAJA"

    # Estimación sencilla usando el movimiento reciente.
    # No es una predicción garantizada.

    recent = closes[-6:]

    changes = []

    for i in range(1, len(recent)):

        previous = recent[i - 1]

        if previous > 0:

            changes.append(
                (recent[i] - previous) / previous
            )

    average_change = (
        sum(changes) / len(changes)
        if changes else 0
    )

    estimated_change = max(
        -0.005,
        min(0.005, average_change * 15)
    )

    prediction = current_price * (
        1 + estimated_change
    )

    return {
        "signal": signal,
        "sube": round(sube, 1),
        "baja": round(baja, 1),
        "indeciso": round(indeciso, 1),
        "reason": (
            "Confluencia alcista de indicadores"
            if signal == "SUBE"
            else
            "Confluencia bajista de indicadores"
            if signal == "BAJA"
            else
            "Señales mixtas: esperar confirmación"
        ),
        "strength": round(abs(score), 1),
        "rsi": round(rsi, 2),
        "ema9": ema9,
        "ema21": ema21,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "vwap_proxy": vwap,
        "prediction": round(prediction, 2),
        "difference": round(prediction - current_price, 2)
    }
    # ============================================================
# DATOS DEL CONTRATO KALSHI
# ============================================================

def get_kalshi_market():

    now = time.time()

    if (
        market_cache["data"] is not None
        and now - market_cache["updated"] < 5
    ):
        return market_cache["data"]

    try:

        response = requests.get(
            f"{KALSHI_REST}/markets",
            params={
                "series_ticker": SERIES_TICKER,
                "status": "open",
                "limit": 100
            },
            timeout=8
        )

        response.raise_for_status()

        markets = response.json().get("markets", [])

        current_time = datetime.now().timestamp()

        valid = []

        for market in markets:

            close_time = market.get("close_time")

            if not close_time:
                continue

            try:
                close_timestamp = datetime.fromisoformat(
                    close_time.replace("Z", "+00:00")
                ).timestamp()

                if close_timestamp > current_time:
                    valid.append(
                        (close_timestamp, market)
                    )

            except Exception:
                continue

        if not valid:
            return None

        valid.sort(key=lambda item: item[0])

        close_timestamp, market = valid[0]

        result = {
            "ticker": market.get("ticker"),
            "title": market.get("title"),
            "close_time": market.get("close_time"),
            "close_timestamp": close_timestamp,
            "yes_bid": safe_float(
                market.get("yes_bid_dollars")
            ),
            "yes_ask": safe_float(
                market.get("yes_ask_dollars")
            ),
            "no_bid": safe_float(
                market.get("no_bid_dollars")
            ),
            "no_ask": safe_float(
                market.get("no_ask_dollars")
            ),
            "strike": safe_float(
                market.get("floor_strike")
            ),
            "last_price": safe_float(
                market.get("last_price_dollars")
                or market.get("last_price")
            )
        }

        # Compatibilidad con respuestas antiguas en centavos.
        for key in ("yes_bid", "yes_ask", "no_bid", "no_ask"):

            if result[key] is not None and result[key] > 1:
                result[key] /= 100

        market_cache["data"] = result
        market_cache["updated"] = now

        return result

    except Exception as error:

        logging.warning(
            "Error consultando Kalshi: %s",
            error
        )

        return None


# ============================================================
# DATOS PARA EL FRONTEND
# ============================================================

def build_data():

    with lock:

        current_price = state["last_price"]
        connected = state["connected"]
        subscribed = state["subscribed"]
        last_tick = state["last_tick_time"]
        candle_data = list(candles)

    analysis = calculate_analysis()

    age = None

    if last_tick:
        age = round(
            (now_ms() - last_tick) / 1000,
            1
        )

    ema9_values = calculate_ema(
        [c["close"] for c in candle_data], 9
    )

    ema21_values = calculate_ema(
        [c["close"] for c in candle_data], 21
    )

    ema9_line = []
    ema21_line = []

    for candle, value in zip(candle_data, ema9_values):

        if value is not None:
            ema9_line.append({
                "time": candle["time"],
                "value": value
            })

    for candle, value in zip(candle_data, ema21_values):

        if value is not None:
            ema21_line.append({
                "time": candle["time"],
                "value": value
            })

    vwap = analysis.get("vwap_proxy")

    vwap_line = []

    if vwap is not None:

        vwap_line = [
            {
                "time": candle["time"],
                "value": vwap
            }
            for candle in candle_data[-100:]
        ]

    return {
        "connected": connected,
        "subscribed": subscribed,
        "price": current_price,
        "last_tick_age": age,
        "candles": candle_data[-500:],
        "ema9_line": ema9_line,
        "ema21_line": ema21_line,
        "vwap_line": vwap_line,
        "analysis": analysis,
        "error": state["error"],
        "warning": state["warning"]
    }


# ============================================================
# INTERFAZ VISUAL
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>Bitcoin Radar PRO by Jady</title>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 12px;
    background: #080c14;
    color: white;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 520px;
    margin: auto;
}

.card {
    background: #111927;
    border: 1px solid #243044;
    border-radius: 15px;
    padding: 13px;
    margin-bottom: 11px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 13px;
}

.logo {
    color: #f5b942;
    font-size: 25px;
    font-weight: 900;
}

.brand {
    font-size: 16px;
    font-weight: 900;
}

.subtitle {
    color: #8491a5;
    font-size: 10px;
    margin-top: 4px;
}

.status {
    font-size: 10px;
    color: #fbbf24;
    border: 1px solid #55421c;
    padding: 7px 9px;
    border-radius: 20px;
}

.probabilities {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 7px;
    margin-bottom: 11px;
}

.signal {
    border-radius: 13px;
    padding: 12px 6px;
    text-align: center;
    background: #111927;
    border: 1px solid #263246;
    min-width: 0;
}

.signal.active {
    box-shadow: 0 0 15px #ffffff12;
}

.signal-name {
    font-size: 11px;
    font-weight: bold;
}

.signal-number {
    font-size: 23px;
    font-weight: 900;
    margin-top: 8px;
}

.up {
    color: #34d399;
    border-color: #087a54;
}

.down {
    color: #fb7185;
    border-color: #8d3043;
}

.neutral {
    color: #fbbf24;
    border-color: #76571b;
}

.prediction {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 9px;
    margin-bottom: 11px;
}

.small-label {
    font-size: 10px;
    color: #8d9aaf;
}

.big-number {
    font-size: 20px;
    font-weight: 900;
    margin-top: 6px;
    overflow-wrap: anywhere;
}

.green {
    color: #34d399;
}

.purple {
    color: #d879ff;
}

.chart-title {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
    margin-bottom: 8px;
}

#chart {
    height: 280px;
    width: 100%;
}

.indicators {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 8px;
    margin-bottom: 11px;
}

.indicator {
    background: #111927;
    border: 1px solid #243044;
    border-radius: 12px;
    padding: 11px 7px;
    min-width: 0;
}

.indicator-name {
    color: #8491a5;
    font-size: 10px;
}

.indicator-value {
    font-size: 16px;
    font-weight: 900;
    margin-top: 8px;
    overflow-wrap: anywhere;
}

.cyan {
    color: #22d3ee;
}

.yellow {
    color: #fbbf24;
}

.pink {
    color: #e879f9;
}

.timer {
    font-size: 29px;
    font-weight: 900;
    letter-spacing: 2px;
    margin-top: 7px;
}

.kalshi {
    font-size: 11px;
    line-height: 1.8;
    color: #b6c1d2;
    overflow-wrap: anywhere;
}

.reason {
    color: #aab6c8;
    font-size: 11px;
    line-height: 1.5;
    margin-top: 8px;
}

.disclaimer {
    font-size: 9px;
    line-height: 1.5;
    color: #738198;
    text-align: center;
    padding: 8px;
}

button {
    border: none;
    border-radius: 10px;
    padding: 10px;
    font-weight: 900;
    cursor: pointer;
}

.refresh {
    background: #27344b;
    color: white;
}

.footer {
    font-size: 10px;
    color: #8b9ab0;
    text-align: center;
    padding: 8px;
}

@media(max-width:360px) {

    .signal-number {
        font-size: 19px;
    }

    .big-number {
        font-size: 17px;
    }

    #chart {
        height: 245px;
    }
}

</style>
</head>

<body>

<div class="container">

    <div class="header">

        <div style="display:flex;gap:9px;align-items:center">

            <div class="logo">₿</div>

            <div>
                <div class="brand">
                    BTC RADAR <span style="color:#d879ff">PRO</span>
                </div>

                <div class="subtitle">
                    BRTI KALSHI • BTC/USD • 15 MIN
                </div>
            </div>

        </div>

        <div class="status" id="connection">
            CONECTANDO
        </div>

    </div>


    <div class="probabilities">

        <div class="signal up" id="box-up">

            <div class="signal-name">▲ SUBE</div>

            <div class="signal-number" id="prob-up">--%</div>

        </div>

        <div class="signal neutral" id="box-neutral">

            <div class="signal-name">◆ INDECISO</div>

            <div class="signal-number" id="prob-neutral">--%</div>

        </div>

        <div class="signal down" id="box-down">

            <div class="signal-name">▼ BAJA</div>

            <div class="signal-number" id="prob-down">--%</div>

        </div>

    </div>


    <div class="prediction">

        <div class="card">

            <div class="small-label">
                PRECIO BTC / BRTI
            </div>

            <div class="big-number green" id="price">
                Esperando...
            </div>

        </div>

        <div class="card">

            <div class="small-label">
                ESTIMACIÓN A 15 MIN
            </div>

            <div class="big-number purple" id="target">
                --
            </div>

            <div class="small-label" id="difference">
                Esperando datos
            </div>

        </div>

    </div>


    <div class="card">

        <div class="chart-title">

            <span>BTC/USD · BRTI REAL</span>

            <button class="refresh" onclick="refreshNow()">
                ↻
            </button>

        </div>

        <div id="chart"></div>

        <div class="subtitle">
            Velas de 1 minuto · EMA 9 · EMA 21 · VWAP aproximado
        </div>

    </div>


    <div class="indicators">

        <div class="indicator">

            <div class="indicator-name">RSI 14</div>

            <div class="indicator-value cyan" id="rsi">
                --
            </div>

        </div>

        <div class="indicator">

            <div class="indicator-name">MACD</div>

            <div class="indicator-value green" id="macd">
                --
            </div>

        </div>

        <div class="indicator">

            <div class="indicator-name">FUERZA</div>

            <div class="indicator-value pink" id="strength">
                --
            </div>

        </div>

        <div class="indicator">

            <div class="indicator-name">EMA 9</div>

            <div class="indicator-value cyan" id="ema9">
                --
            </div>

        </div>

        <div class="indicator">

            <div class="indicator-name">EMA 21</div>

            <div class="indicator-value yellow" id="ema21">
                --
            </div>

        </div>

        <div class="indicator">

            <div class="indicator-name">VWAP TICKS*</div>

            <div class="indicator-value pink" id="vwap">
                --
            </div>

        </div>

    </div>


    <div class="prediction">

        <div class="card">

            <div class="small-label">
                TIEMPO RESTANTE KALSHI
            </div>

            <div class="timer" id="timer">
                --:--
            </div>

            <div class="subtitle" id="contract">
                Buscando contrato...
            </div>

        </div>

        <div class="card">

            <div class="small-label">
                PRECIOS KALSHI
            </div>

            <div class="kalshi" id="kalshi">
                Consultando mercado...
            </div>

        </div>

    </div>


    <div class="card">

        <div class="small-label">
            ANÁLISIS DEL RADAR
        </div>

        <div class="reason" id="reason">
            Esperando datos reales para calcular señales.
        </div>

        <div class="reason" id="data-age">
            Estado de los datos: comprobando...
        </div>

    </div>


    <div class="disclaimer">
        * VWAP aproximado por frecuencia de ticks, no por volumen negociado.
        Las señales son estimaciones técnicas, no garantías.
        Este radar no envía órdenes ni realiza apuestas automáticas.
    </div>

    <div class="footer">
        BITCOIN RADAR PRO BY JADY
    </div>

</div>


<script>

const chartContainer = document.getElementById("chart");

const chart = LightweightCharts.createChart(
    chartContainer,
    {
        width: chartContainer.clientWidth,
        height: 280,

        layout: {
            background: { color: "#111927" },
            textColor: "#94a3b8"
        },

        grid: {
            vertLines: { color: "#202b3c" },
            horzLines: { color: "#202b3c" }
        },

        rightPriceScale: {
            borderColor: "#263246"
        },

        timeScale: {
            borderColor: "#263246",
            timeVisible: true
        },

        crosshair: {
            mode: 1
        }
    }
);


const candleSeries = chart.addCandlestickSeries({

    upColor: "#34d399",
    downColor: "#fb7185",

    borderUpColor: "#34d399",
    borderDownColor: "#fb7185",

    wickUpColor: "#34d399",
    wickDownColor: "#fb7185"

});


const ema9Series = chart.addLineSeries({

    color: "#22d3ee",
    lineWidth: 2,
    title: "EMA 9"

});


const ema21Series = chart.addLineSeries({

    color: "#fbbf24",
    lineWidth: 2,
    title: "EMA 21"

});


const vwapSeries = chart.addLineSeries({

    color: "#e879f9",
    lineWidth: 1,
    title: "VWAP aproximado"

});


function money(value) {

    if (value === null || value === undefined) {
        return "--";
    }

    return Number(value).toLocaleString(
        "en-US",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    );
}


function setText(id, value) {

    document.getElementById(id).textContent = value;
}


function updateSignal(data) {

    setText("prob-up", data.sube + "%");

    setText("prob-neutral", data.indeciso + "%");

    setText("prob-down", data.baja + "%");

    document.getElementById("box-up").classList.toggle(
        "active",
        data.signal === "SUBE"
    );

    document.getElementById("box-neutral").classList.toggle(
        "active",
        data.signal === "INDECISO"
    );

    document.getElementById("box-down").classList.toggle(
        "active",
        data.signal === "BAJA"
    );

    setText("reason", data.reason);
}


function updateIndicators(data) {

    setText(
        "rsi",
        data.rsi === null ? "--" : data.rsi.toFixed(1)
    );

    setText(
        "macd",
        data.macd_histogram === null
            ? "--"
            : data.macd_histogram.toFixed(2)
    );

    setText(
        "strength",
        data.strength + "/100"
    );

    setText("ema9", money(data.ema9));

    setText("ema21", money(data.ema21));

    setText("vwap", money(data.vwap_proxy));
}


function updateChart(data) {

    if (!data.candles || data.candles.length === 0) {
        return;
    }

    candleSeries.setData(data.candles);

    ema9Series.setData(data.ema9_line || []);

    ema21Series.setData(data.ema21_line || []);

    vwapSeries.setData(data.vwap_line || []);

    chart.timeScale().fitContent();
}


async function fetchData() {

    try {

        const response = await fetch(
            "/api/data?t=" + Date.now(),
            { cache: "no-store" }
        );

        if (!response.ok) {
            throw new Error("Error HTTP " + response.status);
        }

        const data = await response.json();

        if (data.price !== null) {

            setText("price", "$" + money(data.price));
        }

        updateSignal(data.analysis);

        updateIndicators(data.analysis);

        updateChart(data);

        if (data.analysis.prediction !== null) {

            setText(
                "target",
                "$" + money(data.analysis.prediction)
            );

            const difference = data.analysis.difference;

            setText(
                "difference",
                (difference >= 0 ? "+" : "") +
                "$" + money(difference) +
                " estimados"
            );

        } else {

            setText("target", "--");

            setText("difference", "Reuniendo datos");
        }

        if (data.connected && data.last_tick_age <= 15) {

            setText("connection", "● BRTI EN VIVO");

        } else {

            setText("connection", "● SIN DATOS ACTUALES");
        }

        setText(
            "data-age",
            "Último tick: " +
            (data.last_tick_age === null
                ? "--"
                : data.last_tick_age + " segundos")
        );

    } catch (error) {

        setText("connection", "ERROR DE DATOS");

        setText("data-age", error.message);
    }
}


async function fetchKalshi() {

    try {

        const response = await fetch(
            "/api/kalshi?t=" + Date.now(),
            { cache: "no-store" }
        );

        const market = await response.json();

        if (!market.ticker) {

            setText("kalshi", "No se encontró contrato abierto");

            setText("contract", "Sin contrato activo");

            setText("timer", "--:--");

            return;
        }

        const yesBid = market.yes_bid === null
            ? "--"
            : (market.yes_bid * 100).toFixed(1) + "¢";

        const yesAsk = market.yes_ask === null
            ? "--"
            : (market.yes_ask * 100).toFixed(1) + "¢";

        const noBid = market.no_bid === null
            ? "--"
            : (market.no_bid * 100).toFixed(1) + "¢";

        const noAsk = market.no_ask === null
            ? "--"
            : (market.no_ask * 100).toFixed(1) + "¢";

        setText(
            "kalshi",
            "YES: " + yesBid + " / " + yesAsk +
            "\nNO: " + noBid + " / " + noAsk
        );

        setText("contract", market.ticker);

        window.marketCloseTimestamp = market.close_timestamp;

    } catch (error) {

        setText("kalshi", "Error consultando Kalshi");
    }
}


function updateTimer() {

    if (!window.marketCloseTimestamp) {
        return;
    }

    const seconds = Math.max(
        0,
        Math.floor(
            window.marketCloseTimestamp - Date.now() / 1000
        )
    );

    const minutes = Math.floor(seconds / 60);

    const remainingSeconds = seconds % 60;

    setText(
        "timer",
        String(minutes).padStart(2, "0") +
        ":" +
        String(remainingSeconds).padStart(2, "0")
    );
}


function refreshNow() {

    fetchData();

    fetchKalshi();
}


setInterval(fetchData, 1500);

setInterval(fetchKalshi, 5000);

setInterval(updateTimer, 1000);

fetchData();

fetchKalshi();


window.addEventListener("resize", () => {

    chart.applyOptions({
        width: chartContainer.clientWidth
    });

});

</script>

</body>
</html>
"""


# ============================================================
# RUTAS FLASK
# ============================================================

@app.route("/")
def home():

    return render_template_string(HTML)


@app.route("/api/data")
def api_data():

    return jsonify(build_data())


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "brti_connected": state["connected"],
        "brti_subscribed": state["subscribed"]
    })


# ============================================================
# INICIO
# ============================================================

if KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY:

    start_brti()

else:

    update_state(
        error="Configura las credenciales BRTI en Render"
    )
    if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
