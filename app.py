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

# ==========================================
# CONFIGURACIÓN
# ==========================================

KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"

INDEX_ID = "BRTI"
PRODUCT = "BTC-USD"

# Mercado activo de Kalshi: objetivo y horario del contrato KXBTC15M
KALSHI_REST = "https://external-api.kalshi.com/trade-api/v2"
MARKET_POLL_SECONDS = 10
active_market = None
market_error = ""

MAX_CANDLES = 300
MAX_TICKS = 3600
STALE_SECONDS = 15

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv(
    "KALSHI_PRIVATE_KEY", ""
).replace("\\n", "\n").strip()

lock = threading.RLock()

brti_ticks = deque(maxlen=MAX_TICKS)
coinbase_candles = deque(maxlen=MAX_CANDLES)

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
session.headers.update({
    "User-Agent": "BitcoinRadar/7.0"
})

# ==========================================
# UTILIDADES
# ==========================================

def valid_price(value):
    try:
        value = float(value)
        return math.isfinite(value) and value > 0
    except (TypeError, ValueError):
        return False

def normalize_timestamp(value):
    if value is None:
        return time.time()

    try:
        value = float(value)

        if value > 10_000_000_000:
            value /= 1000

        return value

    except (TypeError, ValueError):
        return time.time()

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

# ==========================================
# VELAS
# ==========================================

def update_candle(target, price, timestamp=None, volume=0):

    if timestamp is None:
        timestamp = time.time()

    price = float(price)

    if not valid_price(price):
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

# ==========================================
# REGISTRO BRTI
# ==========================================

def set_brti(price, timestamp=None):

    global brti_price, brti_time

    if not valid_price(price):
        return

    price = float(price)

    timestamp = normalize_timestamp(timestamp)

    with lock:

        brti_price = price
        brti_time = timestamp

        brti_ticks.append((timestamp, price))

# ==========================================
# REGISTRO COINBASE
# ==========================================

def set_coinbase(price, timestamp=None):

    global coinbase_price, coinbase_time

    if not valid_price(price):
        return

    price = float(price)

    timestamp = normalize_timestamp(timestamp)

    with lock:

        coinbase_price = price
        coinbase_time = timestamp

        update_candle(
            coinbase_candles,
            price,
            timestamp
        )

# ==========================================
# INDICADORES
# ==========================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    alpha = 2 / (period + 1)

    result = float(values[0])

    for value in values[1:]:

        result = (
            alpha * float(value)
            + (1 - alpha) * result
        )

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

    total = sum(
        float(c["volume"]) for c in rows
    )

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

# ==========================================
# PROMEDIO LOCAL DE LOS ÚLTIMOS 60 SEGUNDOS
# ==========================================

def calculate_brti_average(ticks):

    now = time.time()

    recent = [
        price
        for timestamp, price in ticks
        if now - timestamp <= 60
        and timestamp <= now + 1
    ]

    if not recent:
        return None, 0

    return sum(recent) / len(recent), len(recent)

# ==========================================
# ANÁLISIS DE MOVIMIENTO BRTI
# ==========================================


def calculate_prediction(price, ticks, seconds_to_close, market_target=None):

    if not valid_price(price):
        return {
            "direction": "RECOPILANDO",
            "predicted_price": None,
            "bullish_score": 50,
            "bearish_score": 50,
            "technical_strength": 0,
            "momentum_pct": None,
            "ema9": None,
            "ema21": None,
            "rsi": None,
            "target_influence": 0
        }

    now = time.time()

    recent = [
        (t, p) for t, p in ticks
        if now - t <= 120 and t <= now + 1
    ]

    prices = [p for _, p in recent]

    ema9 = calculate_ema(prices, 9)
    ema21 = calculate_ema(prices, 21)
    rsi = calculate_rsi(prices)

    momentum = 0.0
    short_momentum = 0.0
    velocity = 0.0

    if len(recent) >= 2:
        first_t, first_p = recent[0]
        last_t, last_p = recent[-1]

        elapsed = max(last_t - first_t, 1)

        momentum = (last_p / first_p - 1) * 100
        velocity = (last_p - first_p) / elapsed

        older = [
            p for t, p in recent
            if last_t - t >= 10
        ]

        if older:
            short_momentum = (
                (last_p / older[-1]) - 1
            ) * 100

    remaining = max(0, min(900, seconds_to_close))

    # Proyección basada en velocidad reciente.
    projected_change = (
        (velocity * remaining * 0.18) / price
    ) * 100

    projected_change = max(
        -0.35,
        min(0.35, projected_change)
    )

    predicted = price * (1 + projected_change / 100)

    # Puntuación técnica.
    score = 0.0
    possible = 0.0

    if len(prices) >= 2:
        possible += 2

        score += (
            2 if momentum > 0
            else -2 if momentum < 0
            else 0
        )

    if len(prices) >= 6:
        possible += 2

        score += (
            2 if prices[-1] > prices[-6]
            else -2 if prices[-1] < prices[-6]
            else 0
        )

    if abs(short_momentum) > 0:
        possible += 2

        score += (
            2 if short_momentum > 0
            else -2
        )

    if ema9 is not None and ema21 is not None:
        possible += 2

        score += (
            2 if ema9 > ema21
            else -2 if ema9 < ema21
            else 0
        )

    if rsi is not None:
        possible += 1

        if rsi > 55:
            score += 1
        elif rsi < 45:
            score -= 1

    # Influencia del objetivo real de Kalshi.
    target_influence = 0

    if valid_price(market_target):
        possible += 3

        target_gap = (
            (predicted / market_target) - 1
        ) * 100

        if target_gap > 0.015:
            target_influence = 3
        elif target_gap < -0.015:
            target_influence = -3
        elif target_gap > 0:
            target_influence = 1
        elif target_gap < 0:
            target_influence = -1

        score += target_influence

    normalized = (
        score / possible
        if possible > 0 else 0
    )

    # Porcentajes continuos, sin ESPERAR como decisión dominante.
    bullish = max(
        5,
        min(95, round(50 + normalized * 35, 1))
    )

    bearish = round(100 - bullish, 1)

    if bullish > 52:
        direction = "ALCISTA"
    elif bearish > 52:
        direction = "BAJISTA"
    else:
        direction = "INDECISO"

    return {
        "direction": direction,
        "predicted_price": round(predicted, 2),
        "bullish_score": bullish,
        "bearish_score": bearish,
        "technical_strength": round(abs(normalized) * 100),
        "momentum_pct": round(momentum, 5),
        "ema9": round(ema9, 2) if ema9 is not None else None,
        "ema21": round(ema21, 2) if ema21 is not None else None,
        "rsi": round(rsi, 2) if rsi is not None else None,
        "target_influence": target_influence
    }
    
    # ==========================================
# HISTORIAL COINBASE
# ==========================================

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

            coinbase_candles.extend(
                rows[-MAX_CANDLES:]
            )

            coinbase_error = ""

    except Exception as exc:

        with lock:
            coinbase_error = str(exc)

# ==========================================
# COINBASE WEBSOCKET
# ==========================================

def coinbase_open(ws):

    global coinbase_connected

    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": [PRODUCT],
        "channel": "ticker"
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

                        set_coinbase(
                            item.get("price")
                        )

        with lock:
            coinbase_error = ""

    except Exception as exc:

        with lock:
            coinbase_error = str(exc)

def coinbase_status(value, error=""):

    global coinbase_connected, coinbase_error

    with lock:

        coinbase_connected = value

        if error:
            coinbase_error = str(error)

def coinbase_worker():

    while True:

        try:

            ws = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=coinbase_open,
                on_message=coinbase_message,
                on_error=lambda ws, err: coinbase_status(
                    False, err
                ),
                on_close=lambda ws, code, msg: coinbase_status(
                        False
                )
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:

            coinbase_status(False, exc)

        time.sleep(5)


# ==========================================
# BUSCAR VALOR BRTI EN MENSAJES KALSHI
# ==========================================

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

    index_id = (
        data.get("index_id")
        or data.get("ticker")
        or data.get("id")
    )

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
                raw.get("time")
                or raw.get("ts_ms")
                or raw.get("timestamp")
                or data.get("received_at")
            )

            if valid_price(value):
                return value, timestamp

        value = data.get("value")

        if valid_price(value):

            timestamp = (
                data.get("time")
                or data.get("ts_ms")
                or data.get("timestamp")
                or data.get("received_at")
            )

            return value, timestamp

    for key in ("msg", "payload", "data"):

        nested = data.get(key)

        if isinstance(nested, (dict, list, str)):

            found = find_brti_value(nested)

            if found is not None:
                return found

    return None


# ==========================================
# KALSHI WEBSOCKET
# ==========================================

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

    print(
        "KALSHI: SUSCRIPCIÓN BRTI ENVIADA",
        flush=True
    )


def kalshi_message(ws, message):

    global kalshi_error

    try:

        data = json.loads(message)

        if isinstance(data, dict):

            if data.get("type") == "error":

                error = str(data.get("msg", data))

                with lock:
                    kalshi_error = error

                print(
                    "KALSHI ERROR:",
                    error,
                    flush=True
                )

                return

        found = find_brti_value(data)

        if found is not None:

            value, timestamp = found

            timestamp = normalize_timestamp(timestamp)

            set_brti(value, timestamp)

            with lock:
                kalshi_error = ""

        elif isinstance(data, dict):

            if data.get("type") in (
                "subscribed",
                "subscription"
            ):

                print(
                    "KALSHI SUSCRIPCIÓN CONFIRMADA:",
                    json.dumps(data)[:1000],
                    flush=True
                )

    except Exception as exc:

        with lock:
            kalshi_error = str(exc)

        print(
            "ERROR LEYENDO KALSHI:",
            str(exc),
            flush=True
        )


def kalshi_status(value, error=""):

    global kalshi_connected, kalshi_error

    with lock:

        kalshi_connected = value

        if error:
            kalshi_error = str(error)


def kalshi_worker():

    while True:

        try:

            headers = kalshi_headers()

            ws = websocket.WebSocketApp(
                KALSHI_WS,
                header=headers,
                on_open=kalshi_open,
                on_message=kalshi_message,
                on_error=lambda ws, err: kalshi_status(
                    False, err
                ),
                on_close=lambda ws, code, msg: kalshi_status(
                    False
                )
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:

            kalshi_status(False, exc)

            print(
                "ERROR CONECTANDO KALSHI:",
                str(exc),
                flush=True
            )

        time.sleep(7)


# ==========================================
# MERCADO ACTIVO KALSHI KXBTC15M
# ==========================================

def parse_market_time(value):

    if not value:
        return None

    try:

        from datetime import datetime, timezone

        parsed = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        return parsed.timestamp()

    except Exception:
        return None


def fetch_active_market():

    global active_market, market_error

    try:

        response = session.get(
            f"{KALSHI_REST}/markets",
            params={
                "series_ticker": "KXBTC15M",
                "status": "open",
                "limit": 1000
            },
            timeout=12
        )

        response.raise_for_status()

        payload = response.json()

        markets = (
            payload.get("markets", [])
            if isinstance(payload, dict)
            else []
        )

        now = time.time()
        candidates = []

        for market in markets:

            opened = parse_market_time(
                market.get("open_time")
            )

            closed = parse_market_time(
                market.get("close_time")
            )

            if opened is None or closed is None:
                continue

            if opened <= now < closed:
                candidates.append(
                    (closed, opened, market)
                )

        if not candidates:

            with lock:
                active_market = None
                market_error = (
                    "Kalshi no devolvió un contrato "
                    "KXBTC15M abierto"
                )

            return

        candidates.sort(
            key=lambda row: row[0]
        )

        closed, opened, market = candidates[0]

        raw_target = market.get("floor_strike")

        target = (
            float(raw_target)
            if valid_price(raw_target)
            else None
        )

        snapshot = {
            "ticker": market.get("ticker"),
            "open_time": opened,
            "close_time": closed,
            "target": target,
            "status": market.get("status", "open")
        }

        with lock:
            active_market = snapshot
            market_error = ""

    except Exception as exc:

        with lock:
            market_error = str(exc)


def market_worker():

    while True:

        fetch_active_market()

        time.sleep(MARKET_POLL_SECONDS)


# ==========================================
# INICIAR SERVICIOS UNA SOLA VEZ
# ==========================================

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

    threading.Thread(
        target=market_worker,
        daemon=True
    ).start()


# ==========================================
# DATOS PRINCIPALES
# ==========================================

def get_data():

    with lock:

        bp = brti_price
        bt = brti_time

        cp = coinbase_price
        ct = coinbase_time

        ticks = list(brti_ticks)

        candles = [
            dict(c)
            for c in coinbase_candles
        ]

        kc = kalshi_connected
        cc = coinbase_connected

        ke = kalshi_error
        ce = coinbase_error

        market = (
            dict(active_market)
            if active_market
            else None
        )

        me = market_error

    now = time.time()

    if market:

        countdown = max(
            0,
            int(market["close_time"] - now)
        )

        market_target = market.get("target")
        market_ticker = market.get("ticker")
        market_close_time = market.get("close_time")

    else:

        countdown = 0
        market_target = None
        market_ticker = None
        market_close_time = None

    brti_age = (
        round(now - bt, 1)
        if bt else None
    )

    coinbase_age = (
        round(now - ct, 1)
        if ct else None
    )

    brti_live = (
        bp is not None
        and brti_age is not None
        and 0 <= brti_age <= STALE_SECONDS
    )

    coinbase_live = (
        cp is not None
        and coinbase_age is not None
        and 0 <= coinbase_age <= STALE_SECONDS
    )

     indicators = calculate_prediction(
        bp,
        ticks,
        countdown,
        market_target
     )

    average, average_count = calculate_brti_average(
        ticks
    )

    projected = indicators.get("predicted_price")

    target_gap_pct = (
        round(
            (projected / market_target - 1) * 100,
            5
        )
        if projected is not None and market_target
        else None
    )

    if target_gap_pct is None:
        contract_direction = "SIN OBJETIVO"

    elif target_gap_pct > 0.002:
        contract_direction = "ALCISTA"

    elif target_gap_pct < -0.002:
        contract_direction = "BAJISTA"

    else:
        contract_direction = "INDECISO"

    difference = (
        round(cp - bp, 2)
        if cp is not None and bp is not None
        else None
    )

    return {

        "ok": True,

        "brti_price": (
            round(bp, 2)
            if bp is not None else None
        ),

        "brti_live": brti_live,
        "brti_age": brti_age,
        "brti_samples": len(ticks),

        "kalshi_connected": kc,
        "kalshi_error": ke,

        "coinbase_price": (
            round(cp, 2)
            if cp is not None else None
        ),

        "coinbase_live": coinbase_live,
        "coinbase_age": coinbase_age,
        "coinbase_connected": cc,
        "coinbase_error": ce,

        "price_difference": difference,

        "coinbase_candles": candles,

        "countdown": countdown,

        "market_ticker": market_ticker,
        "market_target": (
            round(market_target, 2)
            if market_target else None
        ),
        "market_close_time": market_close_time,
        "market_target_gap_pct": target_gap_pct,
        "contract_direction": contract_direction,
        "market_error": me,

        "brti_average_60s": (
            round(average, 2)
            if average is not None else None
        ),

        "brti_average_samples": average_count,

        **indicators
    }


# ==========================================
# RUTAS FLASK
# ==========================================

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

    start_services()

    data = get_data()

    return jsonify({

        "ok": True,

        "brti_live": data["brti_live"],
        "brti_price": data["brti_price"],
        "brti_age": data["brti_age"],

        "kalshi_connected": data["kalshi_connected"],
        "kalshi_error": data["kalshi_error"],

        "coinbase_live": data["coinbase_live"],
        "coinbase_price": data["coinbase_price"],
        "coinbase_connected": data["coinbase_connected"],

        "brti_samples": data["brti_samples"],

        "market_ticker": data["market_ticker"],
        "market_target": data["market_target"],
        "market_error": data["market_error"]
    })
HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#080b14">

<title>BTC USD Radar</title>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    padding:12px;
    background:#080b14;
    color:#f3f5ff;
    font-family:Arial,sans-serif;
}

main{
    max-width:750px;
    margin:auto;
}

.header{
    display:flex;
    justify-content:space-between;
    align-items:center;
    margin:5px 0 16px;
    gap:8px;
}

.logo{
    font-size:22px;
    font-weight:900;
    letter-spacing:1px;
}

.purple{
    color:#b88aff;
}

.status{
    font-size:10px;
    text-align:right;
    color:#9ba9c4;
}

.dot{
    display:inline-block;
    width:7px;
    height:7px;
    border-radius:50%;
    background:#ff536b;
    margin-right:4px;
}

.dot.live{
    background:#00e69a;
    box-shadow:0 0 9px #00e69a;
}

.grid{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:9px;
}

.card{
    min-width:0;
    background:#111827;
    border:1px solid #26334b;
    border-radius:15px;
    padding:14px;
    margin-bottom:10px;
}

.label{
    color:#93a2bf;
    font-size:10px;
    letter-spacing:.5px;
    font-weight:700;
}

.price{
    font-size:clamp(19px,5.4vw,29px);
    font-weight:900;
    margin:10px 0 5px;
    overflow-wrap:anywhere;
}

.predicted{
    color:#bd91ff;
}

.small{
    font-size:10px;
    color:#8d9bb6;
    line-height:1.6;
}

.green{
    color:#00e69a;
}

.red{
    color:#ff536b;
}

.yellow{
    color:#f4c95d;
}

.signal{
    font-size:27px;
    font-weight:900;
    margin:12px 0;
}

.bar{
    height:13px;
    display:flex;
    border-radius:20px;
    overflow:hidden;
    background:#29344a;
    margin:12px 0 8px;
}

.bull{
    background:#00d995;
    width:50%;
}

.bear{
    background:#ff5269;
    width:50%;
}

.row{
    display:flex;
    justify-content:space-between;
    align-items:center;
    gap:8px;
}

.section-title{
    color:#a1afca;
    font-size:11px;
    font-weight:800;
    margin-bottom:12px;
}

.metrics{
    display:grid;
    grid-template-columns:repeat(3,1fr);
    gap:8px;
}

.metric{
    background:#0b1220;
    border:1px solid #202d43;
    padding:10px 5px;
    border-radius:10px;
    text-align:center;
    min-width:0;
}

.metric-name{
    font-size:9px;
    color:#8292af;
}

.metric-value{
    font-size:13px;
    font-weight:800;
    margin-top:7px;
    overflow-wrap:anywhere;
}

.chart-wrap{
    width:100%;
    height:300px;
    position:relative;
}

canvas{
    display:block;
    width:100%;
    height:100%;
    touch-action:none;
}

.chart-legend{
    display:flex;
    gap:13px;
    flex-wrap:wrap;
    margin:8px 0;
    font-size:10px;
    color:#9aa9c4;
}

.legend-dot{
    display:inline-block;
    width:8px;
    height:8px;
    border-radius:50%;
    margin-right:4px;
}

button{
    border:1px solid #35476b;
    background:#182640;
    color:white;
    border-radius:9px;
    padding:10px 12px;
    font-size:10px;
    font-weight:800;
}

button:active{
    opacity:.7;
}

button:disabled{
    opacity:.5;
}

.footer{
    text-align:center;
    color:#66758f;
    font-size:9px;
    padding:12px 0 20px;
}

@media(max-width:380px){

    body{
        padding:8px;
    }

    .card{
        padding:11px;
    }

    .logo{
        font-size:19px;
    }

    .chart-wrap{
        height:250px;
    }

    .metrics{
        gap:5px;
    }

}

</style>
</head>

<body>

<main>

<!-- ENCABEZADO -->

<div class="header">

    <div class="logo">
        BTC <span class="purple">RADAR</span>
    </div>

    <div class="status">

        <div>
            <span id="brtiDot" class="dot"></span>
            <span id="brtiStatus">BRTI CONECTANDO</span>
        </div>

        <div>
            <span id="coinDot" class="dot"></span>
            <span id="coinStatus">COINBASE</span>
        </div>

    </div>

</div>


<!-- PRECIOS -->

<div class="grid">

    <section class="card">

        <div class="label">
            PRECIO ACTUAL · BRTI
        </div>

        <div class="price" id="brtiPrice">
            --
        </div>

        <div class="small" id="brtiAge">
            Esperando índice
        </div>

    </section>


    <section class="card">

        <div class="label">
            PREDICCIÓN · 15 MIN
        </div>

        <div class="price predicted" id="predictedPrice">
            --
        </div>

        <div class="small" id="predictionLabel">
            Calculando movimiento
        </div>

    </section>

</div>


<!-- DIRECCIÓN -->

<section class="card">

    <div class="section-title">
        DIRECCIÓN ESTIMADA BRTI
    </div>

    <div class="signal yellow" id="direction">
        RECOPILANDO
    </div>

    <div class="row">

        <div>
            <div class="label">ALCISTA</div>
            <div class="price green" id="bullText">50%</div>
        </div>

        <div style="text-align:right">

            <div class="label">BAJISTA</div>
            <div class="price red" id="bearText">50%</div>

        </div>

    </div>

    <div class="bar">

        <div class="bull" id="bullBar"></div>

        <div class="bear" id="bearBar"></div>

    </div>

    <div class="row">

        <span class="small">
            FUERZA TÉCNICA
        </span>

        <strong id="strength" class="purple">
            --
        </strong>

    </div>

    <div class="small" id="signalStage">
        Esperando movimiento BRTI
    </div>

</section>


<!-- GRÁFICA -->

<section class="card">

    <div class="row">

        <div>

            <div class="section-title">
                GRÁFICA BTC/USD
            </div>

            <div class="small">
                Coinbase · Velas de 1 minuto
            </div>

        </div>

        <button id="refreshButton"
                onclick="refreshData(true)">
            ACTUALIZAR
        </button>

    </div>

    <div class="chart-legend">

        <span>
            <i class="legend-dot"
               style="background:#b98aff"></i>
            EMA 9
        </span>

        <span>
            <i class="legend-dot"
               style="background:#45a9ff"></i>
            EMA 21
        </span>

        <span>
            <i class="legend-dot"
               style="background:#f4c95d"></i>
            PRECIO
        </span>

    </div>

    <div class="chart-wrap">

        <canvas id="chart"></canvas>

    </div>

    <div class="row">

        <span class="small" id="candleCount">
            0 velas
        </span>

        <button onclick="goLive()">
            VOLVER A EN VIVO
        </button>

    </div>

    <div class="small">
        Arrastra para desplazarte. Usa dos dedos
        para acercar o alejar la gráfica.
    </div>

</section>


<!-- INDICADORES -->

<section class="card">

    <div class="section-title">
        INDICADORES DEL BRTI
    </div>

    <div class="metrics">

        <div class="metric">
            <div class="metric-name">EMA 9</div>
            <div class="metric-value" id="ema9">--</div>
        </div>

        <div class="metric">
            <div class="metric-name">EMA 21</div>
            <div class="metric-value" id="ema21">--</div>
        </div>

        <div class="metric">
            <div class="metric-name">RSI 14</div>
            <div class="metric-value" id="rsi">--</div>
        </div>

        <div class="metric">
            <div class="metric-name">MOMENTUM</div>
            <div class="metric-value" id="momentum">--</div>
        </div>

        <div class="metric">
            <div class="metric-name">PROMEDIO LOCAL 60S</div>
            <div class="metric-value" id="averagePrice">--</div>
        </div>

        <div class="metric">
            <div class="metric-name">MUESTRAS BRTI</div>
            <div class="metric-value" id="brtiSamples">0</div>
        </div>

    </div>

    <div class="small" id="averageStatus">
        Promedio de muestras locales recibidas
    </div>

</section>


<!-- CONTRATO ACTIVO KALSHI -->

<section class="card">

    <div class="section-title">
        CONTRATO ACTIVO KALSHI · 15 MIN
    </div>

    <div class="metrics" style="grid-template-columns:1fr 1fr">

        <div class="metric">
            <div class="metric-name">
                OBJETIVO DE APERTURA
            </div>
            <div class="metric-value" id="marketTarget">
                --
            </div>
        </div>

        <div class="metric">
            <div class="metric-name">
                DIFERENCIA PROYECTADA
            </div>
            <div class="metric-value" id="targetGap">
                --
            </div>
        </div>

    </div>

    <div class="row" style="margin-top:12px">

        <div>
            <div class="label">
                LECTURA FRENTE AL OBJETIVO
            </div>
            <div class="signal yellow" id="contractDirection">
                BUSCANDO CONTRATO
            </div>
        </div>

        <div style="text-align:right">
            <div class="label">CONTRATO</div>
            <div class="small" id="marketTicker">--</div>
        </div>

    </div>

    <div class="small" id="marketStatus">
        Buscando el contrato abierto de Kalshi
    </div>

</section>
    <!-- CIERRE Y CONEXIONES -->

<section class="card">

    <div class="row">

        <div>

            <div class="label">
                CIERRE DEL CONTRATO KALSHI
            </div>

            <div class="signal purple" id="countdown">
                15:00
            </div>

        </div>

        <div style="text-align:right">

            <div class="label">
                KALSHI
            </div>

            <div class="value yellow" id="kalshiConnection">
                --
            </div>

        </div>

    </div>

    <div class="small" id="coinbaseConnection"></div>

    <div class="small" id="errors"></div>

</section>


<div class="footer">
    BTC RADAR · DATOS EN TIEMPO REAL · MODO LECTURA
</div>

</main>

<script>

const $ = id => document.getElementById(id);

let candles = [];

let chartOffset = 0;
let chartScale = 1;

let pointers = new Map();
let previousX = 0;

let initialDistance = 0;
let initialZoom = 1;

let busy = false;


// ==========================================
// FORMATO DE PRECIOS
// ==========================================

function money(value){

    if(
        value === null ||
        value === undefined ||
        !Number.isFinite(Number(value))
    ){
        return "--";
    }

    return "$" + Number(value).toLocaleString(
        "en-US",
        {
            minimumFractionDigits:2,
            maximumFractionDigits:2
        }
    );
}


function clock(seconds){

    const m = Math.floor(seconds / 60);
    const s = seconds % 60;

    return String(m).padStart(2,"0")
        + ":" +
        String(s).padStart(2,"0");
}


// ==========================================
// EMA PARA LA GRÁFICA
// ==========================================

function emaSeries(values, period){

    if(!values.length) return [];

    const alpha = 2 / (period + 1);

    let result = values[0];

    const output = [result];

    for(let i=1;i<values.length;i++){

        result =
            alpha * values[i]
            + (1-alpha) * result;

        output.push(result);
    }

    return output;
}


// ==========================================
// DIBUJAR GRÁFICA
// ==========================================

function drawChart(data){

    if(data){
        candles = data;
    }

    const canvas = $("chart");

    const rect = canvas.getBoundingClientRect();

    if(!rect.width || !rect.height) return;

    const ratio = window.devicePixelRatio || 1;

    canvas.width = Math.round(rect.width * ratio);
    canvas.height = Math.round(rect.height * ratio);

    const ctx = canvas.getContext("2d");

    ctx.setTransform(ratio,0,0,ratio,0,0);

    const W = rect.width;
    const H = rect.height;

    ctx.clearRect(0,0,W,H);

    if(!candles.length){

        ctx.fillStyle="#91a2c1";
        ctx.font="13px Arial";
        ctx.textAlign="center";

        ctx.fillText(
            "ESPERANDO VELAS DE COINBASE",
            W/2,
            H/2
        );

        return;
    }

    const count = Math.max(
        10,
        Math.min(
            100,
            Math.floor(65/chartScale)
        )
    );

    const center = Math.floor(count*.58);

    chartOffset = Math.max(
        0,
        Math.min(chartOffset,candles.length-1)
    );

    const end = candles.length-chartOffset;

    const start = Math.max(
        0,
        end-count
    );

    const visible = candles.slice(start,end);

    if(!visible.length) return;

    const closes = candles.map(
        c => Number(c.close)
    );

    const e9 = emaSeries(
        closes,9
    ).slice(start,end);

    const e21 = emaSeries(
        closes,21
    ).slice(start,end);

    const values = visible.flatMap(c=>[
        Number(c.high),
        Number(c.low)
    ]).concat(e9,e21);

    let min = Math.min(...values);
    let max = Math.max(...values);

    const padding = Math.max(
        (max-min)*.13,
        2
    );

    min -= padding;
    max += padding;

    const left = 75;
    const right = W-9;
    const top = 20;
    const bottom = H-20;

    const plotHeight = bottom-top;
    const plotWidth = right-left;

    const y = price =>
        bottom -
        ((price-min)/(max-min))*plotHeight;

    const step = plotWidth/count;

    const candleWidth = Math.max(
        3,
        Math.min(14,step*.65)
    );

    function candleX(i){

        return left + (
            center-(visible.length-1-i)+.5
        )*step;

    }


    // CUADRÍCULA

    ctx.font="10px Arial";
    ctx.strokeStyle="#263247";
    ctx.lineWidth=1;

    for(let i=0;i<=4;i++){

        const yy = top+plotHeight*i/4;

        ctx.beginPath();
        ctx.moveTo(left,yy);
        ctx.lineTo(right,yy);
        ctx.stroke();

        const price =
            max-(max-min)*i/4;

        ctx.fillStyle="#8999b5";

        ctx.fillText(
            price.toFixed(0),
            right+2,
            yy+3
        );
    }


    // VELAS

    visible.forEach((c,i)=>{

        const x = candleX(i);

        const open = Number(c.open);
        const close = Number(c.close);
        const high = Number(c.high);
        const low = Number(c.low);

        const up = close>=open;

        const color = up
            ? "#00df9a"
            : "#ff5269";

        ctx.strokeStyle=color;
        ctx.fillStyle=color;
        ctx.lineWidth=1.5;

        ctx.beginPath();

        ctx.moveTo(x,y(high));
        ctx.lineTo(x,y(low));

        ctx.stroke();

        const bodyTop = Math.min(
            y(open),
            y(close)
        );

        const bodyHeight = Math.max(
            2,
            Math.abs(y(open)-y(close))
        );

        ctx.fillRect(
            x-candleWidth/2,
            bodyTop,
            candleWidth,
            bodyHeight
        );

    });


    // EMA 9 Y EMA 21

    function drawLine(series,color){

        if(series.length<2) return;

        ctx.beginPath();

        ctx.strokeStyle=color;
        ctx.lineWidth=1.7;

        series.forEach((value,i)=>{

            const x=candleX(i);
            const yy=y(value);

            if(i===0){
                ctx.moveTo(x,yy);
            }else{
                ctx.lineTo(x,yy);
            }

        });

        ctx.stroke();
    }

    drawLine(e9,"#b98aff");
    drawLine(e21,"#45a9ff");


    // PRECIO ACTUAL

    if(chartOffset===0){

        const last = candles[candles.length-1];

        const lastPrice = Number(last.close);

        const lastY = y(lastPrice);

        ctx.setLineDash([5,4]);

        ctx.strokeStyle="#f4c95d";

        ctx.beginPath();

        ctx.moveTo(left,lastY);
        ctx.lineTo(right,lastY);

        ctx.stroke();

        ctx.setLineDash([]);

    }

}


// ==========================================
// VOLVER A LA ÚLTIMA VELA
// ==========================================

function goLive(){

    chartOffset=0;
    chartScale=1;

    drawChart();
}


// ==========================================
// CONTROL TÁCTIL Y ZOOM
// ==========================================

function pointerDistance(a,b){

    return Math.hypot(
        a.x-b.x,
        a.y-b.y
    );
}

const canvas=$("chart");

canvas.addEventListener(
    "pointerdown",
    event=>{

        canvas.setPointerCapture(event.pointerId);

        pointers.set(
            event.pointerId,
            {
                x:event.clientX,
                y:event.clientY
            }
        );

        if(pointers.size===1){

            previousX=event.clientX;

        }

        if(pointers.size===2){

            const p=[...pointers.values()];

            initialDistance=pointerDistance(
                p[0],p[1]
            );

            initialZoom=chartScale;

        }

    }
);


canvas.addEventListener(
    "pointermove",
    event=>{

        if(!pointers.has(event.pointerId)) return;

        pointers.set(
            event.pointerId,
            {
                x:event.clientX,
                y:event.clientY
            }
        );

        if(pointers.size===2){

            const p=[...pointers.values()];

            const distance=pointerDistance(
                p[0],p[1]
            );

            if(initialDistance>0){

                chartScale=Math.max(
                    .5,
                    Math.min(
                        8,
                        initialZoom*distance/initialDistance
                    )
                );

                drawChart();

            }

        }else if(pointers.size===1){

            const delta=event.clientX-previousX;

            previousX=event.clientX;

            const count=Math.max(
                10,
                Math.min(
                    100,
                    Math.floor(65/chartScale)
                )
            );

            const step=canvas.clientWidth/count;

            chartOffset+=-delta/Math.max(step,1);

            chartOffset=Math.max(
                0,
                Math.min(
                    chartOffset,
                    candles.length-1
                )
            );

            drawChart();

        }

    }
);


function releasePointer(event){

    pointers.delete(event.pointerId);

    if(pointers.size<2){
        initialDistance=0;
    }

}

canvas.addEventListener(
    "pointerup",
    releasePointer
);

canvas.addEventListener(
    "pointercancel",
    releasePointer
);


// ==========================================
// ACTUALIZAR INFORMACIÓN DEL RADAR
// ==========================================

function updateRadar(d){

    $("brtiPrice").textContent=
        money(d.brti_price);

    $("predictedPrice").textContent=
        money(d.predicted_price);

    $("brtiAge").textContent=
        d.brti_age==null
        ? "Esperando índice"
        : "Dato actualizado hace "+d.brti_age+" s";


    // DIRECCIÓN

    const direction=d.direction || "RECOPILANDO";

    $("direction").textContent=direction;

    $("direction").className="signal "+
        (
            direction==="ALCISTA" ? "green":
            direction==="BAJISTA" ? "red":
            "yellow"
        );


    // PORCENTAJES

    const bullish=Number(d.bullish_score ?? 50);

    const bearish=Number(d.bearish_score ?? 50);

    $("bullText").textContent=
        bullish.toFixed(1)+"%";

    $("bearText").textContent=
        bearish.toFixed(1)+"%";

    $("bullBar").style.width=
        bullish+"%";

    $("bearBar").style.width=
        bearish+"%";


    // FUERZA

    $("strength").textContent=
        Number(d.technical_strength ?? 0)+"%";

    $("signalStage").textContent=
        d.signal_stage || (
            direction==="RECOPILANDO"
            ? "Esperando movimiento BRTI"
            : "Lectura técnica en vivo"
        );


    // INDICADORES

    $("ema9").textContent=
        money(d.ema9);

    $("ema21").textContent=
        money(d.ema21);

    $("rsi").textContent=
        d.rsi==null
        ? "--"
        : Number(d.rsi).toFixed(1);

    $("momentum").textContent=
        d.momentum_pct==null
        ? "--"
        : Number(d.momentum_pct).toFixed(5)+"%";


    // PROMEDIO LOCAL

    $("averagePrice").textContent=
        money(d.brti_average_60s);

    $("averageStatus").textContent=
        "Muestras recibidas en 60 segundos: "+
        (d.brti_average_samples || 0)+
        " · Cálculo local";

    $("brtiSamples").textContent=
        d.brti_samples || 0;


    // CONTRATO KALSHI

    $("marketTarget").textContent=
        money(d.market_target);

    $("targetGap").textContent=
        d.market_target_gap_pct==null
        ? "--"
        : Number(d.market_target_gap_pct).toFixed(4)+"%";

    $("contractDirection").textContent=
        d.contract_direction || "SIN OBJETIVO";

    $("contractDirection").className="signal "+
        (
            d.contract_direction==="ALCISTA" ? "green":
            d.contract_direction==="BAJISTA" ? "red":
            "yellow"
        );

    $("marketTicker").textContent=
        d.market_ticker || "--";

    $("marketStatus").textContent=
        d.market_error
        ? "Mercado no disponible: "+d.market_error
        : "Consulta automática del contrato activo de Kalshi";


    // CONEXIONES

    $("brtiDot").className=
        d.brti_live ? "dot live":"dot";

    $("coinDot").className=
        d.coinbase_live ? "dot live":"dot";

    $("brtiStatus").textContent=
        d.brti_live
        ? "BRTI EN VIVO"
        : "BRTI SIN DATOS";

    $("coinStatus").textContent=
        d.coinbase_live
        ? "COINBASE EN VIVO"
        : "COINBASE SIN DATOS";


    $("kalshiConnection").textContent=
        d.kalshi_connected
        ? "CONECTADO"
        : "DESCONECTADO";

    $("kalshiConnection").className=
        d.kalshi_connected
        ? "green"
        : "red";


    $("coinbaseConnection").textContent=
        "Coinbase WebSocket: "+
        (
            d.coinbase_connected
            ? "CONECTADO"
            : "DESCONECTADO"
        );


    // CUENTA REGRESIVA REAL DEL CONTRATO

    $("countdown").textContent=
        clock(Number(d.countdown || 0));


    // ERRORES

    $("errors").textContent=
        [
            d.kalshi_error,
            d.coinbase_error
        ]
        .filter(Boolean)
        .join(" | ");


    // GRÁFICA

    drawChart(d.coinbase_candles || []);

    $("candleCount").textContent=
        (d.coinbase_candles || []).length+
        " velas";

}


// ==========================================
// ACTUALIZACIÓN AUTOMÁTICA
// ==========================================

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
            "/api?t="+Date.now(),
            {
                cache:"no-store"
            }
        );

        if(!response.ok){

            throw new Error(
                "HTTP "+response.status
            );

        }

        const data=await response.json();

        updateRadar(data);

    }catch(error){

        $("brtiStatus").textContent="ERROR DE DATOS";

        $("errors").textContent=String(error);

        console.error(error);

    }finally{

        busy=false;

        if(manual){

            button.disabled=false;

            button.textContent="ACTUALIZAR";

        }

    }

}


// ==========================================
// ARRANQUE
// ==========================================

window.addEventListener(
    "resize",
    ()=>drawChart()
);

refreshData();

setInterval(
    ()=>refreshData(false),
    1000
);

</script>

</body>
</html>
"""

# ==========================================
# INICIAR APLICACIÓN
# ==========================================

if __name__ == "__main__":

    start_services()

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT",10000)),
        threaded=True,
        debug=False
    )
