import os
import json
import time
import math
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

# ==========================================
# CONFIGURACIÓN
# ==========================================

KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"

KALSHI_REST = "https://external-api.kalshi.com/trade-api/v2"

INDEX_ID = "BRTI"
PRODUCT = "BTC-USD"
MARKET_SERIES = "KXBTC15M"

MARKET_POLL_SECONDS = 10
MAX_CANDLES = 300
MAX_TICKS = 3600
STALE_SECONDS = 15

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()

PRIVATE_KEY_PEM = os.getenv(
    "KALSHI_PRIVATE_KEY", ""
).replace("\\n", "\n").strip()

lock = threading.RLock()

brti_ticks = deque(maxlen=MAX_TICKS)
brti_candles = deque(maxlen=MAX_CANDLES)
coinbase_candles = deque(maxlen=MAX_CANDLES)

brti_price = None
brti_time = 0

coinbase_price = None
coinbase_time = 0

kalshi_connected = False
coinbase_connected = False

kalshi_error = ""
coinbase_error = ""
market_error = ""

active_market = None
started = False

session = requests.Session()

session.headers.update({
    "User-Agent": "BitcoinRadar/8.0"
})


# ==========================================
# UTILIDADES
# ==========================================

def valid_price(value):

    try:
        value = float(value)

        return (
            math.isfinite(value)
            and value > 0
        )

    except (TypeError, ValueError):
        return False


def normalize_timestamp(value=None):

    if value is None:
        return time.time()

    try:
        value = float(value)

        if value > 10_000_000_000:
            value /= 1000

        if value <= 0:
            return time.time()

        return value

    except (TypeError, ValueError):
        return time.time()


def kalshi_headers():

    if not API_KEY_ID or not PRIVATE_KEY_PEM:

        raise RuntimeError(
            "Faltan las credenciales de Kalshi en Render"
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
        "KALSHI-ACCESS-SIGNATURE: "
        + base64.b64encode(signature).decode("utf-8")
    ]


# ==========================================
# VELAS BRTI Y COINBASE
# ==========================================

def update_candle(target, price, timestamp=None, volume=0):

    if not valid_price(price):
        return

    timestamp = normalize_timestamp(timestamp)
    price = float(price)

    bucket = int(timestamp // 60) * 60

    volume = float(volume or 0)

    # Buscar la vela del minuto correspondiente.
    # Esto evita crear velas duplicadas si los
    # mensajes llegan ligeramente desordenados.

    for candle in target:

        if int(candle["time"]) == bucket:

            candle["high"] = max(
                candle["high"],
                price
            )

            candle["low"] = min(
                candle["low"],
                price
            )

            if timestamp >= candle.get(
                "last_timestamp",
                bucket
            ):

                candle["close"] = price
                candle["last_timestamp"] = timestamp

            if timestamp < candle.get(
                "first_timestamp",
                bucket
            ):

                candle["open"] = price
                candle["first_timestamp"] = timestamp

            candle["volume"] += volume

            return

    target.append({
        "time": bucket,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume,
        "first_timestamp": timestamp,
        "last_timestamp": timestamp
    })

    # Mantener orden cronológico.

    ordered = sorted(
        list(target),
        key=lambda item: item["time"]
    )

    target.clear()

    target.extend(
        ordered[-MAX_CANDLES:]
    )


# ==========================================
# REGISTRO BRTI — FUENTE PRINCIPAL
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

        brti_ticks.append(
            (timestamp, price)
        )

        update_candle(
            brti_candles,
            price,
            timestamp
        )


# ==========================================
# REGISTRO COINBASE — RESPALDO
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

    gains = [
        max(change, 0)
        for change in changes
    ]

    losses = [
        max(-change, 0)
        for change in changes
    ]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(changes)):

        avg_gain = (
            avg_gain * (period - 1)
            + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1)
            + losses[i]
        ) / period

    if avg_loss == 0:

        return (
            100.0
            if avg_gain > 0
            else 50.0
        )

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def calculate_vwap(history):

    rows = [
        candle
        for candle in history
        if float(candle.get("volume", 0)) > 0
    ]

    total = sum(
        float(candle["volume"])
        for candle in rows
    )

    if total <= 0:
        return None

    weighted = sum(
        (
            float(candle["high"])
            + float(candle["low"])
            + float(candle["close"])
        ) / 3 * float(candle["volume"])
        for candle in rows
    )

    return weighted / total


# ==========================================
# PROMEDIO BRTI ÚLTIMOS 60 SEGUNDOS
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

    return (
        sum(recent) / len(recent),
        len(recent)
    )


# ==========================================
# PREDICCIÓN BRTI — 15 MINUTOS
# ==========================================

def calculate_prediction(
    price,
    ticks,
    seconds_to_close,
    market_target=None
):

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

    recent = sorted(
        [
            (timestamp, value)
            for timestamp, value in ticks
            if now - timestamp <= 120
            and timestamp <= now + 1
        ],
        key=lambda item: item[0]
    )

    prices = [
        value
        for _, value in recent
    ]

    ema9 = calculate_ema(prices, 9)
    ema21 = calculate_ema(prices, 21)
    rsi = calculate_rsi(prices)

    momentum = 0.0
    short_momentum = 0.0
    velocity = 0.0

    if len(recent) >= 2:

        first_time, first_price = recent[0]
        last_time, last_price = recent[-1]

        elapsed = max(
            last_time - first_time,
            1
        )

        momentum = (
            last_price / first_price - 1
        ) * 100

        velocity = (
            last_price - first_price
        ) / elapsed

        older = [
            value
            for timestamp, value in recent
            if last_time - timestamp >= 10
        ]

        if older:

            short_momentum = (
                last_price / older[-1] - 1
            ) * 100

    remaining = max(
        0,
        min(900, seconds_to_close)
    )

    # Proyección moderada de velocidad.

    projected_change = (
        velocity * remaining * 0.18 / price
    ) * 100

    projected_change = max(
        -0.35,
        min(0.35, projected_change)
    )

    predicted = price * (
        1 + projected_change / 100
    )

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

    # Influencia del objetivo Kalshi.

    target_influence = 0

    if valid_price(market_target):

        possible += 3

        target_gap = (
            predicted / market_target - 1
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
        if possible > 0
        else 0
    )

    bullish = max(
        5,
        min(
            95,
            round(50 + normalized * 35, 1)
        )
    )

    bearish = round(
        100 - bullish,
        1
    )

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
        "technical_strength": round(
            abs(normalized) * 100
        ),
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

        rows.sort(
            key=lambda candle: candle["time"]
        )

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
                            item.get("price"),
                            item.get("time")
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
                    False,
                    err
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
# DETECTAR VALOR BRTI
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
# KALSHI WEBSOCKET — BRTI PRINCIPAL
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

                error = str(
                    data.get("msg", data)
                )

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

            set_brti(
                value,
                normalize_timestamp(timestamp)
            )

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
                    False,
                    err
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
                "series_ticker": MARKET_SERIES,
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
                    "No se encontró un contrato "
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
            dict(candle)
            for candle in brti_candles
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

    # BRTI siempre es la fuente principal.
    # Coinbase queda como respaldo, no sustituye
    # el precio BRTI cuando está disponible.

    source_price = bp

    if source_price is None and coinbase_live:
        source_price = cp

    indicators = calculate_prediction(
        source_price,
        ticks,
        countdown,
        market_target
    )

    average, average_count = calculate_brti_average(
        ticks
    )

    projected = indicators.get(
        "predicted_price"
    )

    target_gap_pct = (
        round(
            (projected / market_target - 1) * 100,
            5
        )
        if projected is not None
        and valid_price(market_target)
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

        "source": (
            "BRTI"
            if bp is not None
            else "COINBASE"
            if source_price is not None
            else "SIN DATOS"
        ),

        "brti_price": (
            round(bp, 2)
            if bp is not None
            else None
        ),

        "brti_live": brti_live,
        "brti_age": brti_age,
        "brti_samples": len(ticks),

        "kalshi_connected": kc,
        "kalshi_error": ke,

        "coinbase_price": (
            round(cp, 2)
            if cp is not None
            else None
        ),

        "coinbase_live": coinbase_live,
        "coinbase_age": coinbase_age,
        "coinbase_connected": cc,
        "coinbase_error": ce,

        "price_difference": difference,

        "brti_candles": candles,

        "countdown": countdown,

        "market_ticker": market_ticker,

        "market_target": (
            round(market_target, 2)
            if valid_price(market_target)
            else None
        ),

        "market_close_time": market_close_time,
        "market_target_gap_pct": target_gap_pct,
        "contract_direction": contract_direction,
        "market_error": me,

        "brti_average_60s": (
            round(average, 2)
            if average is not None
            else None
        ),

        "brti_average_samples": average_count,

        **indicators
    }
    # ==========================================
# DISEÑO VISUAL DEL RADAR
# ==========================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>BRTI Bitcoin Radar</title>

<style>

:root {
    --bg: #080b14;
    --panel: #111827;
    --panel2: #151e30;
    --border: #25334a;
    --text: #f4f7ff;
    --muted: #8492aa;
    --green: #00e69a;
    --red: #ff526f;
    --blue: #39a8ff;
    --purple: #a879ff;
    --yellow: #ffd166;
}

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 12px;
    background:
        radial-gradient(
            circle at 50% -15%,
            #172746 0,
            var(--bg) 48%
        );
    color: var(--text);
    font-family: Arial, sans-serif;
}

.app {
    width: 100%;
    max-width: 1100px;
    margin: auto;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
}

.brand {
    font-size: 19px;
    font-weight: 900;
    letter-spacing: 1px;
}

.brand span {
    color: var(--green);
}

.status {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 10px;
    color: var(--muted);
}

.dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--red);
    display: inline-block;
}

.dot.live {
    background: var(--green);
    box-shadow: 0 0 9px var(--green);
}

.price-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 9px;
}

.card {
    background: linear-gradient(
        145deg,
        var(--panel2),
        var(--panel)
    );
    border: 1px solid var(--border);
    border-radius: 15px;
    padding: 13px;
    min-width: 0;
}

.card-label {
    color: var(--muted);
    font-size: 10px;
    font-weight: bold;
    text-transform: uppercase;
    letter-spacing: .7px;
}

.price {
    font-size: clamp(20px, 5vw, 28px);
    font-weight: 900;
    margin-top: 9px;
    white-space: nowrap;
}

.price.green {
    color: var(--green);
}

.price.purple {
    color: var(--purple);
}

.sub {
    color: var(--muted);
    font-size: 10px;
    margin-top: 6px;
}

.market-strip {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
    margin-top: 9px;
}

.market-item {
    background: #101a2b;
    border: 1px solid var(--border);
    border-radius: 11px;
    padding: 10px;
    min-width: 0;
}

.market-label {
    color: var(--muted);
    font-size: 9px;
    text-transform: uppercase;
}

.market-value {
    font-size: 15px;
    font-weight: 800;
    margin-top: 5px;
    overflow-wrap: anywhere;
}

.market-value.yellow {
    color: var(--yellow);
}

.market-value.blue {
    color: var(--blue);
}

.probability {
    margin-top: 10px;
    padding: 12px;
    border: 1px solid var(--border);
    border-radius: 14px;
    background: var(--panel);
}

.prob-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 11px;
    font-weight: bold;
    margin-bottom: 10px;
}

.bull {
    color: var(--green);
}

.bear {
    color: var(--red);
}

.prob-numbers {
    display: flex;
    justify-content: space-between;
    font-size: 20px;
    font-weight: 900;
    margin-bottom: 8px;
}

.prob-track {
    display: flex;
    width: 100%;
    height: 11px;
    background: #293448;
    overflow: hidden;
    border-radius: 20px;
}

.bull-fill {
    width: 50%;
    background: var(--green);
    transition: width .25s ease;
}

.bear-fill {
    flex: 1;
    background: var(--red);
    transition: width .25s ease;
}

.signal-row {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    align-items: center;
    margin-top: 9px;
    font-size: 10px;
    color: var(--muted);
}

.signal {
    font-weight: 900;
    letter-spacing: .6px;
}

.signal.up {
    color: var(--green);
}

.signal.down {
    color: var(--red);
}

.signal.neutral {
    color: var(--yellow);
}

.chart-panel {
    margin-top: 11px;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 15px;
    padding: 12px;
}

.chart-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
}

.chart-title {
    font-size: 12px;
    font-weight: 900;
}

.chart-tools {
    display: flex;
    gap: 6px;
}

.chart-tools button {
    border: 1px solid var(--border);
    background: #1a2940;
    color: var(--text);
    border-radius: 7px;
    padding: 6px 9px;
    font-size: 11px;
}

.chart-wrap {
    width: 100%;
    height: 290px;
    position: relative;
    overflow: hidden;
    touch-action: none;
}

canvas {
    width: 100%;
    height: 100%;
    display: block;
}

.chart-legend {
    display: flex;
    gap: 13px;
    flex-wrap: wrap;
    font-size: 9px;
    color: var(--muted);
    margin-top: 7px;
}

.legend-line {
    display: inline-block;
    width: 12px;
    height: 3px;
    vertical-align: middle;
    margin-right: 4px;
    border-radius: 3px;
}

.connection-panel {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-top: 10px;
    padding: 10px;
    background: #101827;
    border: 1px solid var(--border);
    border-radius: 11px;
    font-size: 10px;
    color: var(--muted);
}

.connection {
    display: flex;
    gap: 6px;
    align-items: center;
}

.footer {
    text-align: center;
    color: #65738a;
    font-size: 9px;
    padding: 14px 0 5px;
}

@media (min-width: 700px) {

    body {
        padding: 22px;
    }

    .chart-wrap {
        height: 390px;
    }

    .price {
        font-size: 30px;
    }
}

</style>
</head>

<body>

<div class="app">

    <div class="header">

        <div class="brand">
            BTC <span>RADAR</span>
        </div>

        <div class="status">
            <span class="dot" id="brtiDot"></span>
            <span id="brtiStatus">BRTI CONECTANDO</span>
        </div>

    </div>


    <!-- DOS TARJETAS PRINCIPALES -->

    <div class="price-grid">

        <div class="card">

            <div class="card-label">
                Precio actual · BRTI
            </div>

            <div class="price green" id="brtiPrice">
                --
            </div>

            <div class="sub" id="brtiAge">
                Esperando datos
            </div>

        </div>


        <div class="card">

            <div class="card-label">
                Predicción · Cierre 15 min
            </div>

            <div class="price purple" id="predictedPrice">
                --
            </div>

            <div class="sub" id="direction">
                RECOPILANDO
            </div>

        </div>

    </div>


    <!-- OBJETIVO Y CUENTA REGRESIVA -->

    <div class="market-strip">

        <div class="market-item">

            <div class="market-label">
                Objetivo Kalshi
            </div>

            <div class="market-value yellow" id="marketTargetTop">
                --
            </div>

            <div class="sub" id="marketTicker">
                Contrato no disponible
            </div>

        </div>


        <div class="market-item">

            <div class="market-label">
                Tiempo restante
            </div>

            <div class="market-value blue" id="countdownTop">
                15:00
            </div>

            <div class="sub" id="marketStatus">
                Esperando contrato
            </div>

        </div>

    </div>


    <!-- PROBABILIDADES -->

    <div class="probability">

        <div class="prob-head">

            <span class="bull">
                ALCISTA
            </span>

            <span class="bear">
                BAJISTA
            </span>

        </div>

        <div class="prob-numbers">

            <span class="bull" id="bullText">
                50%
            </span>

            <span class="bear" id="bearText">
                50%
            </span>

        </div>

        <div class="prob-track">

            <div
                class="bull-fill"
                id="bullBar"
            ></div>

            <div
                class="bear-fill"
                id="bearBar"
            ></div>

        </div>

        <div class="signal-row">

            <span>
                Señal técnica:
                <strong id="strength">0%</strong>
            </span>

            <span
                class="signal neutral"
                id="signalStage"
            >
                INDECISO
            </span>

        </div>

    </div>


    <!-- GRÁFICA -->

    <div class="chart-panel">

        <div class="chart-header">

            <div class="chart-title">
                BRTI · VELAS DE 1 MINUTO
            </div>

            <div class="chart-tools">

                <button
                    type="button"
                    onclick="zoomChart(1.25)"
                >
                    +
                </button>

                <button
                    type="button"
                    onclick="zoomChart(0.8)"
                >
                    −
                </button>

                <button
                    type="button"
                    onclick="goLive()"
                >
                    LIVE
                </button>

            </div>

        </div>

        <div class="chart-wrap" id="chartWrap">

            <canvas id="chart"></canvas>

        </div>

        <div class="chart-legend">

            <span>
                <i
                    class="legend-line"
                    style="background:#a879ff"
                ></i>
                EMA 9
            </span>

            <span>
                <i
                    class="legend-line"
                    style="background:#39a8ff"
                ></i>
                EMA 21
            </span>

            <span class="bull">
                Vela alcista
            </span>

            <span class="bear">
                Vela bajista
            </span>

        </div>

    </div>


    <!-- CONEXIONES -->

    <div class="connection-panel">

        <div class="connection">
            <span class="dot" id="brtiConnection"></span>
            BRTI
        </div>

        <div class="connection">
            <span class="dot" id="coinDot"></span>
            COINBASE
        </div>

        <div class="connection">
            <span class="dot" id="kalshiDot"></span>
            KALSHI
        </div>

        <span id="coinStatus">
            Respaldo
        </span>

        <span id="kalshiConnection">
            Contrato
        </span>

    </div>


    <div class="footer">
        BRTI PRINCIPAL · COINBASE RESPALDO · KALSHI CONTRATO
    </div>

</div>
<script>

// ==========================================
// VARIABLES DE LA GRÁFICA
// ==========================================

const canvas = document.getElementById("chart");
const ctx = canvas.getContext("2d");
const chartWrap = document.getElementById("chartWrap");

let candles = [];
let chartOffset = 0;
let chartScale = 1;

let pointers = new Map();
let previousX = 0;
let initialDistance = 0;
let initialZoom = 1;

let lastData = null;
let busy = false;


// ==========================================
// UTILIDADES VISUALES
// ==========================================

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


function clock(seconds) {

    seconds = Math.max(
        0,
        Math.floor(Number(seconds) || 0)
    );

    const minutes = Math.floor(seconds / 60);
    const remaining = seconds % 60;

    return (
        String(minutes).padStart(2, "0")
        + ":"
        + String(remaining).padStart(2, "0")
    );
}


function setText(id, value) {

    const element = document.getElementById(id);

    if (element) {
        element.textContent = value;
    }
}


function setDot(id, live) {

    const element = document.getElementById(id);

    if (!element) return;

    element.classList.toggle(
        "live",
        Boolean(live)
    );
}


// ==========================================
// EMA PARA LA GRÁFICA
// ==========================================

function emaSeries(values, period) {

    if (!values.length) return [];

    const alpha = 2 / (period + 1);

    let result = Number(values[0]);

    return values.map((value, index) => {

        if (index > 0) {

            result =
                Number(value) * alpha
                + result * (1 - alpha);
        }

        return result;
    });
}


// ==========================================
// DIBUJAR GRÁFICA
// ==========================================

function drawChart(data) {

    if (data && Array.isArray(data)) {
        candles = data;
    }

    const width = chartWrap.clientWidth;
    const height = chartWrap.clientHeight;

    if (!width || !height) return;

    const ratio = window.devicePixelRatio || 1;

    canvas.width = width * ratio;
    canvas.height = height * ratio;

    ctx.setTransform(
        ratio,
        0,
        0,
        ratio,
        0,
        0
    );

    ctx.clearRect(0, 0, width, height);

    ctx.fillStyle = "#111827";
    ctx.fillRect(0, 0, width, height);

    const valid = candles
        .filter(c =>
            Number.isFinite(Number(c.open))
            && Number.isFinite(Number(c.close))
            && Number.isFinite(Number(c.high))
            && Number.isFinite(Number(c.low))
        )
        .sort((a, b) => Number(a.time) - Number(b.time));

    if (!valid.length) {

        ctx.fillStyle = "#8492aa";
        ctx.font = "12px Arial";
        ctx.textAlign = "center";

        ctx.fillText(
            "Esperando velas BRTI...",
            width / 2,
            height / 2
        );

        return;
    }

    const rightSpace = 58;
    const topSpace = 15;
    const bottomSpace = 24;

    const plotWidth = width - rightSpace;
    const plotHeight = height - topSpace - bottomSpace;

    const candleWidth = Math.max(
        3,
        Math.min(13, 10 * chartScale)
    );

    const step = candleWidth * 1.65;

    const visibleCount = Math.max(
        5,
        Math.floor(plotWidth / step)
    );

    const endIndex = Math.max(
        1,
        valid.length - Math.floor(chartOffset)
    );

    const startIndex = Math.max(
        0,
        endIndex - visibleCount
    );

    const visible = valid.slice(
        startIndex,
        endIndex
    );

    if (!visible.length) return;

    const values = visible.flatMap(c => [
        Number(c.high),
        Number(c.low)
    ]);

    let minPrice = Math.min(...values);
    let maxPrice = Math.max(...values);

    const padding = Math.max(
        (maxPrice - minPrice) * 0.12,
        maxPrice * 0.00015
    );

    minPrice -= padding;
    maxPrice += padding;

    const range = maxPrice - minPrice || 1;

    function y(price) {

        return (
            topSpace
            + (maxPrice - price) / range * plotHeight
        );
    }

    // Cuadrícula y precios.

    ctx.font = "10px Arial";
    ctx.textAlign = "left";
    ctx.fillStyle = "#8492aa";
    ctx.strokeStyle = "#25334a";
    ctx.lineWidth = 1;

    for (let i = 0; i <= 4; i++) {

        const price =
            maxPrice - range * i / 4;

        const py = y(price);

        ctx.beginPath();
        ctx.moveTo(0, py);
        ctx.lineTo(plotWidth, py);
        ctx.stroke();

        ctx.fillText(
            money(price),
            plotWidth + 5,
            py + 3
        );
    }

    // Líneas verticales de tiempo.

    for (let i = 0; i < 4; i++) {

        const x = plotWidth * i / 3;

        ctx.strokeStyle = "#1c2a3d";

        ctx.beginPath();
        ctx.moveTo(x, topSpace);
        ctx.lineTo(x, height - bottomSpace);
        ctx.stroke();
    }

    // EMA 9 y EMA 21.

    const closes = valid.map(
        c => Number(c.close)
    );

    const ema9 = emaSeries(closes, 9);
    const ema21 = emaSeries(closes, 21);

    function drawLine(series, color) {

        ctx.beginPath();

        let started = false;

        for (
            let i = startIndex;
            i < endIndex;
            i++
        ) {

            const localIndex = i - startIndex;

            const x =
                plotWidth
                - (visible.length - 1 - localIndex) * step
                - candleWidth / 2;

            const py = y(series[i]);

            if (!Number.isFinite(py)) continue;

            if (!started) {

                ctx.moveTo(x, py);
                started = true;

            } else {

                ctx.lineTo(x, py);
            }
        }

        ctx.strokeStyle = color;
        ctx.lineWidth = 1.7;
        ctx.stroke();
    }

    drawLine(ema21, "#39a8ff");
    drawLine(ema9, "#a879ff");

    // Velas.

    visible.forEach((candle, index) => {

        const x =
            plotWidth
            - (visible.length - 1 - index) * step
            - candleWidth / 2;

        const open = Number(candle.open);
        const close = Number(candle.close);
        const high = Number(candle.high);
        const low = Number(candle.low);

        const bullish = close >= open;

        const color = bullish
            ? "#00e69a"
            : "#ff526f";

        const openY = y(open);
        const closeY = y(close);
        const highY = y(high);
        const lowY = y(low);

        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;

        // Mecha.

        ctx.beginPath();

        ctx.moveTo(
            x + candleWidth / 2,
            highY
        );

        ctx.lineTo(
            x + candleWidth / 2,
            lowY
        );

        ctx.stroke();

        // Cuerpo.

        const bodyTop = Math.min(
            openY,
            closeY
        );

        const bodyHeight = Math.max(
            2,
            Math.abs(closeY - openY)
        );

        ctx.fillStyle = color;

        ctx.fillRect(
            x,
            bodyTop,
            candleWidth,
            bodyHeight
        );

    });

    // Precio actual.

    const latest = visible[visible.length - 1];

    if (latest) {

        const currentY = y(
            Number(latest.close)
        );

        ctx.setLineDash([4, 4]);
        ctx.strokeStyle = "#ffd166";

        ctx.beginPath();

        ctx.moveTo(0, currentY);
        ctx.lineTo(plotWidth, currentY);

        ctx.stroke();

        ctx.setLineDash([]);

        ctx.fillStyle = "#ffd166";

        ctx.fillRect(
            plotWidth + 2,
            currentY - 9,
            rightSpace - 4,
            18
        );

        ctx.fillStyle = "#080b14";
        ctx.font = "bold 9px Arial";

        ctx.textAlign = "center";

        ctx.fillText(
            money(latest.close),
            plotWidth + rightSpace / 2,
            currentY + 3
        );
    }

    // Hora de las velas.

    ctx.fillStyle = "#8492aa";
    ctx.font = "9px Arial";
    ctx.textAlign = "center";

    visible.forEach((candle, index) => {

        if (index % Math.max(
            1,
            Math.floor(visible.length / 5)
        ) !== 0) return;

        const x =
            plotWidth
            - (visible.length - 1 - index) * step
            - candleWidth / 2;

        const date = new Date(
            Number(candle.time) * 1000
        );

        ctx.fillText(
            date.toLocaleTimeString(
                "es-US",
                {
                    hour: "2-digit",
                    minute: "2-digit"
                }
            ),
            x,
            height - 7
        );
    });
}


// ==========================================
// ZOOM Y MOVIMIENTO
// ==========================================

function zoomChart(factor) {

    chartScale = Math.max(
        0.45,
        Math.min(3, chartScale * factor)
    );

    drawChart();
}


function goLive() {

    chartOffset = 0;
    drawChart();
}


chartWrap.addEventListener(
    "pointerdown",
    event => {

        pointers.set(
            event.pointerId,
            {
                x: event.clientX,
                y: event.clientY
            }
        );

        chartWrap.setPointerCapture(
            event.pointerId
        );

        if (pointers.size === 1) {
            previousX = event.clientX;
        }

        if (pointers.size === 2) {

            const points = [
                ...pointers.values()
            ];

            initialDistance = Math.hypot(
                points[0].x - points[1].x,
                points[0].y - points[1].y
            );

            initialZoom = chartScale;
        }
    }
);


chartWrap.addEventListener(
    "pointermove",
    event => {

        if (!pointers.has(event.pointerId)) {
            return;
        }

        const old = pointers.get(
            event.pointerId
        );

        pointers.set(
            event.pointerId,
            {
                x: event.clientX,
                y: event.clientY
            }
        );

        if (pointers.size === 1) {

            const delta =
                event.clientX - previousX;

            previousX = event.clientX;

            const step =
                Math.max(
                    3,
                    10 * chartScale
                ) * 1.65;

            chartOffset -= delta / step;

            chartOffset = Math.max(
                0,
                Math.min(
                    candles.length - 5,
                    chartOffset
                )
            );

            drawChart();

        } else if (pointers.size === 2) {

            const points = [
                ...pointers.values()
            ];

            const distance = Math.hypot(
                points[0].x - points[1].x,
                points[0].y - points[1].y
            );

            if (initialDistance > 0) {

                chartScale = Math.max(
                    0.45,
                    Math.min(
                        3,
                        initialZoom
                        * distance
                        / initialDistance
                    )
                );

                drawChart();
            }
        }
    }
);


function releasePointer(event) {

    pointers.delete(event.pointerId);

    if (pointers.size === 1) {

        const remaining = [
            ...pointers.values()
        ][0];

        previousX = remaining.x;
    }
}


chartWrap.addEventListener(
    "pointerup",
    releasePointer
);

chartWrap.addEventListener(
    "pointercancel",
    releasePointer
);

chartWrap.addEventListener(
    "lostpointercapture",
    releasePointer
);


chartWrap.addEventListener(
    "wheel",
    event => {

        event.preventDefault();

        zoomChart(
            event.deltaY < 0 ? 1.1 : 0.9
        );

    },
    { passive: false }
);


// ==========================================
// ACTUALIZAR INFORMACIÓN DEL RADAR
// ==========================================

function updateRadar(data) {

    if (!data || !data.ok) return;

    lastData = data;

    const price = data.brti_price;

    setText(
        "brtiPrice",
        money(price)
    );

    setText(
        "predictedPrice",
        money(data.predicted_price)
    );

    setText(
        "brtiAge",
        data.brti_age === null
            ? "Sin conexión"
            : "Actualizado hace " + data.brti_age + " s"
    );

    const direction = data.direction || "INDECISO";

    setText(
        "direction",
        direction
    );

    setText(
        "bullText",
        Number(data.bullish_score || 50).toFixed(1) + "%"
    );

    setText(
        "bearText",
        Number(data.bearish_score || 50).toFixed(1) + "%"
    );

    const bull = Math.max(
        0,
        Math.min(
            100,
            Number(data.bullish_score || 50)
        )
    );

    document.getElementById(
        "bullBar"
    ).style.width = bull + "%";

    document.getElementById(
        "bearBar"
    ).style.width = (100 - bull) + "%";

    setText(
        "strength",
        Number(data.technical_strength || 0) + "%"
    );

    const signal = document.getElementById(
        "signalStage"
    );

    signal.textContent = direction;

    signal.className = "signal " + (
        direction === "ALCISTA"
            ? "up"
            : direction === "BAJISTA"
            ? "down"
            : "neutral"
    );

    setText(
        "marketTargetTop",
        money(data.market_target)
    );

    setText(
        "marketTicker",
        data.market_ticker || "Contrato no disponible"
    );

    setText(
        "countdownTop",
        clock(data.countdown)
    );

    setText(
        "marketStatus",
        data.contract_direction || "SIN OBJETIVO"
    );

    setDot(
        "brtiDot",
        data.brti_live
    );

    setDot(
        "brtiConnection",
        data.brti_live
    );

    setDot(
        "coinDot",
        data.coinbase_live
    );

    setDot(
        "kalshiDot",
        data.kalshi_connected
    );

    setText(
        "brtiStatus",
        data.brti_live
            ? "BRTI EN VIVO"
            : "BRTI SIN ACTUALIZAR"
    );

    setText(
        "coinStatus",
        data.coinbase_live
            ? "Coinbase conectado"
            : "Coinbase respaldo inactivo"
    );

    setText(
        "kalshiConnection",
        data.kalshi_connected
            ? "Kalshi conectado"
            : "Kalshi desconectado"
    );

    if (
        Array.isArray(data.brti_candles)
        && data.brti_candles.length
    ) {

        candles = data.brti_candles;

        drawChart();
    }
}


// ==========================================
// ACTUALIZACIÓN AUTOMÁTICA
// ==========================================

async function refreshData() {

    if (busy) return;

    busy = true;

    try {

        const response = await fetch(
            "/api?t=" + Date.now(),
            {
                cache: "no-store"
            }
        );

        if (!response.ok) {
            throw new Error(
                "HTTP " + response.status
            );
        }

        const data = await response.json();

        updateRadar(data);

    } catch (error) {

        setText(
            "brtiStatus",
            "ERROR DE CONEXIÓN"
        );

        console.error(
            "Radar:",
            error
        );

    } finally {

        busy = false;
    }
}


// ==========================================
// ARRANQUE
// ==========================================

window.addEventListener(
    "resize",
    () => drawChart()
);

refreshData();

setInterval(
    refreshData,
    1000
);

</script>
</body>
</html>
"""


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

    response = jsonify(
        get_data()
    )

    response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, max-age=0"
    )

    return response


@app.route("/health")
def health():

    start_services()

    data = get_data()

    return jsonify({

        "ok": True,

        "source": data["source"],

        "brti_live": data["brti_live"],
        "brti_price": data["brti_price"],
        "brti_age": data["brti_age"],
        "brti_samples": data["brti_samples"],

        "coinbase_live": data["coinbase_live"],
        "coinbase_connected": data["coinbase_connected"],
        "coinbase_price": data["coinbase_price"],

        "kalshi_connected": data["kalshi_connected"],
        "kalshi_error": data["kalshi_error"],

        "market_ticker": data["market_ticker"],
        "market_target": data["market_target"],
        "market_error": data["market_error"],

        "velas": len(data["brti_candles"])
    })


# ==========================================
# INICIO LOCAL
# ==========================================

if __name__ == "__main__":

    start_services()

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False
    )
