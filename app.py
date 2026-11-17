import os
import json
import time
import math
import base64
import threading
from datetime import datetime, timezone
from collections import deque

import requests
import websocket

from flask import Flask, jsonify, render_template_string

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import pattern_memory
app = Flask(__name__)

# ==========================================
# CONFIGURACIÓN
# ==========================================

KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"
KALSHI_REST = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

INDEX_ID = "BRTI"
PRODUCT = "BTC-USD"

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
kalshi_market_target = None
kalshi_market_ticker = ""
kalshi_market_close = None
kalshi_market_error = ""

started = False
last_pattern_snapshot_minute = None
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
        timestamp = float(value)

        if timestamp > 10_000_000_000:
            timestamp /= 1000

        return timestamp

    except (TypeError, ValueError):
        try:
            text = str(value).strip()

            if text.endswith("Z"):
                text = text[:-1] + "+00:00"

            parsed = datetime.fromisoformat(text)

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            return parsed.timestamp()

        except (TypeError, ValueError, OverflowError):
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
    """Construye velas OHLC de 1 minuto y ordena ticks atrasados."""

    timestamp = normalize_timestamp(timestamp)
    price = float(price)

    if not valid_price(price):
        return

    bucket = int(timestamp // 60) * 60
    rows = list(target)

    candle = next(
        (row for row in rows if int(row["time"]) == bucket),
        None
    )

    if candle is None:
        candle = {
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": float(volume or 0),
            "_open_ts": timestamp,
            "_close_ts": timestamp
        }
        rows.append(candle)
    else:
        candle["high"] = max(float(candle["high"]), price)
        candle["low"] = min(float(candle["low"]), price)
        candle["volume"] = (
            float(candle.get("volume", 0))
            + float(volume or 0)
        )

        open_ts = float(candle.get("_open_ts", bucket))
        close_ts = float(candle.get("_close_ts", bucket))

        if timestamp < open_ts:
            candle["open"] = price
            candle["_open_ts"] = timestamp

        if timestamp >= close_ts:
            candle["close"] = price
            candle["_close_ts"] = timestamp

    rows.sort(key=lambda row: int(row["time"]))

    target.clear()
    target.extend(rows[-MAX_CANDLES:])


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
        update_candle(brti_candles, price, timestamp)


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

    pattern_memory.record_price(price, timestamp)


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
# ANÁLISIS DE MOVIMIENTO CON VELAS COINBASE
# ==========================================

def calculate_prediction(
    price,
    ticks,
    seconds_to_close,
    market_target=None,
    candles=None
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
            "pattern": "SIN DATOS",
            "target_influence": "SIN DATOS"
        }

    # --------------------------------------
    # PREPARAR HISTORIAL DE VELAS COINBASE
    # --------------------------------------

    valid_candles = []

    for candle in (candles or []):

        try:
            open_price = float(candle["open"])
            high = float(candle["high"])
            low = float(candle["low"])
            close = float(candle["close"])
            volume = float(candle.get("volume", 0))

            if not all(math.isfinite(x) for x in (
                open_price, high, low, close, volume
            )):
                continue

            if min(open_price, high, low, close) <= 0:
                continue

            if high < low:
                continue

            valid_candles.append({
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": max(0, volume)
            })

        except (TypeError, ValueError, KeyError):
            continue

    # Orden cronológico: antiguas primero
    valid_candles = valid_candles[-300:]

    closes = [
        candle["close"]
        for candle in valid_candles
    ]

    if len(valid_candles) < 3:

        return {
            "direction": "RECOPILANDO",
            "predicted_price": round(float(price), 2),
            "bullish_score": 50,
            "bearish_score": 50,
            "technical_strength": 0,
            "momentum_pct": 0,
            "ema9": None,
            "ema21": None,
            "rsi": None,
            "pattern": "RECOPILANDO VELAS",
            "target_influence": "PENDIENTE"
        }

    # --------------------------------------
    # INDICADORES TÉCNICOS
    # --------------------------------------

    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes, 14)

    # Momentum de las últimas cinco velas
    reference_index = max(0, len(closes) - 6)

    reference_price = closes[reference_index]
    latest_close = closes[-1]

    momentum = (
        (latest_close / reference_price) - 1
    ) * 100 if reference_price > 0 else 0

    # --------------------------------------
    # ANÁLISIS DE LA ÚLTIMA VELA
    # --------------------------------------

    last = valid_candles[-1]

    previous = valid_candles[-2]

    body = abs(last["close"] - last["open"])

    candle_range = max(
        last["high"] - last["low"],
        0.00000001
    )

    upper_wick = (
        last["high"] - max(last["open"], last["close"])
    )

    lower_wick = (
        min(last["open"], last["close"]) - last["low"]
    )

    bullish_candle = last["close"] > last["open"]
    bearish_candle = last["close"] < last["open"]

    pattern = "SIN PATRÓN CLARO"

    # Envolvente alcista
    if (
        previous["close"] < previous["open"]
        and bullish_candle
        and last["open"] <= previous["close"]
        and last["close"] >= previous["open"]
    ):
        pattern = "ENVOLVENTE ALCISTA"

    # Envolvente bajista
    elif (
        previous["close"] > previous["open"]
        and bearish_candle
        and last["open"] >= previous["close"]
        and last["close"] <= previous["open"]
    ):
        pattern = "ENVOLVENTE BAJISTA"

    # Martillo
    elif (
        lower_wick >= body * 2
        and upper_wick <= max(body, 0.00000001)
        and body / candle_range <= 0.40
    ):
        pattern = "POSIBLE MARTILLO"

    # Estrella fugaz
    elif (
        upper_wick >= body * 2
        and lower_wick <= max(body, 0.00000001)
        and body / candle_range <= 0.40
    ):
        pattern = "POSIBLE ESTRELLA FUGAZ"

    # Doji
    elif body / candle_range <= 0.10:
        pattern = "DOJI"

    # --------------------------------------
    # TENDENCIA DE TRES VELAS
    # --------------------------------------

    if len(valid_candles) >= 3:

        a, b, c = valid_candles[-3:]

        if (
            a["close"] > a["open"]
            and b["close"] > b["open"]
            and c["close"] > c["open"]
            and c["close"] > b["close"] > a["close"]
        ):
            pattern = "TRES VELAS ALCISTAS"

        elif (
            a["close"] < a["open"]
            and b["close"] < b["open"]
            and c["close"] < c["open"]
            and c["close"] < b["close"] < a["close"]
        ):
            pattern = "TRES VELAS BAJISTAS"

    # --------------------------------------
    # CÁLCULO DE FUERZA ALCISTA Y BAJISTA
    # --------------------------------------

    score = 0.0
    possible = 0.0

    # Momentum
    possible += 3

    if momentum > 0:
        score += 3
    elif momentum < 0:
        score -= 3

    # EMA 9 y EMA 21
    if ema9 is not None and ema21 is not None:

        possible += 3

        if ema9 > ema21:
            score += 3

        elif ema9 < ema21:
            score -= 3

    # Precio frente a EMA 9
    if ema9 is not None:

        possible += 2

        if price > ema9:
            score += 2

        elif price < ema9:
            score -= 2

    # RSI
    if rsi is not None:

        possible += 2

        if rsi > 55:
            score += 2

        elif rsi < 45:
            score -= 2

    # Patrón de velas
    possible += 2

    if pattern in (
        "ENVOLVENTE ALCISTA",
        "TRES VELAS ALCISTAS"
    ):
        score += 2

    elif pattern in (
        "ENVOLVENTE BAJISTA",
        "TRES VELAS BAJISTAS"
    ):
        score -= 2

    elif pattern == "POSIBLE MARTILLO":
        score += 1

    elif pattern == "POSIBLE ESTRELLA FUGAZ":
        score -= 1

    # Dirección de la última vela
    possible += 1

    if bullish_candle:
        score += 1

    elif bearish_candle:
        score -= 1

    # Volumen comparado con las últimas velas
    recent_volumes = [
        c["volume"]
        for c in valid_candles[-6:]
    ]

    average_volume = (
        sum(recent_volumes[:-1]) / len(recent_volumes[:-1])
        if len(recent_volumes) > 1 else 0
    )

    if average_volume > 0:

        possible += 1

        if last["volume"] > average_volume:

            if bullish_candle:
                score += 1

            elif bearish_candle:
                score -= 1

    # --------------------------------------
    # PROYECCIÓN HASTA EL CIERRE
    # --------------------------------------

    remaining = max(
        0,
        min(900, float(seconds_to_close or 0))
    )

    remaining_minutes = remaining / 60

    # Movimiento medio reciente por vela
    if len(closes) >= 6 and closes[-6] > 0:

        recent_change = (
            (closes[-1] / closes[-6]) - 1
        ) * 100

        projected_change = (
            recent_change
            * (remaining_minutes / 5)
            * 0.15
        )

    else:
        projected_change = 0

    projected_change = max(
        -0.50,
        min(0.50, projected_change)
    )

    predicted = float(price) * (
        1 + projected_change / 100
    )

    # --------------------------------------
    # INFLUENCIA DEL OBJETIVO KALSHI
    # --------------------------------------

    target_influence = "SIN OBJETIVO"

    if valid_price(market_target):
        difference = predicted - float(market_target)
        tolerance = float(market_target) * 0.00005

        if difference > tolerance:
            target_influence = "PROYECCIÓN SOBRE OBJETIVO"

        elif difference < -tolerance:
            target_influence = "PROYECCIÓN BAJO OBJETIVO"

        else:
            target_influence = "CERCA DEL OBJETIVO"

    # --------------------------------------
    # PORCENTAJES FINALES
    # --------------------------------------

    normalized = (
        score / possible
        if possible > 0 else 0
    )

    bullish = max(
        20,
        min(80, round(50 + normalized * 30, 1))
    )

    bearish = round(100 - bullish, 1)

    # --------------------------------------
    # DIRECCIÓN
    # --------------------------------------

    if normalized > 0.08:
        direction = "ALCISTA"

    elif normalized < -0.08:
        direction = "BAJISTA"

    else:
        direction = "INDECISO"

    # --------------------------------------
    # RESULTADO
    # --------------------------------------

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
        "pattern": pattern,
        "target_influence": target_influence
    }


# ==========================================
# HISTORIAL COINBASE
# ==========================================

def load_coinbase_history():
    """
    Descarga continuamente las últimas 300 velas Coinbase de 1 minuto.
    Si falla, reintenta; al actualizar conserva los ticks WebSocket
    de la vela actual para no borrar datos recientes.
    """
    global coinbase_error

    while True:
        try:
            response = session.get(
                f"{COINBASE_REST}/products/{PRODUCT}/candles",
                params={"granularity": 60},
                timeout=15
            )
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, list) or not payload:
                raise ValueError("Coinbase no devolvió historial de velas")

            history = {}

            for item in payload:
                if not isinstance(item, list) or len(item) < 6:
                    continue

                try:
                    timestamp = int(item[0])
                    low = float(item[1])
                    high = float(item[2])
                    open_price = float(item[3])
                    close = float(item[4])
                    volume = float(item[5])
                except (TypeError, ValueError, OverflowError):
                    continue

                values = (low, high, open_price, close, volume)
                if not all(math.isfinite(value) for value in values):
                    continue
                if min(low, high, open_price, close) <= 0 or high < low:
                    continue

                history[timestamp] = {
                    "time": timestamp,
                    "low": low,
                    "high": high,
                    "open": open_price,
                    "close": close,
                    "volume": volume
                }

            if not history:
                raise ValueError("Coinbase no devolvió velas válidas")

            with lock:
                for candle in coinbase_candles:
                    history[int(candle["time"])] = dict(candle)

                ordered = sorted(
                    history.values(),
                    key=lambda candle: int(candle["time"])
                )

                coinbase_candles.clear()
                coinbase_candles.extend(ordered[-MAX_CANDLES:])
                coinbase_error = ""

            time.sleep(60)

        except Exception as exc:
            with lock:
                coinbase_error = str(exc)
            time.sleep(15)


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
    
def find_brti_value(data):
    """Extrae el valor BRTI de los mensajes de Kalshi."""

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (ValueError, TypeError):
            return None

    if isinstance(data, list):
        for item in data:
            result = find_brti_value(item)
            if result is not None:
                return result
        return None

    if not isinstance(data, dict):
        return None

    index = (
        data.get("index_id")
        or data.get("ticker")
        or data.get("id")
    )

    timestamp = (
        data.get("ts_ms")
        or data.get("time")
        or data.get("timestamp")
        or data.get("received_at")
    )

    # Comprobar el índice BRTI
    if index == INDEX_ID:

        # Valor directo
        value = data.get("value")

        if value is not None:
            try:
                return float(value), timestamp
            except (ValueError, TypeError):
                pass

        # Datos anidados
        for key in ("data", "payload", "msg"):
            nested = data.get(key)

            if nested is not None:
                result = find_brti_value(nested)
                if result is not None:
                    return result

        # Promedio de 60 segundos
        average = data.get("avg_60s_data")

        if isinstance(average, dict):
            value = average.get("value")

            if value is not None:
                try:
                    return float(value), timestamp
                except (ValueError, TypeError):
                    pass

    # Buscar dentro de otros objetos
    for key in ("msg", "data", "payload"):
        nested = data.get(key)

        if isinstance(nested, (dict, list, str)):
            result = find_brti_value(nested)

            if result is not None:
                return result

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
# OBJETIVO DEL CONTRATO ACTIVO DE KALSHI
# ==========================================

def kalshi_market_worker():

    global kalshi_market_target
    global kalshi_market_ticker
    global kalshi_market_close
    global kalshi_market_error

    while True:

        try:

            response = session.get(
                f"{KALSHI_REST}/markets",
                params={
                    "series_ticker": KALSHI_SERIES,
                    "status": "open",
                    "limit": 100
                },
                timeout=12
            )

            response.raise_for_status()

            markets = response.json().get("markets", [])

            now = time.time()
            found = []

            for market in markets:

                raw = market.get("floor_strike")

                if raw is None:
                    raw = market.get("strike_price")

                if raw is None:
                    raw = market.get("cap_strike")

                try:
                    target = float(raw)
                except (TypeError, ValueError):
                    continue

                if not valid_price(target):
                    continue

                close_value = (
                    market.get("close_time")
                    or market.get("expiration_time")
                )

                close_ts = (
                    normalize_timestamp(close_value)
                    if close_value else now + 3600
                )

                if close_ts >= now - 30:

                    found.append((
                        close_ts,
                        str(market.get("ticker", "")),
                        target
                    ))

            if found:

                found.sort(key=lambda item: item[0])

                close_ts, ticker, target = found[0]

                with lock:

                    kalshi_market_target = target
                    kalshi_market_ticker = ticker
                    kalshi_market_close = close_ts
                    kalshi_market_error = ""

            else:

                with lock:

                    kalshi_market_error = (
                        "No se encontró floor_strike "
                        "en el contrato abierto."
                    )

        except Exception as exc:

            with lock:
                kalshi_market_error = str(exc)

        time.sleep(10)


# ==========================================
# INICIAR SERVICIOS UNA SOLA VEZ
# ==========================================

def start_services():

    global started

    with lock:

        if started:
            return

        started = True
        
    def init_pattern_database():
        while not pattern_memory.init_db():
            time.sleep(30)

    threading.Thread(
        target=init_pattern_database,
        daemon=True
    ).start()
    threading.Thread(
        target=load_coinbase_history,
        daemon=True
    ).start()

    threading.Thread(
        target=kalshi_market_worker,
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
            dict(c) for c in brti_candles
        ]

        coin_candles = [
            dict(c) for c in coinbase_candles
        ]

        market_target = kalshi_market_target
        market_ticker = kalshi_market_ticker
        market_close = kalshi_market_close
        market_error = kalshi_market_error

        kc = kalshi_connected
        cc = coinbase_connected

        ke = kalshi_error
        ce = coinbase_error

    now = time.time()

    countdown = max(0, int(market_close - now)) if market_close else 0

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
        cp,
        [],
        countdown,
        market_target,
        coin_candles
    )
    global last_pattern_snapshot_minute

    snapshot_minute = int(now // 60)
    should_save_snapshot = False

    if cp is not None and coinbase_live:
        with lock:
            if last_pattern_snapshot_minute != snapshot_minute:
                last_pattern_snapshot_minute = snapshot_minute
                should_save_snapshot = True

    if should_save_snapshot:
        pattern_memory.save_snapshot({
            "signal_ts": now,
            "price": cp,
            "market_ticker": market_ticker,
            "market_close_ts": market_close,
            "direction": indicators.get("direction"),
            "bullish_score": indicators.get("bullish_score"),
            "bearish_score": indicators.get("bearish_score"),
            "predicted_price": indicators.get("predicted_price"),
            "ema9": indicators.get("ema9"),
            "ema21": indicators.get("ema21"),
            "rsi": indicators.get("rsi"),
            "momentum_pct": indicators.get("momentum_pct"),
            "pattern": indicators.get("pattern"),
            "features": {
    "target_influence": indicators.get("target_influence"),
    "technical_strength": indicators.get("technical_strength"),
    "direction_method": "technical_only_v1"
            }
        })
        
    average, average_count = calculate_brti_average(
        ticks
    )

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

        "brti_candles": candles,
        "coinbase_candles": coin_candles,

        "kalshi_market_target": market_target,
        "kalshi_market_ticker": market_ticker,
        "kalshi_market_close": market_close,
        "kalshi_market_error": market_error,

        "countdown": countdown,

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

        "coinbase_candles": len(data["coinbase_candles"]),

        "database_status": pattern_memory.database_status(),
        "database_counts": pattern_memory.get_database_counts(),
        "prediction_accuracy": pattern_memory.get_prediction_accuracy()

    })

@app.route("/stats")
def stats():

    start_services()

    return jsonify({

        "database_status":
            pattern_memory.database_status(),

        "database_counts":
            pattern_memory.get_database_counts(),

        "prediction_accuracy":
            pattern_memory.get_prediction_accuracy()

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
    grid-template-columns:repeat(3,minmax(0,1fr));
    gap:8px;
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


/* IDENTIDAD VISUAL DE LAS TRES TARJETAS */

.coinbase-card{
    border:1px solid #278cff;
    background:linear-gradient(145deg,#10243c,#111827 75%);
    box-shadow:0 0 14px rgba(39,140,255,.13);
}

.kalshi-card{
    border:1px solid #00d995;
    background:linear-gradient(145deg,#103329,#111827 75%);
    box-shadow:0 0 14px rgba(0,217,149,.13);
}

.forecast-card{
    border:1px solid #bd75ff;
    background:linear-gradient(145deg,#29183f,#111827 75%);
    box-shadow:0 0 14px rgba(189,117,255,.16);
}

.brand-mark{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    min-width:25px;
    height:25px;
    padding:0 7px;
    border-radius:7px;
    font-size:12px;
    font-weight:900;
    margin-bottom:9px;
}

.coinbase-card .brand-mark{
    color:#55aaff;
    background:#12365c;
}

.kalshi-card .brand-mark{
    color:#00e6a0;
    background:#104b38;
}

.forecast-card .brand-mark{
    color:#d4a2ff;
    background:#43245e;
}

.coinbase-card .label{
    color:#69b7ff;
}

.kalshi-card .label{
    color:#49e9b1;
}

.forecast-card .label{
    color:#d4a2ff;
}

.coinbase-card .price{
    color:#70bdff;
}

.kalshi-card .price{
    color:#4af0b3;
}

.forecast-card .price{
    color:#d5a5ff;
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

/* Mantener oculto el panel técnico antiguo */
.technical-card {
    display: none !important;
}


/* TRES TARJETAS GRANDES EN UNA FILA */

@media(max-width:600px){

    .grid{
        grid-template-columns:repeat(3,minmax(0,1fr));
        gap:6px;
    }

    .grid .card{
        padding:11px 7px;
        margin-bottom:8px;
        border-radius:12px;
    }

    .grid .brand-mark{
        min-width:23px;
        height:23px;
        font-size:12px;
        margin-bottom:8px;
    }

    .grid .label{
        font-size:9px;
        letter-spacing:0;
        line-height:1.35;
    }

    .grid .price{
        font-size:clamp(12px,3.8vw,18px);
        font-weight:950;
        margin:9px 0 6px;
        overflow-wrap:anywhere;
    }

    .grid .small{
        font-size:9px;
        line-height:1.35;
    }
}


/* ANIMACIÓN ALCISTA */

.signal.green {
    animation: signalPulseGreen 1.5s ease-in-out infinite;
}

@keyframes signalPulseGreen {
    0%, 100% {
        text-shadow: 0 0 4px rgba(0,230,154,.25);
    }

    50% {
        text-shadow: 0 0 18px rgba(0,230,154,.9);
    }
}

/* ANIMACIÓN BAJISTA */

.signal.red {
    animation: signalPulseRed 1.5s ease-in-out infinite;
}

@keyframes signalPulseRed {
    0%, 100% {
        text-shadow: 0 0 4px rgba(255,83,107,.25);
    }

    50% {
        text-shadow: 0 0 18px rgba(255,83,107,.9);
    }
}

</style>
</head>


<body>

<main>

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


<div class="grid">

    
<div class="grid">

    <section class="card coinbase-card">

        <div class="brand-mark">CB</div>

        <div class="label">
            PRECIO ACTUAL · COINBASE
        </div>

        <div class="price" id="brtiPrice">--</div>

        <div class="small" id="brtiAge">
            Esperando índice
        </div>

    </section>

    <section class="card target-card kalshi-card">

        <div class="brand-mark">K</div>

        <div class="label">
            PRECIO OBJETIVO · KALSHI
        </div>

        <div class="price" id="kalshiTarget">--</div>

        <div class="small" id="kalshiTargetLabel">
            Esperando contrato activo
        </div>

    </section>

    <section class="card forecast-card">

        <div class="brand-mark">15M</div>

        <div class="label">
            PREDICCIÓN · CIERRE 15 MIN
        </div>

        <div class="price predicted" id="predictedPrice">--</div>

        <div class="small" id="predictionLabel">
            Esperando objetivo de Kalshi
        </div>

    </section>

</div>


<section class="card">

    <div class="section-title">
        PROBABILIDAD DEL MOTOR · 15 MIN
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


<section class="card">

    <div class="row">

        <div>

            <div class="section-title">
                GRÁFICA BTC/USD
            </div>

            <div class="small">
                COINBASE · Velas de 1 minuto
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


<!-- CONTINÚA LA GRÁFICA Y LOS INDICADORES -->

<section class="card technical-card">

    <div class="section-title">
        INDICADORES TÉCNICOS
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


<section class="card">

    <div class="row">

        <div>

            <div class="label">
                CIERRE DEL BLOQUE 15 MIN
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


function money(value){

    if(
        value === null ||
        value === undefined ||
        !Number.isFinite(Number(value))
    ){
        return "--";
    }

    return Number(value).toLocaleString(
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


function drawChart(data){

    if(Array.isArray(data)){
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
            "ESPERANDO VELAS COINBASE",
            W/2,
            H/2
        );

        return;
    }

    const count = Math.max(
        10,
        Math.min(
            100,
            Math.floor(65 / chartScale)
        )
    );

    const center = Math.floor(count * 0.72);

    const left = 12;
    const right = W - 66;
    const top = 20;
    const bottom = H - 20;

    const plotHeight = bottom - top;
    const plotWidth = right - left;

    const step = plotWidth / count;

    const candleWidth = Math.max(
        3,
        Math.min(14, step * 0.65)
    );

    const lastIndex = candles.length - 1;

    chartOffset = Math.max(
        0,
        Math.min(
            chartOffset,
            Math.max(0, candles.length - 1)
        )
    );

    function candleX(index){

        const distanceFromLast = lastIndex - index;

        return left + (
            center
            - distanceFromLast
            + 0.5
            + chartOffset
        ) * step;
    }

    const closes = candles.map(
        c => Number(c.close)
    );

    const e9 = emaSeries(closes,9);
    const e21 = emaSeries(closes,21);

    const visible = [];

    for(let i=0;i<candles.length;i++){

        const x = candleX(i);

        if(
            x >= left - candleWidth
            && x <= right + candleWidth
        ){

            visible.push({
                index:i,
                candle:candles[i],
                x:x
            });
        }
    }

    if(!visible.length) return;

    const values = [];

    visible.forEach(item=>{

        const c = item.candle;

        values.push(
            Number(c.high),
            Number(c.low)
        );

        if(e9[item.index] != null){
            values.push(e9[item.index]);
        }

        if(e21[item.index] != null){
            values.push(e21[item.index]);
        }

    });
        let min = Math.min(...values);
    let max = Math.max(...values);

    if(!Number.isFinite(min) || !Number.isFinite(max)){
        return;
    }

    if(max === min){
        max += 1;
        min -= 1;
    }

    const padding = Math.max(
        (max-min)*0.13,
        2
    );

    min -= padding;
    max += padding;

    const y = price =>
        bottom -
        ((price-min)/(max-min))*plotHeight;


    // CUADRÍCULA Y PRECIOS

    ctx.font="10px Arial";
    ctx.textAlign="left";
    ctx.strokeStyle="#263247";
    ctx.lineWidth=1;

    for(let i=0;i<=4;i++){

        const yy = top + plotHeight*i/4;

        ctx.beginPath();
        ctx.moveTo(left,yy);
        ctx.lineTo(right,yy);
        ctx.stroke();

        const price =
            max-(max-min)*i/4;

        ctx.fillStyle="#8999b5";

        ctx.fillText(
            price.toFixed(0),
            right+7,
            yy+3
        );
    }


    // VELAS COINBASE

    visible.forEach(item=>{

        const c = item.candle;
        const x = item.x;

        const open = Number(c.open);
        const close = Number(c.close);
        const high = Number(c.high);
        const low = Number(c.low);

        if(
            !Number.isFinite(open)
            || !Number.isFinite(close)
            || !Number.isFinite(high)
            || !Number.isFinite(low)
        ){
            return;
        }

        const up = close >= open;

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

        if(!series.length) return;

        ctx.beginPath();

        ctx.strokeStyle=color;
        ctx.lineWidth=1.8;
        ctx.lineJoin="round";
        ctx.lineCap="round";

        let startedLine = false;

        visible.forEach(item=>{

            const value = series[item.index];

            if(!Number.isFinite(value)) return;

            const x = item.x;
            const yy = y(value);

            if(!startedLine){

                ctx.moveTo(x,yy);
                startedLine = true;

            }else{

                ctx.lineTo(x,yy);

            }

        });

        if(startedLine){
            ctx.stroke();
        }
    }

    drawLine(e9,"#b98aff");
    drawLine(e21,"#45a9ff");


    // PRECIO DE LA ÚLTIMA VELA COINBASE

    if(chartOffset < 0.05){

        const last = candles[lastIndex];
        const lastPrice = Number(last.close);
        const lastY = y(lastPrice);

        ctx.setLineDash([5,4]);

        ctx.strokeStyle="#f4c95d";
        ctx.lineWidth=1;

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

    chartOffset = 0;
    chartScale = 1;

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

const canvas = $("chart");


canvas.addEventListener(
    "pointerdown",
    event=>{

        event.preventDefault();

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

        event.preventDefault();

        const oldPoint = pointers.get(event.pointerId);

        const deltaX = event.clientX - oldPoint.x;

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
                    0.5,
                    Math.min(
                        8,
                        initialZoom*distance/initialDistance
                    )
                );

                drawChart();

            }

        }

        else if(pointers.size===1){

            const count=Math.max(
                10,
                Math.min(
                    100,
                    Math.floor(65/chartScale)
                )
            );

            const plotWidth = Math.max(
                canvas.clientWidth-78,
                1
            );

            const step=plotWidth/count;

            chartOffset += deltaX/Math.max(step,1);

            chartOffset=Math.max(
                0,
                Math.min(
                    chartOffset,
                    Math.max(0,candles.length-1)
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

    if(pointers.size===1){

        const remaining=[...pointers.values()][0];

        previousX=remaining.x;

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

canvas.addEventListener(
    "lostpointercapture",
    releasePointer
);


// ==========================================
// ACTUALIZAR INFORMACIÓN DEL RADAR
// ==========================================

function updateRadar(d){

    $("brtiPrice").textContent =
        money(d.coinbase_price);

    $("predictedPrice").textContent =
        money(d.predicted_price);

        $("predictionLabel").textContent =
        "Estimación del cierre del bloque de 15 minutos";

    $("kalshiTarget").textContent =
        d.kalshi_market_target == null
        ? "--"
        : money(d.kalshi_market_target);

    $("kalshiTargetLabel").textContent =
        d.kalshi_market_ticker
        ? "Contrato activo: " + d.kalshi_market_ticker
        : (
            d.kalshi_market_error
            || "Esperando contrato activo de Kalshi"
        );

     $("brtiAge").textContent=
        d.coinbase_age==null
        ? "Esperando Coinbase"
        : "Coinbase actualizado hace "+d.coinbase_age+" s";


    const direction=d.direction || "RECOPILANDO";

    $("direction").textContent=direction;

    $("direction").className="signal "+
        (
            direction==="ALCISTA" ? "green":
            direction==="BAJISTA" ? "red":
            "yellow"
        );


    const bullish=Math.max(
        20,
        Math.min(80,Number(d.bullish_score ?? 50))
    );

    const bearish=100-bullish;

    $("bullText").textContent=
        bullish.toFixed(1)+"%";

    $("bearText").textContent=
        bearish.toFixed(1)+"%";

    $("bullBar").style.width=
        bullish+"%";

    $("bearBar").style.width=
        bearish+"%";


    $("strength").textContent=
        Number(d.technical_strength ?? 0)+"%";

    $("signalStage").textContent=
        d.signal_stage || (
            direction==="RECOPILANDO"
            ? "Esperando movimiento BRTI"
            : "Lectura técnica en vivo"
        );


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


    $("averagePrice").textContent=
        money(d.brti_average_60s);

    $("averageStatus").textContent=
        "Muestras recibidas en 60 segundos: "+
        (d.brti_average_samples || 0)+
        " · Cálculo local";

    $("brtiSamples").textContent=
        d.brti_samples || 0;


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


    $("countdown").textContent=
        clock(Number(d.countdown || 0));


    $("errors").textContent=
        [
            d.kalshi_error,
            d.coinbase_error
        ]
        .filter(Boolean)
        .join(" | ");


    drawChart(d.coinbase_candles || []);

    $("candleCount").textContent=
        (d.coinbase_candles || []).length+
        " velas Coinbase";

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
