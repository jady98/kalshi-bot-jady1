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
market_resolution_queue = deque(maxlen=20)

started = False
last_pattern_snapshot_minute = None
_last_brti_candle_save = 0.0

# Alertas informativas: no ejecutan cierres ni órdenes.
_alert_state = {
    "ticker": None,
    "previous_direction": None,
    "previous_bullish": None,
    "previous_coinbase_live": None,
    "previous_brti_live": None,
    "peak_directional_probability": None,
    "last_alert_ts": 0.0,
    "last_alert_key": None,
    "alert": {
        "type": "INFO",
        "level": "INFO",
        "message": "Sin alertas nuevas",
        "detail": "El radar vigila cambios de dirección, debilitamiento y divergencia entre fuentes.",
        "timestamp": None
    }
}

# Estado temporal para evitar cambios de dirección por ruido de un solo tick.
_signal_state = {
    "smoothed": None,
    "last_ts": 0.0,
    "last_remaining": None,
    "direction": "INDECISO",
    "candidate": None,
    "candidate_since": 0.0
}

_pattern_stats_cache = {}
_guidance_state = {
    "ticker": None,
    "stage": "SIN_SEÑAL",
    "direction": None,
    "last_alert_ts": 0.0,
    "alert": None
}

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

    global brti_price, brti_time, _last_brti_candle_save

    if not valid_price(price):
        return

    price = float(price)
    timestamp = normalize_timestamp(timestamp)

    completed_candle = None
    current_snapshot = None
    bucket = int(timestamp // 60) * 60
    monotonic_now = time.monotonic()

    with lock:
        brti_price = price
        brti_time = timestamp

        previous = next(
            (dict(row) for row in reversed(brti_candles)
             if int(row.get("time", -1)) < bucket),
            None
        )
        brti_ticks.append((timestamp, price))
        update_candle(brti_candles, price, timestamp)

        if previous is not None and int(previous["time"]) == bucket - 60:
            completed_candle = previous

        # Guarda también la vela en formación cada 10 segundos.
        # Así un reinicio no borra toda la vela del minuto actual.
        if monotonic_now - _last_brti_candle_save >= 10:
            current_snapshot = dict(brti_candles[-1])
            _last_brti_candle_save = monotonic_now

    if completed_candle is not None:
        pattern_memory.save_brti_candle(completed_candle)

    if current_snapshot is not None:
        pattern_memory.save_brti_candle(current_snapshot)

    pattern_memory.record_price(price, timestamp)


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

def calculate_brti_window_average(ticks, start_ts, end_ts):
    """Average within each second first, so faster message bursts cannot overweight a second."""
    per_second = {}
    for timestamp, price in ticks:
        try:
            timestamp = float(timestamp)
            price = float(price)
        except (TypeError, ValueError):
            continue
        if start_ts <= timestamp < end_ts and valid_price(price):
            per_second.setdefault(int(timestamp), []).append(price)

    if not per_second:
        return None, 0
    averages = [sum(values) / len(values) for values in per_second.values()]
    return sum(averages) / len(averages), len(averages)


def calculate_brti_average(ticks):
    now = time.time()
    return calculate_brti_window_average(ticks, now - 60, now + 0.001)


def get_cached_pattern_stats(pattern):
    """Consulta el historial de patrones como máximo una vez por minuto."""
    if not pattern or pattern == "SIN PATRÓN CLARO":
        return {"samples": 0, "bullish_pct": None, "bearish_pct": None}
    now = time.monotonic()
    with lock:
        cached = _pattern_stats_cache.get(pattern)
        if cached and now - cached[0] < 60:
            return dict(cached[1])
    try:
        stats = pattern_memory.get_pattern_stats(pattern)
        if not isinstance(stats, dict):
            stats = {"samples": 0, "bullish_pct": None, "bearish_pct": None}
    except Exception:
        stats = {"samples": 0, "bullish_pct": None, "bearish_pct": None}
    with lock:
        _pattern_stats_cache[pattern] = (now, dict(stats))
    return stats


# ==========================================
# ANÁLISIS DE MOVIMIENTO CON VELAS COINBASE
# ==========================================
def stabilize_direction(normalized, seconds_to_close):
    """
    Suaviza la puntuación y exige persistencia antes de cambiar de dirección.
    Usa tiempo monotónico para que los intervalos no dependan del reloj del sistema.
    """
    now = time.monotonic()

    with lock:
        previous_ts = _signal_state["last_ts"]
        previous_remaining = _signal_state["last_remaining"]

        new_contract = (
            previous_remaining is not None
            and seconds_to_close > previous_remaining + 30
        )

        if previous_ts == 0 or new_contract:
            smoothed = float(normalized)
            _signal_state["direction"] = "INDECISO"
            _signal_state["candidate"] = None
            _signal_state["candidate_since"] = 0.0
        else:
            elapsed = max(0.1, min(10.0, now - previous_ts))
            alpha = 1.0 - math.exp(-elapsed / 7.0)
            previous = _signal_state["smoothed"]
            smoothed = (
                float(normalized) if previous is None
                else previous + alpha * (float(normalized) - previous)
            )

        current = _signal_state["direction"]

        if current == "ALCISTA":
            if smoothed <= -0.20:
                desired = "BAJISTA"
            elif smoothed <= 0.025:
                desired = "INDECISO"
            else:
                desired = "ALCISTA"
        elif current == "BAJISTA":
            if smoothed >= 0.20:
                desired = "ALCISTA"
            elif smoothed >= -0.025:
                desired = "INDECISO"
            else:
                desired = "BAJISTA"
        else:
            if smoothed >= 0.14:
                desired = "ALCISTA"
            elif smoothed <= -0.14:
                desired = "BAJISTA"
            else:
                desired = "INDECISO"

        if desired == current:
            _signal_state["candidate"] = None
            _signal_state["candidate_since"] = 0.0
        else:
            if _signal_state["candidate"] != desired:
                _signal_state["candidate"] = desired
                _signal_state["candidate_since"] = now

            required_seconds = 15.0 if desired == "INDECISO" else 6.0

            if now - _signal_state["candidate_since"] >= required_seconds:
                _signal_state["direction"] = desired
                _signal_state["candidate"] = None
                _signal_state["candidate_since"] = 0.0

        _signal_state["smoothed"] = smoothed
        _signal_state["last_ts"] = now
        _signal_state["last_remaining"] = seconds_to_close

        return smoothed, _signal_state["direction"]



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
    # PREPARAR HISTORIAL DE VELAS BRTI
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
        # Arranque rápido: mientras se forman tres velas de 1 minuto,
        # calcula una señal provisional con los ticks BRTI recientes.
        now = time.time()
        recent_ticks = []
        for item in (ticks or []):
            try:
                ts, tick_price = float(item[0]), float(item[1])
                if now - 60 <= ts <= now + 2 and valid_price(tick_price):
                    recent_ticks.append((ts, tick_price))
            except (TypeError, ValueError, IndexError):
                continue
        recent_ticks.sort(key=lambda item: item[0])

        if len(recent_ticks) < 3:
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
                "pattern": "ESPERANDO TICKS BRTI",
                "target_influence": "PENDIENTE"
            }

        first_ts, first_price = recent_ticks[0]
        last_ts, last_price = recent_ticks[-1]
        cutoff_15 = last_ts - 15
        older = [item for item in recent_ticks if item[0] <= cutoff_15]
        reference_15 = older[-1][1] if older else first_price
        change_60 = ((last_price / first_price) - 1) * 100
        change_15 = ((last_price / reference_15) - 1) * 100
        momentum = change_15 if older else change_60

        elapsed_minutes = max(0.25, (last_ts - first_ts) / 60.0)
        rate_per_minute = change_60 / elapsed_minutes if elapsed_minutes > 0 else 0.0
        remaining_minutes = max(0.0, min(15.0, float(seconds_to_close or 0) / 60.0))
        projected_change = max(-0.30, min(0.30, rate_per_minute * remaining_minutes * 0.20))
        predicted = float(price) * (1 + projected_change / 100)

        target_gap_pct = None
        target_direction = "SIN OBJETIVO"
        target_influence = "SIN OBJETIVO"
        normalized = max(-1.0, min(1.0, momentum / 0.05)) * 0.55

        if valid_price(market_target):
            target_gap_pct = ((predicted - float(market_target)) / float(market_target)) * 100
            target_normalized = max(-1.0, min(1.0, target_gap_pct / 0.05))
            normalized = (
                math.copysign(0.70 * abs(target_normalized) + 0.30 * abs(normalized), target_gap_pct)
                if abs(target_gap_pct) > 0.005 else normalized * 0.30
            )
            target_direction = (
                "ALCISTA" if target_gap_pct > 0.005
                else "BAJISTA" if target_gap_pct < -0.005
                else "CERCA DEL OBJETIVO"
            )
            target_influence = (
                "PROYECCIÓN SOBRE OBJETIVO" if target_gap_pct > 0.005
                else "PROYECCIÓN BAJO OBJETIVO" if target_gap_pct < -0.005
                else "CERCA DEL OBJETIVO"
            )

        normalized, direction = stabilize_direction(normalized, seconds_to_close)
        bullish = max(20, min(80, round(50 + normalized * 30, 1)))
        bearish = round(100 - bullish, 1)
        if direction == "ALCISTA" and bullish < 50:
            direction = "INDECISO"
        elif direction == "BAJISTA" and bearish < 50:
            direction = "INDECISO"

        return {
            "direction": direction,
            "predicted_price": round(predicted, 2),
            "bullish_score": bullish,
            "bearish_score": bearish,
            "technical_strength": round(abs(normalized) * 100),
            "momentum_pct": round(momentum, 5),
            "ema9": None,
            "ema21": None,
            "rsi": None,
            "pattern": "SEÑAL INICIAL BRTI · TICKS",
            "target_influence": target_influence,
            "target_gap_pct": round(target_gap_pct, 5) if target_gap_pct is not None else None,
            "target_direction": target_direction,
            "technical_score": round(normalized, 4),
            "direction_method": "BRTI_TICKS_BOOTSTRAP_15M"
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
    # ESTRUCTURA Y PATRONES DE VARIAS VELAS
    # --------------------------------------

    # Ruptura de rango: exige cierre fuera del máximo/mínimo de las
    # cinco velas previas, no solo una mecha intravela.
    if len(valid_candles) >= 6:
        prior = valid_candles[-6:-1]
        prior_high = max(c["high"] for c in prior)
        prior_low = min(c["low"] for c in prior)
        if last["close"] > prior_high and previous["close"] <= prior_high:
            pattern = "RUPTURA ALCISTA DE RANGO"
        elif last["close"] < prior_low and previous["close"] >= prior_low:
            pattern = "RUPTURA BAJISTA DE RANGO"

    # Estructura de mercado: máximos y mínimos crecientes/decrecientes.
    if len(valid_candles) >= 3:
        x, y, z = valid_candles[-3:]
        if (
            x["high"] < y["high"] < z["high"]
            and x["low"] < y["low"] < z["low"]
        ):
            pattern = "ESTRUCTURA ALCISTA · HH/HL"
        elif (
            x["high"] > y["high"] > z["high"]
            and x["low"] > y["low"] > z["low"]
        ):
            pattern = "ESTRUCTURA BAJISTA · LH/LL"

    # Una vela dentro del rango anterior suele representar compresión.
    if (
        last["high"] <= previous["high"]
        and last["low"] >= previous["low"]
    ):
        pattern = "VELA INTERNA · COMPRESIÓN"

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
        "TRES VELAS ALCISTAS",
        "RUPTURA ALCISTA DE RANGO",
        "ESTRUCTURA ALCISTA · HH/HL"
    ):
        score += 2

    elif pattern in (
        "ENVOLVENTE BAJISTA",
        "TRES VELAS BAJISTAS",
        "RUPTURA BAJISTA DE RANGO",
        "ESTRUCTURA BAJISTA · LH/LL"
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

    # Proyección prudente: combina el ritmo de 5 y 15 velas,
    # reduce su influencia y limita el movimiento estimado.
    rates = []

    if len(closes) >= 6 and closes[-6] > 0:
        change_5m = ((closes[-1] / closes[-6]) - 1) * 100
        rates.append(change_5m / 5)

    if len(closes) >= 16 and closes[-16] > 0:
        change_15m = ((closes[-1] / closes[-16]) - 1) * 100
        rates.append(change_15m / 15)

    if rates:
        if len(rates) == 2:
            rate_per_minute = rates[0] * 0.65 + rates[1] * 0.35
        else:
            rate_per_minute = rates[0]
        projected_change = rate_per_minute * remaining_minutes * 0.25
    else:
        rate_per_minute = 0.0
        projected_change = 0.0

    projected_change = max(-0.30, min(0.30, projected_change))

    # Kalshi compara promedios BRTI de 60 segundos, no el último tick.
    # Proyecta el precio al cierre y corrige al punto medio de la ventana final.
    projected_close_price = float(price) * (1 + projected_change / 100)
    effective_rate = rate_per_minute * 0.25
    closing_window_fraction = min(1.0, remaining / 60.0)
    average_adjustment_pct = effective_rate * closing_window_fraction * 0.5
    predicted = projected_close_price / (1 + average_adjustment_pct / 100)

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
    # SEÑAL FINAL: TÉCNICA + OBJETIVO KALSHI
    # --------------------------------------

    technical_normalized = (
        score / possible
        if possible > 0 else 0
    )

    normalized = technical_normalized
    target_gap_pct = None
    target_direction = "SIN OBJETIVO"

    if valid_price(market_target):
        target_gap_pct = (
            (predicted - float(market_target))
            / float(market_target)
        ) * 100

        # El precio proyectado respecto al strike determina el lado
        # probable del contrato. La técnica también se calcula con velas BRTI.
        target_normalized = max(
            -1.0,
            min(1.0, target_gap_pct / 0.05)
        )

        if abs(target_gap_pct) > 0.005:
            # BRTI determina el lado frente al strike; Coinbase solo confirma.
            normalized = math.copysign(
                0.70 * abs(target_normalized)
                + 0.30 * abs(technical_normalized),
                target_gap_pct
            )
        else:
            # Si la proyección está prácticamente sobre el strike,
            # no se fuerza un lado: la técnica puede dejarlo indeciso.
            normalized = 0.30 * technical_normalized

        tolerance_pct = 0.005
        if target_gap_pct > tolerance_pct:
            target_direction = "ALCISTA"
        elif target_gap_pct < -tolerance_pct:
            target_direction = "BAJISTA"
        else:
            target_direction = "CERCA DEL OBJETIVO"

    pattern_stats = get_cached_pattern_stats(pattern)
    pattern_samples = int(pattern_stats.get("samples") or 0)
    historical_bullish_pct = pattern_stats.get("bullish_pct")
    historical_bearish_pct = pattern_stats.get("bearish_pct")
    # El historial solo pesa 15% y requiere al menos 20 resultados reales.
    pattern_learning_used = bool(
        pattern_samples >= 20
        and historical_bullish_pct is not None
        and historical_bearish_pct is not None
    )
    if pattern_learning_used:
        historical_bias = max(
            -1.0,
            min(1.0, (float(historical_bullish_pct) - float(historical_bearish_pct)) / 100.0)
        )
        normalized = (normalized * 0.85) + (historical_bias * 0.15)

    # El aprendizaje histórico puede modular la confianza, pero no invertir
    # el lado del contrato cuando la proyección ya está claramente sobre/bajo
    # el objetivo vigente de Kalshi.
    target_side_locked = bool(
        valid_price(market_target)
        and target_gap_pct is not None
        and abs(target_gap_pct) > 0.005
    )
    if target_side_locked:
        normalized = math.copysign(abs(normalized), target_gap_pct)

    normalized, direction = stabilize_direction(
        normalized,
        remaining
    )

    bullish = max(
        20,
        min(80, round(50 + normalized * 30, 1))
    )

    bearish = round(100 - bullish, 1)

    # No mostrar una dirección que contradiga la probabilidad calculada.
    # Durante la espera de confirmación de un giro, mostrar INDECISO.
    display_direction = direction
    if direction == "ALCISTA" and bullish < 50:
        display_direction = "INDECISO"
    elif direction == "BAJISTA" and bearish < 50:
        display_direction = "INDECISO"

    # --------------------------------------
    # RESULTADO
    # --------------------------------------

    return {
        "direction": display_direction,
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
        "target_influence": target_influence,
        "target_gap_pct": (
            round(target_gap_pct, 5)
            if target_gap_pct is not None else None
        ),
        "target_direction": target_direction,
        "technical_score": round(technical_normalized, 4),
        "pattern_samples": pattern_samples,
        "pattern_historical_bullish_pct": historical_bullish_pct,
        "pattern_historical_bearish_pct": historical_bearish_pct,
        "pattern_learning_used": pattern_learning_used,
        "target_side_locked": target_side_locked,
        "contract_yes_probability": bullish if valid_price(market_target) else None,
        "contract_no_probability": bearish if valid_price(market_target) else None,
        "direction_method": (
            "BRTI_PRIMARY_KALSHI_15M"
            if valid_price(market_target)
            else "BRTI_PRIMARY_NO_TARGET"
        )
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
                if not close_value:
                    # Sin cierre oficial no se puede emitir una señal de entrada.
                    continue
                close_ts = normalize_timestamp(close_value)
                open_value = market.get("open_time")
                open_ts = normalize_timestamp(open_value) if open_value else None
                ticker_value = str(market.get("ticker", "")).strip()

                # Solo contratos realmente vigentes: nunca aceptar uno vencido
                # ni uno cuyo horario de apertura todavía no haya llegado.
                if (
                    ticker_value
                    and close_ts > now + 1
                    and (open_ts is None or open_ts <= now + 1)
                ):
                    found.append((close_ts, ticker_value, target))

            if found:
                found.sort(key=lambda item: item[0])
                close_ts, ticker, target = found[0]

                with lock:
                    if (
                        kalshi_market_ticker
                        and kalshi_market_ticker != ticker
                        and kalshi_market_close is not None
                    ):
                        market_resolution_queue.append((
                            kalshi_market_ticker,
                            kalshi_market_close
                        ))
                    kalshi_market_target = target
                    kalshi_market_ticker = ticker
                    kalshi_market_close = close_ts
                    kalshi_market_error = ""
            else:
                with lock:
                    # No conservar un strike/ticker antiguo como si siguiera activo.
                    if kalshi_market_ticker and kalshi_market_close is not None:
                        market_resolution_queue.append((
                            kalshi_market_ticker,
                            kalshi_market_close
                        ))
                    kalshi_market_target = None
                    kalshi_market_ticker = ""
                    kalshi_market_close = None
                    kalshi_market_error = "No hay contrato KXBTC15M activo con objetivo y cierre válidos."

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

            # Recupera velas BRTI reales guardadas en PostgreSQL.
            # No utiliza Coinbase ni crea velas artificiales.
            restored = pattern_memory.get_brti_candles(MAX_CANDLES)
            if restored:
                with lock:
                    # Las velas BRTI en memoria tienen prioridad si llegaron
                    # mientras se recuperaba PostgreSQL.
                    merged = {
                        int(row["time"]): dict(row)
                        for row in restored
                    }
                    for row in brti_candles:
                        merged[int(row["time"])] = dict(row)
                    brti_candles.clear()
                    brti_candles.extend(
                        sorted(
                            merged.values(),
                            key=lambda row: int(row["time"])
                        )[-MAX_CANDLES:]
                    )
                print(
                    "BRTI: HISTORIAL RESTAURADO",
                    len(restored),
                    "velas",
                    flush=True
                )
            else:
                print(
                    "BRTI: SIN HISTORIAL PERSISTIDO; "
                    "se acumulará desde ahora",
                    flush=True
                )


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
# ALERTAS DE CAMBIOS Y CONFIRMACIÓN DE FUENTES
# ==========================================

def update_alert_state(direction, bullish, ticker, divergence_pct,
                       brti_live, coinbase_live):
    now = time.time()
    alert = None

    with lock:
        state = _alert_state

        if state["ticker"] != ticker:
            state["ticker"] = ticker
            state["previous_direction"] = None
            state["previous_bullish"] = None
            state["peak_directional_probability"] = None
            state["last_alert_key"] = None

        previous_direction = state["previous_direction"]
        previous_bullish = state["previous_bullish"]
        previous_coinbase_live = state["previous_coinbase_live"]
        previous_brti_live = state["previous_brti_live"]

        if previous_brti_live is True and not brti_live:
            alert = (
                "BRTI_DESCONECTADO",
                "CRITICA",
                "BRTI SIN DATOS",
                "Se perdió el índice de liquidación de Kalshi. La predicción queda suspendida hasta recuperar BRTI."
            )

        if alert is None and previous_coinbase_live is True and not coinbase_live:
            alert = (
                "COINBASE_DESCONECTADO",
                "AVISO",
                "COINBASE SIN DATOS",
                "Se perdió la fuente secundaria de confirmación. BRTI sigue siendo la referencia principal si continúa en vivo."
            )

        if alert is None and direction in ("ALCISTA", "BAJISTA"):
            if (
                previous_direction in ("ALCISTA", "BAJISTA")
                and previous_direction != direction
            ):
                alert = (
                    "CAMBIO_DIRECCION",
                    "CRITICA",
                    "CAMBIO DE DIRECCIÓN",
                    "La señal pasó de " + previous_direction
                    + " a " + direction
                    + ". Revisa el contrato y tu posición."
                )

            elif previous_direction == direction:
                current_side_probability = (
                    bullish if direction == "ALCISTA"
                    else 100 - bullish
                )
                peak = state["peak_directional_probability"]
                if peak is None or current_side_probability > peak:
                    state["peak_directional_probability"] = current_side_probability
                    peak = current_side_probability

                drop = peak - current_side_probability
                if drop >= 8:
                    alert = (
                        "DEBILITAMIENTO",
                        "AVISO",
                        "SEÑAL DEBILITÁNDOSE",
                        "La probabilidad a favor de " + direction
                        + " cayó " + str(round(drop, 1))
                        + " puntos desde su máximo reciente. Revisa si la señal sigue respaldada."
                    )

        if (
            alert is None
            and brti_live
            and coinbase_live
            and divergence_pct is not None
            and abs(divergence_pct) >= 0.05
        ):
            alert = (
                "DIVERGENCIA",
                "AVISO",
                "BRTI Y COINBASE DIVERGEN",
                "Las fuentes difieren " + str(round(divergence_pct, 3))
                + "%. BRTI es la referencia de liquidación; Coinbase es la confirmación secundaria."
            )

        if alert is not None:
            key = alert[0] + ":" + str(ticker)
            cooldown_ok = now - state["last_alert_ts"] >= 45
            if cooldown_ok or key != state["last_alert_key"]:
                state["alert"] = {
                    "type": alert[0],
                    "level": alert[1],
                    "message": alert[2],
                    "detail": alert[3],
                    "timestamp": now
                }
                state["last_alert_ts"] = now
                state["last_alert_key"] = key

        if direction in ("ALCISTA", "BAJISTA"):
            if direction != previous_direction:
                state["peak_directional_probability"] = (
                    bullish if direction == "ALCISTA"
                    else 100 - bullish
                )
            else:
                current_side_probability = (
                    bullish if direction == "ALCISTA"
                    else 100 - bullish
                )
                peak = state["peak_directional_probability"]
                state["peak_directional_probability"] = (
                    current_side_probability
                    if peak is None
                    else max(peak, current_side_probability)
                )

        state["previous_direction"] = (
            direction if direction in ("ALCISTA", "BAJISTA")
            else state["previous_direction"]
        )
        state["previous_bullish"] = bullish
        state["previous_coinbase_live"] = coinbase_live
        state["previous_brti_live"] = brti_live

        current_alert = dict(state["alert"])

    if (
        current_alert.get("timestamp") is not None
        and now - current_alert["timestamp"] > 120
    ):
        return {
            "type": "INFO",
            "level": "INFO",
            "message": "Sin alertas nuevas",
            "detail": "El radar sigue vigilando cambios de dirección, debilitamiento y divergencia entre fuentes.",
            "timestamp": None
        }

    return current_alert


# ==========================================
def _tick_change_over_seconds(ticks, seconds, now):
    """Cambio observado contra el último tick anterior al horizonte solicitado."""
    valid = []
    for item in ticks or []:
        try:
            ts, value = float(item[0]), float(item[1])
            if math.isfinite(ts) and valid_price(value) and ts <= now + 2:
                valid.append((ts, value))
        except (TypeError, ValueError, IndexError):
            continue
    if len(valid) < 2:
        return None
    valid.sort(key=lambda row: row[0])
    current_ts, current_price = valid[-1]
    cutoff = current_ts - float(seconds)
    reference = None
    for ts, value in valid:
        if ts <= cutoff:
            reference = (ts, value)
        else:
            break
    if reference is None:
        return None
    age = current_ts - reference[0]
    if age < seconds * 0.65 or age > seconds + 45:
        return None
    return ((current_price / reference[1]) - 1.0) * 100.0


def _candle_change_over_seconds(candles, price, seconds, now):
    """Respaldo con velas BRTI persistidas cuando el historial de ticks aún se está formando."""
    if not valid_price(price):
        return None
    candidates = []
    for candle in candles or []:
        try:
            close_ts = float(candle.get("_close_ts", candle.get("time", 0)))
            close_price = float(candle.get("close", 0))
            if (
                math.isfinite(close_ts)
                and valid_price(close_price)
                and close_ts <= now - float(seconds)
            ):
                candidates.append((close_ts, close_price))
        except (TypeError, ValueError, AttributeError):
            continue
    if not candidates:
        return None
    reference_ts, reference_price = max(candidates, key=lambda item: item[0])
    age = now - reference_ts
    # Velas de un minuto: tolerancia suficiente para cierres y pequeños retrasos.
    if age < float(seconds) * 0.65 or age > float(seconds) + 90:
        return None
    return ((float(price) / reference_price) - 1.0) * 100.0


def build_forward_guidance(
    ticks, price, indicators, candles=None,
    contract_active=False, ticker=None, close_ts=None
):
    """Proyección corta separada del pronóstico principal al cierre de 15 minutos."""
    now = time.time()
    seconds_left = max(0, int(float(close_ts) - now)) if close_ts else 0
    if not contract_active or not ticker or not close_ts or seconds_left <= 0:
        return {
            "short_change_1m_pct": None,
            "short_change_3m_pct": None,
            "short_predicted_price_1m": None,
            "short_predicted_price_3m": None,
            "short_direction": "INDECISO",
            "entry_stage": "SIN CONTRATO ACTIVO",
            "entry_direction": None,
            "entry_ticker": None,
            "entry_seconds_remaining": 0,
            "signal_stage": "SIN CONTRATO ACTIVO · análisis predictivo sin entrada"
        }
    change_1m = _tick_change_over_seconds(ticks, 60, now)
    change_3m = _tick_change_over_seconds(ticks, 180, now)
    # Tras un reinicio de Render, los ticks empiezan vacíos. Usar velas BRTI
    # ya recuperadas de PostgreSQL evita esperar a reconstruir todo el historial.
    if change_1m is None:
        change_1m = _candle_change_over_seconds(candles, price, 60, now)
    if change_3m is None:
        change_3m = _candle_change_over_seconds(candles, price, 180, now)
    forecast_1m = (
        float(price) * (1 + change_1m / 100)
        if valid_price(price) and change_1m is not None else None
    )
    if change_3m is not None:
        projected_3m_change = max(-0.18, min(0.18, change_3m))
    elif change_1m is not None:
        projected_3m_change = max(-0.18, min(0.18, change_1m * 2.2))
    else:
        projected_3m_change = None
    forecast_3m = (
        float(price) * (1 + projected_3m_change / 100)
        if valid_price(price) and projected_3m_change is not None else None
    )
    direction = indicators.get("direction", "INDECISO")
    bullish = float(indicators.get("bullish_score", 50) or 50)
    side_probability = (
        bullish if direction == "ALCISTA"
        else 100 - bullish if direction == "BAJISTA"
        else 50.0
    )
    def agrees(value):
        if value is None or direction not in ("ALCISTA", "BAJISTA"):
            return False
        return value > 0.001 if direction == "ALCISTA" else value < -0.001
    align_1m = agrees(change_1m)
    align_3m = agrees(change_3m)
    # No dar entrada si quedan menos de 60 segundos: no hay margen operativo
    # razonable y aumenta el riesgo de alertar justo al expirar el contrato.
    stage = "OBSERVAR"
    enough_time = seconds_left >= 60
    if enough_time and direction in ("ALCISTA", "BAJISTA"):
        if side_probability >= 56 and (align_1m or align_3m):
            stage = "PREALERTA"
        if side_probability >= 64 and align_1m and align_3m:
            stage = "ENTRADA"
    if not enough_time:
        stage = "CIERRE PRÓXIMO · SIN ENTRADA"
    elif change_1m is None and change_3m is None:
        stage = "SIN CONFIRMACIÓN CORTA"
    return {
        "short_change_1m_pct": round(change_1m, 5) if change_1m is not None else None,
        "short_change_3m_pct": round(change_3m, 5) if change_3m is not None else None,
        "short_predicted_price_1m": round(forecast_1m, 2) if forecast_1m is not None else None,
        "short_predicted_price_3m": round(forecast_3m, 2) if forecast_3m is not None else None,
        "short_direction": direction if stage in ("PREALERTA", "ENTRADA") else "INDECISO",
        "entry_stage": stage,
        "entry_direction": direction if stage in ("PREALERTA", "ENTRADA") else None,
        "entry_ticker": ticker,
        "entry_seconds_remaining": seconds_left,
        "signal_stage": (
            stage + " · " + direction + " · " + str(round(side_probability, 1)) + "%"
            if stage in ("PREALERTA", "ENTRADA")
            else stage + " · confirmación de 1-3 min"
        )
    }


def update_guidance_alert(guidance, ticker, contract_active=False):
    """Solo alerta entradas ligadas al contrato vigente; nunca ejecuta órdenes."""
    now = time.time()
    stage = guidance.get("entry_stage")
    direction = guidance.get("entry_direction")
    seconds_left = int(guidance.get("entry_seconds_remaining") or 0)
    alert = None
    with lock:
        state = _guidance_state
        if not contract_active or not ticker or seconds_left <= 0:
            state.update({
                "ticker": None,
                "stage": "SIN_CONTRATO",
                "direction": None,
                "last_alert_ts": 0.0,
                "alert": None
            })
            return None
        if state["ticker"] != ticker:
            state.update({
                "ticker": ticker,
                "stage": "SIN_SEÑAL",
                "direction": None,
                "last_alert_ts": 0.0,
                "alert": None
            })
        previous_stage = state["stage"]
        previous_direction = state["direction"]
        if previous_stage == "ENTRADA" and (
            stage != "ENTRADA" or direction != previous_direction
        ):
            alert = {
                "type": "PROTEGER",
                "level": "CRITICA",
                "message": "SEÑAL PERDIÓ CONFIRMACIÓN · VALORA SALIR",
                "detail": "La entrada anterior ya no mantiene la confirmación conjunta de 1 y 3 minutos. Revisa el contrato y el riesgo antes de mantener la posición.",
                "timestamp": now
            }
        elif stage == "ENTRADA" and (
            previous_stage != "ENTRADA" or direction != previous_direction
        ):
            alert = {
                "type": "ENTRADA",
                "level": "AVISO",
                "message": "ENTRADA CONFIRMADA · " + str(direction),
                "detail": "Contrato " + str(ticker) + ". Quedan " + str(seconds_left) + " segundos. Dirección principal y movimientos de 1 y 3 minutos alineados; verifica precio y riesgo antes de decidir.",
                "ticker": str(ticker),
                "seconds_remaining": seconds_left,
                "timestamp": now
            }
        elif stage == "PREALERTA" and previous_stage not in ("PREALERTA", "ENTRADA"):
            alert = {
                "type": "PREALERTA",
                "level": "AVISO",
                "message": "PREALERTA · POSIBLE " + str(direction),
                "detail": "Contrato " + str(ticker) + ". Quedan " + str(seconds_left) + " segundos. Se está formando una dirección; todavía no es una entrada confirmada.",
                "ticker": str(ticker),
                "seconds_remaining": seconds_left,
                "timestamp": now
            }
        state["stage"] = stage
        state["direction"] = direction
        # Cada transición se anuncia una sola vez; no se pierde una entrada
        # por el intervalo de enfriamiento de una prealerta anterior.
        if alert:
            state["last_alert_ts"] = now
            state["alert"] = alert

        # Mantener la alerta visible entre las consultas periódicas de la pantalla.
        # Así no desaparece después de una sola respuesta de la API.
        saved_alert = state.get("alert")
        if (
            saved_alert
            and saved_alert.get("ticker") == ticker
            and saved_alert.get("seconds_remaining", 0) > 0
            and saved_alert.get("timestamp") is not None
            and now - float(saved_alert["timestamp"]) <= 300
        ):
            return dict(saved_alert)
    return None


# DATOS PRINCIPALES
# ==========================================

def get_data():

    global market_resolution_queue

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

    contract_active = bool(
        market_ticker
        and valid_price(market_target)
        and market_close is not None
        and float(market_close) > now
    )
    if not contract_active:
        market_ticker = ""
        market_target = None
        market_close = None
    countdown = max(0, int(market_close - now)) if contract_active else 0

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

    if market_ticker and market_close is not None and now >= market_close:
        with lock:
            if not any(item[0] == market_ticker for item in market_resolution_queue):
                market_resolution_queue.append((market_ticker, market_close))

    for queued_ticker, queued_close in list(market_resolution_queue):
        if now < queued_close:
            continue
        closing_average, closing_samples = calculate_brti_window_average(
            ticks, queued_close - 60, queued_close
        )
        if closing_average is not None and closing_samples >= 45:
            saved = pattern_memory.record_market_close(
                queued_ticker, closing_average, now
            )
            if saved:
                with lock:
                    market_resolution_queue = deque(
                        (item for item in market_resolution_queue if item[0] != queued_ticker),
                        maxlen=20
                    )

    # FUENTE UNICA DE PREDICCION: BRTI.
    # Coinbase permanece conectado solo para diagnostico/comparacion.
    analysis_price = bp if brti_live else None
    signal_source = "BRTI" if brti_live else "SIN DATOS BRTI"

    analysis_candles = candles if brti_live else []
    candle_source = (
        "BRTI" if brti_live and len(candles) >= 3
        else "BRTI TICKS (VELAS EN FORMACIÓN)"
        if brti_live and len(ticks) >= 3
        else "SIN VELAS BRTI"
    )

    indicators = calculate_prediction(
        analysis_price,
        ticks,
        countdown,
        market_target if contract_active else None,
        analysis_candles
    )

    indicators["signal_source"] = signal_source
    indicators["candle_source"] = candle_source
    if not brti_live:
        indicators["direction_method"] = "BRTI_WAITING_FOR_DATA"
    global last_pattern_snapshot_minute

    snapshot_minute = int(now // 60)
    snapshot_slot = (
        str(snapshot_minute) + ":" +
        (market_ticker if contract_active else "GLOBAL")
    )
    should_save_snapshot = False

    # Registrar una predicción BRTI por minuto aunque Kalshi no tenga
    # contrato activo. El cierre del contrato se guarda solo como dato adicional.
    if bp is not None and brti_live and valid_price(bp):
        with lock:
            if last_pattern_snapshot_minute != snapshot_slot:
                last_pattern_snapshot_minute = snapshot_slot
                should_save_snapshot = True

        if should_save_snapshot:
            pattern_memory.save_snapshot({
                "signal_ts": now,
                "price": bp,
                "market_ticker": market_ticker if contract_active else None,
                "market_close_ts": market_close if contract_active else None,
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
                    "direction_method": indicators.get("direction_method"),
                    "signal_source": signal_source,
                    "candle_source": candle_source,
                    "market_target": market_target
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

    divergence_pct = (
        ((bp - cp) / cp) * 100
        if cp is not None and bp is not None and cp > 0
        else None
    )

    alert_data = update_alert_state(
        indicators.get("direction"),
        float(indicators.get("bullish_score", 50)),
        market_ticker if contract_active else None,
        divergence_pct,
        brti_live,
        coinbase_live
    )
    guidance = build_forward_guidance(
        ticks, bp if brti_live else None, indicators, candles,
        contract_active=contract_active,
        ticker=market_ticker if contract_active else None,
        close_ts=market_close if contract_active else None
    )
    guidance_alert = update_guidance_alert(
        guidance, market_ticker if contract_active else None,
        contract_active=contract_active
    )
    if guidance_alert is not None:
        alert_data = guidance_alert
    indicators.update(guidance)

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
        "divergence_pct": (
            round(divergence_pct, 5)
            if divergence_pct is not None else None
        ),
        "brti_confirmation": (
            "CONFIRMA" if brti_live and coinbase_live
            and divergence_pct is not None and abs(divergence_pct) < 0.05
            else "DIVERGENCIA" if brti_live and coinbase_live
            else "SIN CONFIRMACIÓN"
        ),
        "alert": alert_data,

        "brti_candles": candles,
        "coinbase_candles": coin_candles,

        "kalshi_market_active": contract_active,
        "kalshi_market_target": market_target,
        "kalshi_market_ticker": market_ticker if contract_active else None,
        "kalshi_market_close": market_close,
        "seconds_to_close": countdown,
        "kalshi_market_error": market_error,

        "countdown": countdown,

        "brti_average_60s": (
            round(average, 2)
            if average is not None else None
        ),

        "brti_average_samples": average_count,
        "brti_tick_count": len(ticks),
        "signal_source": signal_source,
        "candle_source": candle_source,

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
        "kalshi_market_active": data.get("kalshi_market_active", False),
        "kalshi_market_ticker": data.get("kalshi_market_ticker"),
        "kalshi_market_target": data.get("kalshi_market_target"),
        "seconds_to_close": data.get("seconds_to_close", 0),
        "entry_stage": data.get("entry_stage"),
        "entry_ticker": data.get("entry_ticker"),
        "pattern": data.get("pattern"),
        "pattern_samples": data.get("pattern_samples", 0),
        "pattern_learning_used": data.get("pattern_learning_used", False),

        "coinbase_live": data["coinbase_live"],
        "coinbase_price": data["coinbase_price"],
        "coinbase_connected": data["coinbase_connected"],

        "coinbase_candles": len(data["coinbase_candles"]),
        "brti_candles": len(data["brti_candles"]),
        "brti_ticks": data["brti_tick_count"],
        "candle_source": data["candle_source"],
        "direction_method": data["direction_method"],
        "pattern": data["pattern"],
        "pattern_samples": data.get("pattern_samples", 0),
        "pattern_learning_used": data.get("pattern_learning_used", False),
        "short_change_1m_pct": data.get("short_change_1m_pct"),
        "short_change_3m_pct": data.get("short_change_3m_pct"),
        "entry_stage": data.get("entry_stage"),
        "signal_source": data["signal_source"],
        "brti_average_60s": data["brti_average_60s"],
        "brti_average_samples": data["brti_average_samples"],
        "direction": data["direction"],
        "predicted_price": data["predicted_price"],
        "kalshi_market_target": data["kalshi_market_target"],
        "kalshi_market_ticker": data["kalshi_market_ticker"],

        "database_status": pattern_memory.database_status(),
        "database_counts": pattern_memory.get_database_counts(),
        "prediction_accuracy": pattern_memory.get_prediction_accuracy(),
        "prediction_accuracy_by_source": pattern_memory.get_prediction_accuracy_by_source(),
        "official_market_accuracy":
            pattern_memory.get_official_market_accuracy()

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
            pattern_memory.get_prediction_accuracy(),

         "prediction_accuracy_by_source":
            pattern_memory.get_prediction_accuracy_by_source(),

         "prediction_accuracy_15m":
            pattern_memory.get_15m_prediction_accuracy(),

         "prediction_accuracy_15m_by_source":
            pattern_memory.get_15m_prediction_accuracy_by_source(),

         "official_market_accuracy":
            pattern_memory.get_official_market_accuracy()

    })

@app.route("/history")
def history():
    start_services()
    return jsonify(
        pattern_memory.get_recent_predictions(50)
    )



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
    color:#f4c95d;
}

.forecast-card .price.predicted.bullish{
    color:#00e69a;
}

.forecast-card .price.predicted.bearish{
    color:#ff536b;
}

.forecast-card .price.predicted.uncertain{
    color:#f4c95d;
}


/* IDENTIDAD VISUAL DE LAS TRES TARJETAS */

.brti-card{
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

.brti-card .brand-mark{
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

.brti-card .label{
    color:#69b7ff;
}

.kalshi-card .label{
    color:#49e9b1;
}

.forecast-card .label{
    color:#d4a2ff;
}

.brti-card .price{
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
    min-height:180px;
    position:relative;
    overflow:hidden;
}

#chart{
    display:block;
    width:100%;
    height:100%;
    touch-action:pan-y;
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

.alert-stack {
    display: grid;
    gap: 12px;
    margin: 14px 0;
}
.alert-card {
    display: flex;
    align-items: center;
    gap: 14px;
    min-height: 104px;
    padding: 16px 18px;
    border: 2px solid #34425b;
    border-radius: 18px;
    background: rgba(8,12,28,.88);
    opacity: .38;
    filter: saturate(.55);
    transition: opacity .2s ease, box-shadow .2s ease, transform .2s ease;
}
.alert-card.active {
    opacity: 1;
    filter: none;
    transform: scale(1.01);
}
.alert-card .alert-icon {
    flex: 0 0 58px;
    width: 58px;
    height: 58px;
    display: grid;
    place-items: center;
    border-radius: 50%;
    font-size: 36px;
    font-weight: 1000;
}
.alert-card .alert-copy { min-width: 0; }
.alert-card .alert-title {
    font-size: clamp(21px, 5.5vw, 30px);
    font-weight: 1000;
    line-height: 1.05;
    letter-spacing: .2px;
    margin-bottom: 7px;
}
.alert-card .alert-detail {
    color: #f2f5ff;
    font-size: clamp(14px, 3.8vw, 18px);
    line-height: 1.3;
    font-weight: 650;
}
.alert-card.prealert { border-color: #ffd21f; }
.alert-card.prealert .alert-icon { color: #111; background: #ffd21f; }
.alert-card.prealert.active { box-shadow: 0 0 22px rgba(255,210,31,.28); }
.alert-card.entry { border-color: #25ed77; }
.alert-card.entry .alert-icon { color: #042112; background: #25ed77; }
.alert-card.entry.active { box-shadow: 0 0 22px rgba(37,237,119,.28); }
.alert-card.protect { border-color: #ff405b; }
.alert-card.protect .alert-icon { color: white; background: #ff405b; }
.alert-card.protect.active { box-shadow: 0 0 22px rgba(255,64,91,.32); }
@media (max-width: 420px) {
    .alert-card { min-height: 92px; padding: 13px 12px; gap: 11px; }
    .alert-card .alert-icon { flex-basis: 48px; width: 48px; height: 48px; font-size: 29px; }
}

/* AJUSTE MOVIL: radar completo al ancho del telefono */
@media (max-width: 600px) {
    html, body {
        width: 100%;
        max-width: 100%;
        overflow-x: hidden;
    }
    body {
        padding: 6px !important;
    }
    .container, main, .dashboard {
        width: 100%;
        max-width: 100%;
        min-width: 0;
    }
    .card {
        min-width: 0;
        padding: 9px !important;
        margin-bottom: 8px;
    }
    .chart-wrap {
        height: 190px !important;
    }
    .alert-stack {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 6px;
        width: 100%;
        margin: 8px 0;
        min-width: 0;
    }
    .alert-card {
        box-sizing: border-box;
        min-width: 0;
        min-height: 0;
        height: 148px;
        padding: 8px 5px;
        border-width: 1.5px;
        border-radius: 12px;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        align-items: center;
        gap: 6px;
        text-align: center;
        overflow: hidden;
    }
    .alert-card.active {
        transform: none;
    }
    .alert-card .alert-icon {
        flex: 0 0 30px;
        width: 30px;
        height: 30px;
        font-size: 20px;
    }
    .alert-card .alert-copy {
        width: 100%;
        min-width: 0;
    }
    .alert-card .alert-title {
        font-size: clamp(10px, 2.8vw, 14px);
        line-height: 1.12;
        letter-spacing: 0;
        overflow-wrap: anywhere;
        margin: 0 0 5px;
    }
    .alert-card .alert-detail {
        font-size: clamp(8px, 2.25vw, 11px);
        line-height: 1.2;
        overflow-wrap: anywhere;
    }
}
@media (max-width: 360px) {
    .alert-card { height: 142px; padding: 7px 3px; }
    .alert-card .alert-title { font-size: 10px; }
    .alert-card .alert-detail { font-size: 8px; }
    .chart-wrap { height: 165px !important; }
}


/* RADAR COMPLETO EN UNA SOLA PANTALLA MOVIL */
@media (max-width: 600px) {
    *, *::before, *::after { box-sizing: border-box; }
    html, body { width:100%; height:100%; min-height:0; margin:0; overflow:hidden !important; }
    body { padding:0 !important; }
    main {
        width:100%; max-width:none; height:100vh; height:100dvh; min-height:0;
        padding:5px; margin:0; display:grid;
        grid-template-rows:auto auto auto auto minmax(100px,1fr) auto;
        gap:5px; overflow:hidden;
    }
    .header { min-height:25px; margin:0; padding:0 3px; }
    .logo { font-size:clamp(16px,4.5vw,21px); }
    .status { font-size:8px; gap:3px; }
    .grid { min-height:0; margin:0; grid-template-columns:repeat(3,minmax(0,1fr)); gap:5px; }
    .grid .card { min-width:0; margin:0; padding:7px 4px !important; border-radius:10px; }
    .grid .brand-mark { display:none; }
    .grid .label { font-size:clamp(7px,2vw,9px); line-height:1.1; overflow-wrap:anywhere; }
    .grid .price { font-size:clamp(11px,3.5vw,16px); line-height:1.1; margin:6px 0 3px; overflow-wrap:anywhere; }
    .grid .small { font-size:7px; line-height:1.15; }
    main > section.card:nth-of-type(1) { min-height:0; margin:0; padding:7px 9px !important; }
    main > section.card:nth-of-type(1) .section-title { font-size:8px; margin:0 0 3px; }
    main > section.card:nth-of-type(1) .signal { font-size:clamp(16px,4vw,20px); margin:2px 0 4px; line-height:1.05; }
    main > section.card:nth-of-type(1) .price { font-size:clamp(13px,3.5vw,17px); margin:1px 0; }
    main > section.card:nth-of-type(1) .bar { height:7px; margin:4px 0; }
    main > section.card:nth-of-type(1) .small { font-size:7px; line-height:1.15; }
    .alert-stack { min-height:0; margin:0; gap:5px; grid-template-columns:repeat(3,minmax(0,1fr)); }
    .alert-card { height:92px; min-height:0; padding:5px 3px; gap:3px; border-radius:10px; border-width:1px; }
    .alert-card .alert-icon { flex:0 0 22px; width:22px; height:22px; font-size:15px; }
    .alert-card .alert-title { font-size:clamp(8px,2.4vw,11px); line-height:1.08; margin:0 0 3px; }
    .alert-card .alert-detail { font-size:clamp(7px,1.9vw,9px); line-height:1.12; }
    main > section.card:nth-of-type(3) {
        min-height:0; height:100%; margin:0; padding:6px !important;
        display:flex; flex-direction:column; overflow:hidden;
    }
    main > section.card:nth-of-type(3) .row { flex:0 0 auto; }
    main > section.card:nth-of-type(3) .section-title { font-size:8px; margin:0 0 2px; }
    main > section.card:nth-of-type(3) .chart-legend { margin:3px 0; gap:7px; font-size:8px; }
    main > section.card:nth-of-type(3) .chart-wrap { flex:1 1 auto; min-height:0; height:auto !important; }
    main > section.card:nth-of-type(3) .row:last-of-type { flex:0 0 auto; }
    main > section.card:nth-of-type(3) > .small { font-size:7px; flex:0 0 auto; }
    main > section.card:nth-of-type(5) { min-height:0; margin:0; padding:5px 8px !important; }
    main > section.card:nth-of-type(5) .signal { font-size:clamp(15px,4vw,19px); margin:2px 0; }
    main > section.card:nth-of-type(5) .label,
    main > section.card:nth-of-type(5) .small { font-size:7px; }
    .technical-card, .footer { display:none !important; }
}
@media (max-width: 380px) {
    main { padding:4px; gap:4px; }
    .alert-card { height:84px; }
    .alert-card .alert-title { font-size:8px; }
    .alert-card .alert-detail { font-size:7px; }
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
            <span id="sourceDot" class="dot live"></span>
            <span id="sourceStatus">BRTI · FUENTE ÚNICA</span>
        </div>

    </div>

</div>


<div class="grid">


    <section class="card brti-card">

        <div class="brand-mark">BR</div>

        <div class="label">
            PRECIO ACTUAL · BRTI
        </div>

        <div class="price" id="brtiPrice">--</div>

        <div class="small" id="brtiAgeLabel">
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
            FUERZA DE SEÑAL
        </span>

        <strong id="strength" class="purple">
            --
        </strong>

    </div>

    <div class="small" id="signalStage">
        BRTI · FUENTE ÚNICA DE PREDICCIÓN
    </div>

</section>

<section class="alert-stack" id="alertStack" aria-label="Alertas del radar">
    <article class="alert-card prealert" id="prealertCard">
        <div class="alert-icon">!</div>
        <div class="alert-copy">
            <div class="alert-title" id="prealertTitle">PREALERTA</div>
            <div class="alert-detail" id="prealertDetail">Aviso temprano cuando empieza a formarse una dirección.</div>
        </div>
    </article>
    <article class="alert-card entry" id="entryCard">
        <div class="alert-icon">✓</div>
        <div class="alert-copy">
            <div class="alert-title" id="entryTitle">ENTRADA CONFIRMADA</div>
            <div class="alert-detail" id="entryDetail">Se activa cuando la señal principal y los movimientos de 1 y 3 minutos coinciden.</div>
        </div>
    </article>
    <article class="alert-card protect" id="protectCard">
        <div class="alert-icon">!</div>
        <div class="alert-copy">
            <div class="alert-title" id="protectTitle">PROTEGER TU ENTRADA</div>
            <div class="alert-detail" id="protectDetail">Se activa si una entrada confirmada pierde su confirmación.</div>
        </div>
    </article>
</section>


<section class="card">

    <div class="row">

        <div>

            <div class="section-title">
                GRÁFICA BTC/USD
            </div>

            <div class="small">
                BRTI · Velas de 1 minuto
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

        <span>
            <i class="legend-dot"
               style="background:#00d995"></i>
            OBJETIVO KALSHI
        </span>

    </div>

    <div class="chart-wrap">

        <div id="chart" role="img" aria-label="Gráfica de velas BTC/USD"></div>

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

    <div class="small" id="sourceConnection">Predicción y gráfica: BRTI</div>

    <div class="small" id="errors"></div>

</section>


<div class="footer">
    BTC RADAR · DATOS EN TIEMPO REAL · MODO LECTURA
</div>

</main>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<script>

const $ = id => document.getElementById(id);

let candles = [];
let chartTargetValue = null;
let chart = null;
let candleSeries = null;
let ema9Line = null;
let ema21Line = null;
let chartTargetLine = null;
let chartFollowLive = true;
let chartInitialized = false;
let busy = false;

function money(value){
    if(value === null || value === undefined || !Number.isFinite(Number(value))){
        return "--";
    }
    return Number(value).toLocaleString("en-US", {
        minimumFractionDigits:2,
        maximumFractionDigits:2
    });
}

function clock(seconds){
    const safe = Math.max(0, Number(seconds) || 0);
    const m = Math.floor(safe / 60);
    const s = Math.floor(safe % 60);
    return String(m).padStart(2,"0") + ":" + String(s).padStart(2,"0");
}

function emaSeries(values, period){
    if(!values.length) return [];
    const alpha = 2 / (period + 1);
    let result = Number(values[0]);
    const output = [result];
    for(let i=1;i<values.length;i++){
        result = alpha * Number(values[i]) + (1-alpha) * result;
        output.push(result);
    }
    return output;
}

function initChart(){
    const container = $("chart");
    if(!container || !window.LightweightCharts) {
        if(container) container.textContent = "No se pudo cargar el motor de gráficas.";
        return;
    }

    chart = LightweightCharts.createChart(container, {
        width: Math.max(1, container.clientWidth),
        height: Math.max(180, container.clientHeight),
        layout: {
            background: { type:"solid", color:"#0b1220" },
            textColor:"#91a2c1",
            fontFamily:"Arial, sans-serif",
            fontSize:10
        },
        grid: {
            vertLines:{ color:"#1b2940" },
            horzLines:{ color:"#1b2940" }
        },
        rightPriceScale:{
            borderColor:"#2a3850",
            autoScale:true,
            scaleMargins:{ top:0.12, bottom:0.12 }
        },
        timeScale:{
            borderColor:"#2a3850",
            timeVisible:true,
            secondsVisible:false,
            rightOffset:5,
            barSpacing:7,
            minBarSpacing:2,
            lockVisibleTimeRangeOnResize:true
        },
        crosshair:{
            mode:LightweightCharts.CrosshairMode.Normal,
            vertLine:{ color:"#60718e", width:1, style:2 },
            horzLine:{ color:"#60718e", width:1, style:2 }
        },
        handleScroll:{
            mouseWheel:true,
            pressedMouseMove:true,
            horzTouchDrag:true,
            vertTouchDrag:false
        },
        handleScale:{
            axisPressedMouseMove:true,
            mouseWheel:true,
            pinch:true
        },
        localization:{
            priceFormatter: price => Number(price).toLocaleString("en-US", {
                minimumFractionDigits:2,
                maximumFractionDigits:2
            })
        }
    });

    candleSeries = chart.addCandlestickSeries({
        upColor:"#00df9a",
        downColor:"#ff5269",
        borderUpColor:"#00df9a",
        borderDownColor:"#ff5269",
        wickUpColor:"#00df9a",
        wickDownColor:"#ff5269",
        priceLineVisible:true,
        lastValueVisible:true
    });

    ema9Line = chart.addLineSeries({
        color:"#b98aff",
        lineWidth:2,
        priceLineVisible:false,
        lastValueVisible:false,
        crosshairMarkerVisible:false
    });

    ema21Line = chart.addLineSeries({
        color:"#45a9ff",
        lineWidth:2,
        priceLineVisible:false,
        lastValueVisible:false,
        crosshairMarkerVisible:false
    });

    container.addEventListener("pointerdown", () => {
        chartFollowLive = false;
    }, {passive:true});

    if(window.ResizeObserver){
        const observer = new ResizeObserver(entries => {
            const box = entries[0] && entries[0].contentRect;
            if(box && chart){
                chart.applyOptions({
                    width:Math.max(1, Math.floor(box.width)),
                    height:Math.max(180, Math.floor(box.height))
                });
            }
        });
        observer.observe(container);
    } else {
        window.addEventListener("resize", () => {
            if(chart) chart.applyOptions({
                width:Math.max(1,container.clientWidth),
                height:Math.max(180,container.clientHeight)
            });
        });
    }

    chartInitialized = true;
}

function drawChart(data){
    if(Array.isArray(data)) candles = data;
    if(!chartInitialized) initChart();
    if(!chart || !candleSeries) return;

    const previousRange = chartFollowLive
        ? null
        : chart.timeScale().getVisibleLogicalRange();

    const rows = candles
        .map(c => ({
            time:Math.floor(Number(c.time)),
            open:Number(c.open),
            high:Number(c.high),
            low:Number(c.low),
            close:Number(c.close)
        }))
        .filter(c =>
            Number.isFinite(c.time) &&
            Number.isFinite(c.open) &&
            Number.isFinite(c.high) &&
            Number.isFinite(c.low) &&
            Number.isFinite(c.close) &&
            c.time > 0 &&
            c.low > 0 &&
            c.high >= c.low
        )
        .sort((a,b)=>a.time-b.time)
        .filter((c,i,arr)=>i===0 || c.time!==arr[i-1].time);

    if(!rows.length){
        candleSeries.setData([]);
        ema9Line.setData([]);
        ema21Line.setData([]);
        return;
    }

    candleSeries.setData(rows);

    const closes = rows.map(c=>c.close);
    const e9 = emaSeries(closes,9);
    const e21 = emaSeries(closes,21);

    ema9Line.setData(rows.map((c,i)=>({time:c.time,value:e9[i]})));
    ema21Line.setData(rows.map((c,i)=>({time:c.time,value:e21[i]})));

    if(chartTargetLine){
        candleSeries.removePriceLine(chartTargetLine);
        chartTargetLine = null;
    }
    if(Number.isFinite(chartTargetValue) && chartTargetValue > 0){
        chartTargetLine = candleSeries.createPriceLine({
            price:chartTargetValue,
            color:"#00d995",
            lineWidth:1,
            lineStyle:LightweightCharts.LineStyle.Dashed,
            axisLabelVisible:true,
            title:"KALSHI"
        });
    }

    if(chartFollowLive || !previousRange){
        chart.timeScale().setVisibleLogicalRange({
            from:Math.max(0,rows.length-55),
            to:rows.length+4
        });
    } else {
        const from = Math.max(-5, Math.min(previousRange.from, rows.length-2));
        const to = Math.max(from+10, Math.min(previousRange.to, rows.length+8));
        chart.timeScale().setVisibleLogicalRange({from,to});
    }
}

function goLive(){
    chartFollowLive = true;
    if(chart && candles.length){
        chart.timeScale().scrollToRealTime();
    }
}

// ==========================================
// ACTUALIZAR INFORMACIÓN DEL RADAR
// ==========================================

function updateRadar(d){

    $("brtiPrice").textContent =
        money(d.brti_price);

    const direction=d.direction || "RECOPILANDO";
    const predictedElement=$("predictedPrice");
    predictedElement.textContent=money(d.predicted_price);
    predictedElement.className="price predicted "+(
        direction==="ALCISTA" ? "bullish" :
        direction==="BAJISTA" ? "bearish" :
        "uncertain"
    );

    chartTargetValue=(
        d.kalshi_market_target==null
        ? null
        : Number(d.kalshi_market_target)
    );
    if(!Number.isFinite(chartTargetValue)) chartTargetValue=null;

    $("predictionLabel").textContent =
        d.target_gap_pct == null
        ? "Estimación 15 min · esperando objetivo de Kalshi"
        : "Respecto al objetivo: " + d.target_direction
          + " (" + Number(d.target_gap_pct).toFixed(3) + "%)";

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

     $("brtiAgeLabel").textContent =
        d.brti_age == null
        ? "Esperando índice BRTI"
        : "BRTI actualizado hace " + d.brti_age + " s";


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

    $("signalStage").textContent =
        (d.signal_stage || (
            direction==="RECOPILANDO"
            ? "Esperando historial BRTI suficiente"
            : "Predicción calculada exclusivamente con BRTI"
        )) +
        (d.short_predicted_price_1m != null
            ? " · 1 min: " + money(d.short_predicted_price_1m)
            : "") +
        (d.short_predicted_price_3m != null
            ? " · 3 min: " + money(d.short_predicted_price_3m)
            : "") +
        (d.pattern_learning_used
            ? " · patrón validado con " + d.pattern_samples + " resultados"
            : " · aprendizaje histórico en recopilación");

    const alertData = d.alert || {};
    const alertType = String(alertData.type || "").toUpperCase();
    const entryStage = String(d.entry_stage || "").toUpperCase();
    const alertDirection = String(d.entry_direction || d.direction || "").toUpperCase();
    const activePrealert = alertType === "PREALERTA" ||
        (!alertType && entryStage === "PREALERTA");
    const activeEntry = alertType === "ENTRADA" ||
        (!alertType && entryStage === "ENTRADA");
    const activeProtect = alertType === "PROTEGER";
    $("prealertCard").classList.toggle("active", activePrealert);
    $("entryCard").classList.toggle("active", activeEntry);
    $("protectCard").classList.toggle("active", activeProtect);
    $("prealertTitle").textContent = activePrealert
        ? "PREALERTA " + (alertDirection || "") : "PREALERTA";
    $("entryTitle").textContent = activeEntry
        ? "ENTRADA CONFIRMADA " + (alertDirection || "") : "ENTRADA CONFIRMADA";
    $("protectTitle").textContent = activeProtect
        ? "PROTEGER TU ENTRADA" : "PROTEGER TU ENTRADA";
    if (activePrealert) {
        $("prealertDetail").textContent = alertData.detail ||
            "Se está formando un movimiento " + (alertDirection || "direccional") + ". Todavía no es confirmación completa.";
    }
    if (activeEntry) {
        $("entryDetail").textContent = alertData.detail ||
            "La dirección principal y los movimientos de 1 y 3 minutos están alineados.";
    }
    if (activeProtect) {
        $("protectDetail").textContent = alertData.detail ||
            "La señal confirmada se debilitó. Revisa el contrato y el riesgo.";
    }


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

    $("sourceDot").className=
        d.brti_live ? "dot live":"dot";

    $("brtiStatus").textContent=
        d.brti_live
        ? "BRTI EN VIVO"
        : "BRTI SIN DATOS";

    $("sourceStatus").textContent=
        d.brti_live ? "BRTI · FUENTE ÚNICA" : "BRTI SIN DATOS";


    $("kalshiConnection").textContent=
        d.kalshi_connected
        ? "CONECTADO"
        : "DESCONECTADO";

    $("kalshiConnection").className=
        d.kalshi_connected
        ? "green"
        : "red";


    $("sourceConnection").textContent =
        "Fuente única de predicción y gráfica: BRTI";


    $("countdown").textContent=
        clock(Number(d.countdown || 0));


    $("errors").textContent=
        [
            d.kalshi_error
        ]
        .filter(Boolean)
        .join(" | ");


    drawChart(d.brti_candles || []);

    $("candleCount").textContent=
        (d.brti_candles || []).length+
        " velas BRTI";

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

window.addEventListener("resize", () => {
    if(chart) chart.applyOptions({
        width:Math.max(1,$("chart").clientWidth),
        height:Math.max(180,$("chart").clientHeight)
    });
});

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
