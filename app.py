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

from flask import Flask, jsonify, render_template_string, request

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import pattern_memory
import market_intelligence
app = Flask(__name__)

# ==========================================
# CONFIGURACIÓN
# ==========================================

KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"
KALSHI_REST = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_MARKET_DATA_REST = "https://external-api.kalshi.com/trade-api/v2"
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

# Diagnóstico del flujo BRTI: conexión, recepción, lectura y persistencia.
_brti_feed_diag = {
    "ws_open_count": 0,
    "ws_message_count": 0,
    "brti_parsed_count": 0,
    "db_save_attempts": 0,
    "db_save_success": 0,
    "db_save_failures": 0,
    "last_ws_open_ts": None,
    "last_ws_message_ts": None,
    "last_brti_parsed_ts": None,
    "last_db_save_ts": None,
    "last_db_save_error": ""
}

kalshi_error = ""
coinbase_error = ""
kalshi_market_target = None
kalshi_market_ticker = ""
kalshi_market_close = None
kalshi_market_error = ""
orderbook_cache = {"ticker": None, "updated_ts": 0.0, "data": None}
market_resolution_queue = deque(maxlen=20)
_market_result_last_check = {}

started = False
last_pattern_snapshot_minute = None
last_contract_snapshot_minute = None
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
_entry_validation_cache = {"checked_at": 0.0, "data": None}
_guidance_state = {
    "ticker": None,
    "stage": "SIN_SEÑAL",
    "direction": None,
    "last_alert_ts": 0.0,
    "alert": None,
    "danger_count": 0,
    "exit_latched_direction": None,
    "hold_announced": False
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


def get_kalshi_orderbook(ticker):
    """Lee el libro real de Kalshi y deriva asks desde bids binarios."""
    if not ticker:
        return {"available": False, "error": "SIN CONTRATO"}

    now = time.time()
    with lock:
        cached = dict(orderbook_cache)
    if cached.get("ticker") == ticker and cached.get("data") and now - cached.get("updated_ts", 0) < 3:
        return dict(cached["data"])

    try:
        response = session.get(
            f"{KALSHI_MARKET_DATA_REST}/markets/{ticker}/orderbook",
            timeout=5
        )
        response.raise_for_status()
        payload = response.json()
        book = payload.get("orderbook_fp") or payload.get("orderbook") or {}

        def levels(name):
            raw = book.get(name + "_dollars")
            dollar_values = True
            if raw is None:
                raw = book.get(name) or []
                dollar_values = False
            parsed = []
            for row in raw:
                try:
                    price = float(row[0])
                    size = float(row[1])
                    if not dollar_values:
                        price /= 100.0
                    if math.isfinite(price) and math.isfinite(size) and 0 < price < 1 and size > 0:
                        parsed.append((price, size))
                except (TypeError, ValueError, IndexError):
                    continue
            return parsed

        yes_levels = levels("yes")
        no_levels = levels("no")
        best_yes = max(yes_levels, key=lambda x: x[0]) if yes_levels else None
        best_no = max(no_levels, key=lambda x: x[0]) if no_levels else None

        yes_ask = 1.0 - best_no[0] if best_no else None
        no_ask = 1.0 - best_yes[0] if best_yes else None
        data = {
            "available": bool(best_yes or best_no),
            "ticker": ticker,
            "best_yes_bid": round(best_yes[0], 4) if best_yes else None,
            "best_yes_bid_size": round(best_yes[1], 2) if best_yes else None,
            "best_no_bid": round(best_no[0], 4) if best_no else None,
            "best_no_bid_size": round(best_no[1], 2) if best_no else None,
            "best_yes_ask": round(yes_ask, 4) if yes_ask is not None else None,
            "best_yes_ask_size": round(best_no[1], 2) if best_no else None,
            "best_no_ask": round(no_ask, 4) if no_ask is not None else None,
            "best_no_ask_size": round(best_yes[1], 2) if best_yes else None,
            "yes_spread": round(yes_ask - best_yes[0], 4) if yes_ask is not None and best_yes else None,
            "no_spread": round(no_ask - best_no[0], 4) if no_ask is not None and best_no else None,
            "updated_ts": now,
            "error": None
        }
        with lock:
            orderbook_cache.update({"ticker": ticker, "updated_ts": now, "data": data})
        return data
    except Exception as exc:
        data = {
            "available": False, "ticker": ticker, "best_yes_bid": None,
            "best_yes_bid_size": None, "best_yes_ask": None,
            "best_yes_ask_size": None, "best_no_bid": None,
            "best_no_bid_size": None, "best_no_ask": None,
            "best_no_ask_size": None, "yes_spread": None,
            "no_spread": None, "updated_ts": now,
            "error": str(exc)[:240]
        }
        with lock:
            orderbook_cache.update({"ticker": ticker, "updated_ts": now, "data": data})
        return data


def kalshi_orderbook_worker():
    """Actualiza el libro en segundo plano para no retrasar /api ni la pantalla."""
    while True:
        try:
            with lock:
                ticker = kalshi_market_ticker
                close_ts = kalshi_market_close
            if ticker and close_ts and float(close_ts) > time.time():
                get_kalshi_orderbook(ticker)
            else:
                with lock:
                    if orderbook_cache.get("ticker"):
                        orderbook_cache.update({
                            "ticker": None, "updated_ts": 0.0, "data": None
                        })
        except Exception as exc:
            print("ERROR LIBRO KALSHI:", str(exc), flush=True)
        time.sleep(2.5)


def get_cached_kalshi_orderbook(ticker):
    if not ticker:
        return {"available": False, "error": "SIN CONTRATO"}
    with lock:
        cached = dict(orderbook_cache)
    if cached.get("ticker") == ticker and cached.get("data"):
        data = dict(cached["data"])
        if time.time() - float(cached.get("updated_ts") or 0) <= 8:
            return data
    return {"available": False, "ticker": ticker, "error": "ESPERANDO LIBRO ACTUALIZADO"}

def calculate_entry_economics(direction, bullish_probability, bearish_probability, orderbook):
    """EV indicativo por contrato, usando ask del libro y tarifa taker publicada."""
    if not orderbook or not orderbook.get("available"):
        return {"available": False, "reason": "LIBRO NO DISPONIBLE"}
    side = "YES" if direction == "ALCISTA" else "NO" if direction == "BAJISTA" else None
    if not side:
        return {"available": False, "reason": "SIN DIRECCIÓN"}
    ask = orderbook.get("best_yes_ask" if side == "YES" else "best_no_ask")
    probability_pct = bullish_probability if side == "YES" else bearish_probability
    try:
        price = float(ask)
        probability = float(probability_pct) / 100.0
    except (TypeError, ValueError):
        return {"available": False, "reason": "PRECIO O PROBABILIDAD NO DISPONIBLE"}
    if not (0 < price < 1 and 0 <= probability <= 1):
        return {"available": False, "reason": "DATOS FUERA DE RANGO"}

    # Tarifa taker general publicada: 7% * P * (1-P), redondeada hacia arriba
    # al centavo superior para una compra inmediata de 1 contrato.
    raw_fee = 0.07 * price * (1.0 - price)
    fee = math.ceil((raw_fee - 1e-12) * 100.0) / 100.0
    total_cost = price + fee
    expected_value = probability - total_cost
    return {
        "available": True,
        "side": side,
        "entry_ask": round(price, 4),
        "model_probability_pct": round(probability * 100.0, 2),
        "estimated_taker_fee": round(fee, 4),
        "estimated_total_cost": round(total_cost, 4),
        "estimated_net_ev_per_contract": round(expected_value, 4),
        "estimated_net_ev_pct_of_cost": round((expected_value / total_cost) * 100.0, 2) if total_cost > 0 else None,
        "break_even_probability_pct": round(total_cost * 100.0, 2),
        "is_positive_estimated_ev": expected_value > 0,
        "warning": "ESTIMACIÓN CON PROBABILIDAD DEL MODELO; NO ES RENTABILIDAD VALIDADA"
    }


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
    wall_now = time.time()

    # Evita que mensajes retrasados o con reloj incorrecto hagan retroceder
    # el precio actual y contaminen el historial reciente.
    if (
        not math.isfinite(timestamp)
        or timestamp <= 0
        or timestamp > wall_now + 2
    ):
        return
    with lock:
        if brti_time and timestamp < brti_time - 5:
            return

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

        # Guarda la vela en formación cada 5 segundos para reducir
        # la pérdida de datos ante reinicios de Render.
        if monotonic_now - _last_brti_candle_save >= 5:
            current_snapshot = dict(brti_candles[-1])
            _last_brti_candle_save = monotonic_now

    candles_to_save = []
    if completed_candle is not None:
        candles_to_save.append(completed_candle)
    if current_snapshot is not None:
        candles_to_save.append(current_snapshot)

    for candle in candles_to_save:
        saved = pattern_memory.save_brti_candle(candle)
        with lock:
            _brti_feed_diag["db_save_attempts"] += 1
            if saved:
                _brti_feed_diag["db_save_success"] += 1
                _brti_feed_diag["last_db_save_ts"] = time.time()
                _brti_feed_diag["last_db_save_error"] = ""
            else:
                _brti_feed_diag["db_save_failures"] += 1
                _brti_feed_diag["last_db_save_error"] = str(
                    pattern_memory.database_status()
                )[:180]

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


_probability_calibration_cache = {"ts": 0.0, "bands": []}

def get_cached_probability_calibration():
    """Bandas empíricas BRTI; consulta como máximo una vez por minuto."""
    now = time.monotonic()
    with lock:
        cached = _probability_calibration_cache
        if now - cached["ts"] < 60:
            return list(cached["bands"])
    try:
        bands = pattern_memory.get_15m_probability_calibration()
        if not isinstance(bands, list):
            bands = []
    except Exception:
        bands = []
    with lock:
        _probability_calibration_cache["ts"] = now
        _probability_calibration_cache["bands"] = list(bands)
    return bands


def calibrate_directional_scores(bullish, bearish):
    """
    Convierte la fuerza bruta en una estimación empírica por banda.
    Exige 30 contratos distintos resueltos; si no, conserva el valor bruto.
    """
    bullish = float(bullish)
    bearish = float(bearish)
    raw_confidence = max(bullish, bearish)
    band_floor = int(raw_confidence // 5) * 5
    samples = 0
    calibrated = False
    output_confidence = raw_confidence

    for row in get_cached_probability_calibration():
        try:
            if int(row.get("band_floor")) != band_floor:
                continue
            samples = int(row.get("samples") or 0)
            correct = int(row.get("correct") or 0)
            if samples >= 30:
                # Suavizado beta(5,5): evita extremos con muestras limitadas.
                empirical = ((correct + 5.0) / (samples + 10.0)) * 100.0
                output_confidence = max(50.0, min(95.0, empirical))
                calibrated = True
            break
        except (TypeError, ValueError):
            continue

    if bullish >= bearish:
        return (
            round(output_confidence, 1),
            round(100.0 - output_confidence, 1),
            calibrated,
            samples,
            "BRTI_900S_CONTRATOS_UNICOS" if calibrated else "SIN_MUESTRA_SUFICIENTE"
        )
    return (
        round(100.0 - output_confidence, 1),
        round(output_confidence, 1),
        calibrated,
        samples,
        "BRTI_900S_CONTRATOS_UNICOS" if calibrated else "SIN_MUESTRA_SUFICIENTE"
    )


def get_cached_pattern_stats(pattern):
    """Cachea estadísticas válidas 60 s; los ceros solo 5 s para recoger cierres nuevos."""
    if not pattern or pattern == "SIN PATRÓN CLARO":
        return {"samples": 0, "bullish_pct": None, "bearish_pct": None}
    now = time.monotonic()
    with lock:
        cached = _pattern_stats_cache.get(pattern)
        if cached:
            cached_stats = cached[1]
            cached_samples = int(cached_stats.get("samples") or 0)
            cache_ttl = 60 if cached_samples > 0 else 5
            if now - cached[0] < cache_ttl:
                return dict(cached_stats)
    try:
        stats = pattern_memory.get_pattern_stats(pattern)
        if not isinstance(stats, dict):
            stats = {"samples": 0, "bullish_pct": None, "bearish_pct": None}
    except Exception:
        stats = {"samples": 0, "bullish_pct": None, "bearish_pct": None}
    with lock:
        _pattern_stats_cache[pattern] = (now, dict(stats))
    return stats


def get_entry_validation():
    """No habilita entradas hasta reunir una muestra post-reinicio suficiente."""
    now = time.monotonic()
    with lock:
        cached = _entry_validation_cache.get("data")
        if cached and now - float(_entry_validation_cache.get("checked_at", 0)) < 60:
            return dict(cached)
    try:
        stats = pattern_memory.get_15m_prediction_accuracy_by_contract()
        samples = int(stats.get("contracts_evaluated") or 0)
        accuracy = stats.get("accuracy_pct")
        accuracy = float(accuracy) if accuracy is not None else None
        minimum_samples = 30
        minimum_accuracy = 55.0
        qualified = (
            samples >= minimum_samples
            and accuracy is not None
            and accuracy >= minimum_accuracy
        )
        data = {
            "samples": samples,
            "minimum_samples": minimum_samples,
            "accuracy_pct": accuracy,
            "minimum_accuracy_pct": minimum_accuracy,
            "qualified": qualified,
            "status": (
                "VALIDADA" if qualified
                else "MUESTRA INSUFICIENTE" if samples < minimum_samples
                else "PRECISIÓN INSUFICIENTE"
            )
        }
    except Exception as exc:
        data = {
            "samples": 0, "minimum_samples": 30,
            "accuracy_pct": None, "minimum_accuracy_pct": 55.0,
            "qualified": False, "status": "ERROR DE VALIDACIÓN",
            "error": str(exc)[:160]
        }
    with lock:
        _entry_validation_cache.update({"checked_at": now, "data": dict(data)})
    return data


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



def calculate_gbm_baseline(spot, strike, seconds_remaining, ticks):
    """
    Modelo GBM sin deriva para P(BRTI al cierre >= strike).
    Usa cierres de minuto reconstruidos de ticks BRTI; no sustituye la señal
    principal y no calcula probabilidad si el historial es insuficiente.
    """
    result = {
        "gbm_status": "SIN OBJETIVO",
        "gbm_yes_probability": None,
        "gbm_no_probability": None,
        "gbm_volatility_annualized_pct": None,
        "gbm_volatility_samples": 0
    }
    if not valid_price(spot) or not valid_price(strike):
        return result

    try:
        horizon = max(1.0, min(900.0, float(seconds_remaining or 0)))
        now = time.time()
        current_minute = int(now // 60)
        minute_closes = {}

        for item in ticks or []:
            try:
                ts, value = float(item[0]), float(item[1])
                if (
                    not math.isfinite(ts)
                    or not valid_price(value)
                    or ts > now + 2
                    or ts < now - 181 * 60
                ):
                    continue
                minute = int(ts // 60)
                previous = minute_closes.get(minute)
                if previous is None or ts > previous[0]:
                    minute_closes[minute] = (ts, value)
            except (TypeError, ValueError, IndexError, OverflowError):
                continue

        ordered = sorted(minute_closes.items())
        log_returns = []
        for index in range(1, len(ordered)):
            previous_minute, previous_row = ordered[index - 1]
            minute, row = ordered[index]
            if minute != previous_minute + 1:
                continue
            previous_price = previous_row[1]
            current_price = row[1]
            if previous_price > 0 and current_price > 0:
                log_returns.append(math.log(current_price / previous_price))

        sample_count = len(log_returns)
        result["gbm_volatility_samples"] = sample_count
        if sample_count < 20:
            result["gbm_status"] = "HISTORIAL_INSUFICIENTE"
            return result

        mean_return = sum(log_returns) / sample_count
        variance = sum((value - mean_return) ** 2 for value in log_returns) / (sample_count - 1)
        sigma_per_minute = math.sqrt(max(0.0, variance))
        sigma_per_second = sigma_per_minute / math.sqrt(60.0)
        sigma_horizon = sigma_per_second * math.sqrt(horizon)

        annualized = sigma_per_second * math.sqrt(365 * 24 * 60 * 60) * 100
        result["gbm_volatility_annualized_pct"] = round(annualized, 2)

        if sigma_horizon <= 1e-10:
            probability_yes = 0.99 if float(spot) >= float(strike) else 0.01
        else:
            z_score = (
                math.log(float(spot) / float(strike))
                - 0.5 * sigma_horizon ** 2
            ) / sigma_horizon
            probability_yes = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))

        probability_yes = max(0.01, min(0.99, probability_yes))
        result.update({
            "gbm_status": "OK",
            "gbm_yes_probability": round(probability_yes * 100, 1),
            "gbm_no_probability": round((1.0 - probability_yes) * 100, 1)
        })
        return result
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        result["gbm_status"] = "ERROR_CALCULO"
        return result


def calculate_prediction(
    price,
    ticks,
    seconds_to_close,
    market_target=None,
    candles=None,
    stabilize=True
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
            "target_influence": "SIN DATOS",
            "prediction_data_quality": "SIN DATOS BRTI",
            "contiguous_candles_used": 0,
            "candle_gap_detected": False
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

            if (
                high < low
                or high < max(open_price, close)
                or low > min(open_price, close)
            ):
                continue

            candle_time = int(candle.get("time", 0) or 0)
            if candle_time <= 0:
                continue

            valid_candles.append({
                "time": candle_time,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": max(0, volume)
            })

        except (TypeError, ValueError, KeyError, OverflowError):
            continue

    # Orden cronológico: no tratar velas separadas por huecos como minutos
    # consecutivos. Protege EMA, RSI, patrones, momentum y proyección.
    valid_candles = sorted(valid_candles[-300:], key=lambda row: row["time"])
    candle_gap_detected = any(
        valid_candles[i]["time"] - valid_candles[i - 1]["time"] > 90
        for i in range(1, len(valid_candles))
    )
    contiguous_candles = []
    for candle in reversed(valid_candles):
        if contiguous_candles:
            gap_seconds = contiguous_candles[-1]["time"] - candle["time"]
            if gap_seconds < 50 or gap_seconds > 90:
                break
        contiguous_candles.append(candle)
    valid_candles = list(reversed(contiguous_candles))
    candle_data_quality = (
        "HISTORIAL CON HUECOS · USANDO SOLO VELAS CONTIGUAS"
        if candle_gap_detected else "VELAS CONTIGUAS"
    )

    closes = [
        candle["close"]
        for candle in valid_candles
    ]

    if len(valid_candles) < 22:
        # No calcular EMA21/RSI14 con un tramo incompleto: exigir al menos
        # 22 velas consecutivas. Mientras se reconstruye, usar ticks BRTI
        # recientes como señal provisional y evitar patrones con poco historial.
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
                "target_influence": "PENDIENTE",
                "prediction_data_quality": candle_data_quality,
                "contiguous_candles_used": len(valid_candles),
                "candle_gap_detected": candle_gap_detected
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
        target_expected_move_pct = None
        target_z_score = None
        target_direction = "SIN OBJETIVO"
        target_influence = "SIN OBJETIVO"
        normalized = max(-1.0, min(1.0, momentum / 0.05)) * 0.55

        if valid_price(market_target):
            target_gap_pct = ((predicted - float(market_target)) / float(market_target)) * 100
            tick_prices = [item[1] for item in recent_ticks if valid_price(item[1])]
            tick_range_pct = (
                (max(tick_prices) - min(tick_prices))
                / (sum(tick_prices) / len(tick_prices)) * 100
                if len(tick_prices) >= 3 else 0.0
            )
            target_expected_move_pct = max(
                0.015,
                tick_range_pct * math.sqrt(max(1.0, remaining_minutes)) * 0.75
            )
            target_z_score = target_gap_pct / target_expected_move_pct
            target_normalized = math.tanh(target_z_score)
            # El strike se compara con el movimiento que todavía cabe en el tiempo restante.
            # Evita que una distancia fija de 0.05% sature la señal en mercados volátiles.
            normalized = 0.65 * target_normalized + 0.35 * normalized
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

        if stabilize:
            normalized, direction = stabilize_direction(normalized, seconds_to_close)
        else:
            direction = (
                "ALCISTA" if normalized >= 0.14
                else "BAJISTA" if normalized <= -0.14
                else "INDECISO"
            )
        raw_bullish = max(20, min(80, round(50 + normalized * 30, 1)))
        raw_bearish = round(100 - raw_bullish, 1)
        bullish, bearish, probability_calibrated, probability_calibration_samples, probability_calibration_method = calibrate_directional_scores(
            raw_bullish, raw_bearish
        )
        # No sobrescribir la dirección estabilizada con el porcentaje mayor:
        # un empate o una señal débil no debe convertirse artificialmente en lado.
        return {
            "direction": direction,
            "predicted_price": round(predicted, 2),
            "bullish_score": bullish,
            "bearish_score": bearish,
            "raw_bullish_score": raw_bullish,
            "raw_bearish_score": raw_bearish,
            "technical_strength": round(abs(normalized) * 100),
            "momentum_pct": round(momentum, 5),
            "ema9": None,
            "ema21": None,
            "rsi": None,
            "pattern": "SEÑAL INICIAL BRTI · TICKS",
            "target_influence": target_influence,
            "target_gap_pct": round(target_gap_pct, 5) if target_gap_pct is not None else None,
            "target_expected_move_pct": round(target_expected_move_pct, 5) if target_expected_move_pct is not None else None,
            "target_z_score": round(target_z_score, 4) if target_z_score is not None else None,
            "target_direction": target_direction,
            "technical_score": round(normalized, 4),
            "probability_calibrated": probability_calibrated,
            "probability_calibration_samples": probability_calibration_samples,
            "probability_calibration_method": probability_calibration_method,
            "direction_method": "BRTI_TICKS_BOOTSTRAP_15M",
            "prediction_data_quality": (
                "TICKS RECIENTES · " + candle_data_quality
                if candle_gap_detected else "TICKS BRTI RECIENTES"
            ),
            "contiguous_candles_used": len(valid_candles),
            "candle_gap_detected": candle_gap_detected
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
    # PROYECCIÓN ADAPTATIVA HASTA EL CIERRE
    # --------------------------------------

    remaining = max(
        0,
        min(900, float(seconds_to_close or 0))
    )
    remaining_minutes = remaining / 60.0

    # Combina ritmos de 1, 3, 5 y 15 minutos. Los plazos cortos
    # reaccionan antes a los giros; los largos evitan perseguir ruido.
    rate_windows = (
        (1, 0.40),
        (3, 0.30),
        (5, 0.20),
        (15, 0.10)
    )
    available_rates = []
    for window, weight in rate_windows:
        if len(closes) > window and closes[-(window + 1)] > 0:
            change = (
                (closes[-1] / closes[-(window + 1)]) - 1
            ) * 100
            available_rates.append((change / float(window), weight, window))

    if available_rates:
        weight_total = sum(item[1] for item in available_rates)
        rate_per_minute = sum(
            rate * weight for rate, weight, _ in available_rates
        ) / max(weight_total, 0.000001)
    else:
        rate_per_minute = 0.0

    # Si el último minuto se mueve contra la tendencia de 5/15m,
    # se reduce la extrapolación para no proyectar tarde un giro.
    short_rate = next(
        (rate for rate, _, window in available_rates if window == 1),
        None
    )
    long_rates = [
        rate for rate, _, window in available_rates if window >= 5
    ]
    reversal_conflict = bool(
        short_rate is not None
        and long_rates
        and short_rate * (sum(long_rates) / len(long_rates)) < 0
    )
    if reversal_conflict:
        rate_per_minute *= 0.55

    # La técnica puede confirmar o moderar el ritmo, nunca invertirlo
    # por sí sola. Esto evita amplificar una proyección contradictoria.
    pre_projection_technical = score / possible if possible > 0 else 0.0
    if (
        rate_per_minute != 0
        and pre_projection_technical * rate_per_minute < 0
        and abs(pre_projection_technical) >= 0.25
    ):
        rate_per_minute *= 0.70
    elif (
        rate_per_minute != 0
        and pre_projection_technical * rate_per_minute > 0
        and abs(pre_projection_technical) >= 0.45
    ):
        rate_per_minute *= 1.08

    # Límite basado en el rango real reciente de las velas, no en un
    # porcentaje fijo. La raíz del tiempo evita crecer linealmente sin control.
    recent_ranges_pct = [
        ((c["high"] - c["low"]) / c["close"]) * 100
        for c in valid_candles[-15:]
        if c["close"] > 0
    ]
    average_range_pct = (
        sum(recent_ranges_pct) / len(recent_ranges_pct)
        if recent_ranges_pct else 0.04
    )
    volatility_cap = max(
        0.02,
        min(0.45, average_range_pct * math.sqrt(max(1.0, remaining_minutes)) * 0.75)
    )

    projection_damping = 0.22
    projected_change = (
        rate_per_minute * remaining_minutes * projection_damping
    )
    projected_change = max(
        -volatility_cap,
        min(volatility_cap, projected_change)
    )

    # Kalshi compara promedios BRTI de 60 segundos, no el último tick.
    projected_close_price = float(price) * (1 + projected_change / 100)
    effective_rate = rate_per_minute * projection_damping
    closing_window_fraction = min(1.0, remaining / 60.0)
    average_adjustment_pct = (
        effective_rate * closing_window_fraction * 0.5
    )
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
    target_expected_move_pct = None
    target_z_score = None
    target_normalized = None
    target_direction = "SIN OBJETIVO"

    if valid_price(market_target):
        target_gap_pct = (
            (predicted - float(market_target))
            / float(market_target)
        ) * 100

        # Escala de riesgo basada en el rango BRTI observado y el tiempo restante.
        # La distancia al strike deja de usar un umbral fijo para todos los mercados.
        target_expected_move_pct = max(
            0.015,
            average_range_pct * math.sqrt(max(1.0, remaining_minutes)) * 0.75
        )
        target_z_score = target_gap_pct / target_expected_move_pct
        target_normalized = math.tanh(target_z_score)

        # Combina la posición proyectada frente al strike con la estructura BRTI.
        # El strike pesa más, pero no puede imponer el lado cuando la ventaja es pequeña.
        normalized = (
            0.65 * target_normalized
            + 0.35 * technical_normalized
        )

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
    # El aprendizaje usa el resultado real del contrato Kalshi al cierre
    # (outcome horizon_seconds=-1), no el simple cambio desde el precio de entrada.
    pattern_learning_weight = 0.0
    pattern_learning_used = False
    if (
        pattern_samples >= 12
        and historical_bullish_pct is not None
        and historical_bearish_pct is not None
    ):
        historical_bias = max(
            -1.0,
            min(1.0, (
                float(historical_bullish_pct)
                - float(historical_bearish_pct)
            ) / 100.0)
        )
        # Exige una ventaja histórica mínima de 10 puntos porcentuales.
        # Cada muestra representa un contrato distinto ya resuelto por BRTI.
        if abs(historical_bias) >= 0.10:
            # Aprendizaje prudente: empieza con 4% a partir de 12 contratos
            # y sube gradualmente hasta un máximo de 8%; nunca domina la señal.
            sample_reliability = min(
                1.0,
                max(0.0, (pattern_samples - 12) / 28.0)
            )
            pattern_learning_weight = 0.04 + (0.04 * sample_reliability)
            normalized = (
                normalized * (1.0 - pattern_learning_weight)
                + historical_bias * pattern_learning_weight
            )
            pattern_learning_used = True

    # El aprendizaje histórico puede modular la confianza, pero no invertir
    # el lado del contrato cuando la proyección ya está claramente sobre/bajo
    # el objetivo vigente de Kalshi.
    target_side_locked = bool(
        valid_price(market_target)
        and target_normalized is not None
        and abs(target_normalized) >= 0.90
    )
    if target_side_locked:
        normalized = math.copysign(abs(normalized), target_gap_pct)

    if stabilize:
        normalized, direction = stabilize_direction(
            normalized,
            remaining
        )
    else:
        direction = (
            "ALCISTA" if normalized >= 0.14
            else "BAJISTA" if normalized <= -0.14
            else "INDECISO"
        )

    raw_bullish = max(
        20,
        min(80, round(50 + normalized * 30, 1))
    )
    raw_bearish = round(100 - raw_bullish, 1)
    bullish, bearish, probability_calibrated, probability_calibration_samples, probability_calibration_method = calibrate_directional_scores(
        raw_bullish, raw_bearish
    )

    # La dirección respeta el umbral y la estabilización anteriores.
    # Los porcentajes pueden mostrar una inclinación sin convertirla en señal firme.

    # --------------------------------------
    # RESULTADO
    # --------------------------------------

    return {
        "direction": direction,
        "predicted_price": round(predicted, 2),
        "bullish_score": bullish,
        "bearish_score": bearish,
        "raw_bullish_score": raw_bullish,
        "raw_bearish_score": raw_bearish,
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
        "target_expected_move_pct": (
            round(target_expected_move_pct, 5)
            if target_expected_move_pct is not None else None
        ),
        "target_z_score": (
            round(target_z_score, 4)
            if target_z_score is not None else None
        ),
        "target_direction": target_direction,
        "technical_score": round(technical_normalized, 4),
        "probability_calibrated": probability_calibrated,
        "probability_calibration_samples": probability_calibration_samples,
        "probability_calibration_method": probability_calibration_method,
        "pattern_samples": pattern_samples,
        "pattern_historical_bullish_pct": historical_bullish_pct,
        "pattern_historical_bearish_pct": historical_bearish_pct,
        "pattern_learning_used": pattern_learning_used,
        "pattern_learning_weight_pct": round(pattern_learning_weight * 100, 2),
        "pattern_learning_source": "KALSHI_CONTRACT_CLOSE_BRTI_AVERAGE",
        "target_side_locked": target_side_locked,
        "contract_yes_probability": bullish if valid_price(market_target) else None,
        "contract_no_probability": bearish if valid_price(market_target) else None,
        "direction_method": (
            "BRTI_PRIMARY_KALSHI_15M"
            if valid_price(market_target)
            else "BRTI_PRIMARY_NO_TARGET"
        ),
        "prediction_data_quality": candle_data_quality,
        "contiguous_candles_used": len(valid_candles),
        "candle_gap_detected": candle_gap_detected
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
    """Extrae el precio spot BRTI del mensaje oficial de Kalshi.

    Kalshi entrega el frame CF Benchmarks original en `data` como JSON
    serializado y, por separado, avg_60s_data. Se prioriza el valor spot
    original; el promedio de 60s queda como respaldo, nunca como primera
    opción para el precio instantáneo.
    """

    def parse_json(value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (ValueError, TypeError):
                return None
        return value

    def positive_number(value):
        try:
            number = float(value)
            return number if math.isfinite(number) and number > 0 else None
        except (TypeError, ValueError, OverflowError):
            return None

    def raw_spot(node):
        node = parse_json(node)
        if isinstance(node, dict):
            for key in ("value_usd", "value", "index_value", "price"):
                number = positive_number(node.get(key))
                if number is not None:
                    return number
            for key in ("data", "payload", "msg", "result"):
                if key in node:
                    number = raw_spot(node[key])
                    if number is not None:
                        return number
        elif isinstance(node, list):
            for item in node:
                number = raw_spot(item)
                if number is not None:
                    return number
        return None

    def raw_timestamp(node):
        node = parse_json(node)
        if isinstance(node, dict):
            for key in ("source_ts_ms", "ts_ms", "timestamp", "time", "received_at"):
                value = node.get(key)
                if value is not None:
                    return value
            for key in ("data", "payload", "msg", "result"):
                if key in node:
                    value = raw_timestamp(node[key])
                    if value is not None:
                        return value
        elif isinstance(node, list):
            for item in node:
                value = raw_timestamp(item)
                if value is not None:
                    return value
        return None

    data = parse_json(data)

    if isinstance(data, list):
        for item in data:
            result = find_brti_value(item)
            if result is not None:
                return result
        return None

    if not isinstance(data, dict):
        return None

    index = data.get("index_id") or data.get("ticker") or data.get("id")
    timestamp = (
        data.get("source_ts_ms")
        or data.get("ts_ms")
        or data.get("time")
        or data.get("timestamp")
        or data.get("received_at")
    )

    if index == INDEX_ID:
        # 1) Precio spot del frame original de CF Benchmarks.
        raw_data = data.get("data")
        spot = raw_spot(raw_data)
        if spot is not None:
            return spot, raw_timestamp(raw_data) or timestamp

        # 2) Compatibilidad con variantes que exponen el spot directamente.
        for key in ("value_usd", "value", "index_value", "price"):
            spot = positive_number(data.get(key))
            if spot is not None:
                return spot, timestamp

        # 3) Último respaldo: promedio móvil oficial de 60 segundos.
        average = data.get("avg_60s_data")
        if isinstance(average, dict):
            value = positive_number(average.get("value"))
            if value is not None:
                average_ts = (
                    average.get("window_end_ts_exclusive")
                    or average.get("window_start_ts_ms")
                    or timestamp
                )
                return value, average_ts

    # Algunos mensajes envuelven el payload dentro de msg/data/payload.
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
        _brti_feed_diag["ws_open_count"] += 1
        _brti_feed_diag["last_ws_open_ts"] = time.time()

    print(
        "KALSHI: SUSCRIPCIÓN BRTI ENVIADA",
        flush=True
    )


def kalshi_message(ws, message):

    global kalshi_error

    try:

        with lock:
            _brti_feed_diag["ws_message_count"] += 1
            _brti_feed_diag["last_ws_message_ts"] = time.time()

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
                _brti_feed_diag["brti_parsed_count"] += 1
                _brti_feed_diag["last_brti_parsed_ts"] = time.time()
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
        market_intelligence.start_worker()
        
        def init_pattern_database():
            while not pattern_memory.init_db():
                time.sleep(30)

            # Recupera minutos que faltan usando solo ticks BRTI reales
            # persistidos en los últimas 72 horas; no inventa precios.
            rebuilt = pattern_memory.rebuild_brti_candles_from_ticks()
            print(
                "BRTI: MINUTOS AUSENTES RECONSTRUIDOS DESDE TICKS REALES",
                rebuilt,
                flush=True
            )

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


            # Recupera también muestras BRTI de 5 segundos. Esto permite
            # calcular ventanas reales de 1 y 3 minutos tras un reinicio,
            # sin inventar ticks ni rellenar huecos con precios sintéticos.
            restored_ticks = pattern_memory.get_brti_ticks(MAX_TICKS)
            if restored_ticks:
                with lock:
                    merged_ticks = {
                        float(ts): (float(ts), float(value))
                        for ts, value in restored_ticks
                    }
                    for ts, value in brti_ticks:
                        merged_ticks[float(ts)] = (float(ts), float(value))
                    brti_ticks.clear()
                    brti_ticks.extend(
                        sorted(
                            merged_ticks.values(),
                            key=lambda row: row[0]
                        )[-MAX_TICKS:]
                    )
                print(
                    "BRTI: HISTORIAL DE TICKS RESTAURADO",
                    len(restored_ticks),
                    "muestras",
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
        target=kalshi_orderbook_worker,
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
    elapsed = current_ts - reference[0]
    # Acepta irregularidad moderada del feed sin comparar ventanas
    # demasiado cortas ni extrapolar desde referencias demasiado antiguas.
    if elapsed < seconds * 0.65 or elapsed > seconds * 1.50:
        return None
    observed_change = ((current_price / reference[1]) - 1.0) * 100.0
    return observed_change * (float(seconds) / elapsed)


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
    contract_active=False, ticker=None, close_ts=None,
    validation=None
):
    """Proyección corta separada del pronóstico principal al cierre de 15 minutos."""
    now = time.time()
    validation = validation or get_entry_validation()
    seconds_left = max(0, int(float(close_ts) - now)) if close_ts else 0
    if not close_ts or seconds_left <= 0:
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
    strength = float(indicators.get("technical_strength", 0) or 0)
    ema9 = indicators.get("ema9")
    ema21 = indicators.get("ema21")
    rsi = indicators.get("rsi")

    def agrees(value):
        if value is None or direction not in ("ALCISTA", "BAJISTA"):
            return False
        return value > 0.001 if direction == "ALCISTA" else value < -0.001

    align_1m = agrees(change_1m)
    align_3m = agrees(change_3m)
    opposite_1m = change_1m is not None and (
        change_1m < -0.012 if direction == "ALCISTA" else change_1m > 0.012
    )
    opposite_3m = change_3m is not None and (
        change_3m < -0.012 if direction == "ALCISTA" else change_3m > 0.012
    )
    ema_aligned = (
        ema9 is not None and ema21 is not None
        and (float(ema9) > float(ema21) if direction == "ALCISTA"
             else float(ema9) < float(ema21))
    ) if direction in ("ALCISTA", "BAJISTA") else False
    rsi_blocked = (
        rsi is not None and (
            (direction == "ALCISTA" and float(rsi) >= 78)
            or (direction == "BAJISTA" and float(rsi) <= 22)
        )
    )

    stage = "OBSERVAR"
    note = "Sin coincidencia suficiente todavía. Espera confirmación en 1 y 3 minutos."
    if not valid_price(price):
        stage = "BRTI SIN DATOS"
        note = "No se emite señal: el índice BRTI no está disponible."
    elif not contract_active:
        stage = "SIN CONTRATO KALSHI ACTIVO"
        note = "El radar mantiene el análisis direccional, pero no hay un contrato KXBTC15M vigente. No se confirma una entrada."
    elif seconds_left < 120:
        stage = "CIERRE PRÓXIMO · SIN ENTRADA"
        note = "Quedan menos de 2 minutos. Evita una entrada tardía en este contrato."
    elif change_1m is None:
        stage = "RECOPILANDO CONFIRMACIÓN"
        note = "Faltan datos recientes para medir el movimiento de 1 minuto."
    elif direction not in ("ALCISTA", "BAJISTA"):
        stage = "OBSERVAR · SIN DIRECCIÓN"
        note = "La señal principal no tiene dirección definida; no se fuerza una alerta."
    elif opposite_1m or opposite_3m:
        stage = "OBSERVAR · MOVIMIENTOS EN CONFLICTO"
        note = "El movimiento corto contradice la dirección principal. Espera que se estabilice."
    elif side_probability >= 55 and strength >= 20 and align_1m:
        stage = "PREALERTA"
        note = (
            "Posible " + direction + " · " + str(round(side_probability, 1))
            + "% a favor · movimiento de 1 min alineado"
            + " · 3 min " + ("alineado" if align_3m else "todavía sin confirmar")
            + " · EMA " + ("alineadas" if ema_aligned else "sin confirmar")
            + ". PREALERTA temprana; todavía no es entrada confirmada."
        )
        # Confirmación más temprana para contratos de 15 minutos:
        # exige 1m alineado, que 3m no contradiga, EMA alineadas y fuerza/probabilidad
        # moderadas. No espera obligatoriamente a que 3m también esté alineado.
        if (
            side_probability >= 65
            and strength >= 35
            and align_1m
            and align_3m
            and ema_aligned
            and not rsi_blocked
        ):
            stage = "ENTRADA"
            note = (
                "ENTRADA CONFIRMADA " + direction + " · " + str(round(side_probability, 1))
                + "% a favor · fuerza " + str(round(strength))
                + "% · 1 min alineado · 3 min "
                + ("alineado" if align_3m else "sin contradicción")
                + " · EMA 9/21 alineadas"
                + (" · RSI " + str(round(float(rsi), 1)) if rsi is not None else "")
                + ". Comprueba el precio ejecutable y el riesgo antes de decidir."
            )
        elif rsi_blocked:
            note += " RSI muy extendido: no perseguir el movimiento."
        elif not ema_aligned:
            note += " Falta confirmación de EMA 9/21."
        elif strength < 32:
            note += " La fuerza técnica aún es insuficiente para confirmar."
        elif side_probability < 63:
            note += " La probabilidad todavía no alcanza el umbral de confirmación."
    elif side_probability < 55:
        note = "Probabilidad direccional inferior al umbral de prealerta (55%)."
    elif strength < 20:
        note = "Fuerza técnica insuficiente para activar una prealerta."
    elif not align_1m:
        note = "El movimiento de 1 minuto todavía no acompaña la dirección."
    # Bloquear entradas si el historial real aún no demuestra fiabilidad.
    if stage == "ENTRADA" and not bool(validation.get("qualified")):
        stage = "ENTRADA BLOQUEADA · " + str(validation.get("status") or "VALIDACIÓN PENDIENTE")
        note = (
            "NO HAY ENTRADA VALIDADA. Se requieren al menos "
            + str(validation.get("minimum_samples", 30))
            + " contratos resueltos y "
            + str(validation.get("minimum_accuracy_pct", 55.0))
            + "% de acierto. La lectura técnica no basta para autorizar una entrada."
        )

    return {
        "short_change_1m_pct": round(change_1m, 5) if change_1m is not None else None,
        "short_change_3m_pct": round(change_3m, 5) if change_3m is not None else None,
        "short_predicted_price_1m": round(forecast_1m, 2) if forecast_1m is not None else None,
        "short_predicted_price_3m": round(forecast_3m, 2) if forecast_3m is not None else None,
        "guidance_probability": round(side_probability, 1),
        "guidance_strength": round(strength, 1),
        "guidance_1m_aligned": align_1m,
        "guidance_3m_aligned": align_3m,
        "guidance_ema_aligned": ema_aligned,
        "guidance_note": note,
        "entry_validation": validation,
        "short_direction": direction if direction in ("ALCISTA", "BAJISTA") else "INDECISO",
        "entry_stage": stage,
        "entry_direction": direction if stage in ("PREALERTA", "ENTRADA") else None,
        "entry_ticker": ticker,
        "entry_seconds_remaining": seconds_left,
        "signal_stage": (
            stage + " · " + direction + " · " + str(round(side_probability, 1)) + "%"
            if stage in ("PREALERTA", "ENTRADA") else stage
        )
    }


def update_guidance_alert(guidance, ticker, contract_active=False):
    """Mantiene una entrada ante ruido breve y avisa salida ante deterioro persistente."""
    now = time.time()
    stage = guidance.get("entry_stage")
    direction = guidance.get("entry_direction")
    seconds_left = int(guidance.get("entry_seconds_remaining") or 0)
    alert = None
    with lock:
        state = _guidance_state
        if not contract_active or not ticker or seconds_left <= 0:
            state.update({
                "ticker": None, "stage": "SIN_CONTRATO", "direction": None,
                "last_alert_ts": 0.0, "alert": None, "danger_count": 0,
                "exit_latched_direction": None, "hold_announced": False
            })
            return None
        if state["ticker"] != ticker:
            state.update({
                "ticker": ticker, "stage": "SIN_SEÑAL", "direction": None,
                "last_alert_ts": 0.0, "alert": None, "danger_count": 0
            })

        previous_stage = state["stage"]
        previous_direction = state["direction"]
        short_direction = guidance.get("short_direction")

        # Bloquear reentrada en la misma dirección después de SALIDA, dentro del contrato.
        latched_direction = state.get("exit_latched_direction")
        if stage == "ENTRADA" and direction == latched_direction:
            stage = "SALIDA"
            direction = None
            guidance["entry_stage"] = "SALIDA"
            guidance["entry_direction"] = None
            guidance["signal_stage"] = "SALIDA · DIRECCIÓN BLOQUEADA"
            guidance["guidance_note"] = "La dirección anterior ya tuvo una salida en este contrato. No se permite volver a entrar en la misma dirección."

        # Emitir MANTENER una sola vez después de confirmar y estabilizar la entrada.
        if previous_stage == "ENTRADA" and stage == "ENTRADA" and direction == previous_direction and not state.get("hold_announced"):
            alert = {
                "type": "MANTENER", "level": "AVISO",
                "message": "MANTENER · " + str(previous_direction),
                "detail": "La entrada continúa confirmada y la dirección se mantiene.",
                "ticker": str(ticker), "seconds_remaining": seconds_left,
                "timestamp": now
            }
            state["hold_announced"] = True
        opposite_confirmed = (
            previous_stage == "ENTRADA"
            and short_direction in ("ALCISTA", "BAJISTA")
            and short_direction != previous_direction
            and guidance.get("guidance_1m_aligned")
            and guidance.get("guidance_3m_aligned")
        )

        # No salir por una sola lectura débil: exigir deterioro en 3 actualizaciones.
        # Una reversión confirmada en 1m y 3m sí activa salida inmediata.
        if previous_stage == "ENTRADA":
            if stage == "ENTRADA" and direction == previous_direction:
                state["danger_count"] = 0
            elif opposite_confirmed:
                state["danger_count"] = 3
            else:
                state["danger_count"] = int(state.get("danger_count") or 0) + 1
                if state["danger_count"] < 3:
                    stage = "ENTRADA"
                    direction = previous_direction
                    guidance["entry_stage"] = "ENTRADA"
                    guidance["entry_direction"] = previous_direction
                    guidance["signal_stage"] = "MANTENER · " + str(previous_direction)
                    guidance["guidance_note"] = (
                        "MANTENER: apareció una lectura débil aislada. "
                        "El radar espera confirmación del deterioro antes de indicar salida. "
                        + str(guidance.get("guidance_note") or "")
                    )
                    if state["danger_count"] == 1:
                        alert = {
                            "type": "MANTENER", "level": "AVISO",
                            "message": "MANTENER · " + str(previous_direction),
                            "detail": "La entrada sigue activa; el radar detectó una lectura débil aislada y vigila si se confirma el deterioro.",
                            "ticker": str(ticker), "seconds_remaining": seconds_left,
                            "timestamp": now
                        }

        should_exit = previous_stage == "ENTRADA" and (
            stage != "ENTRADA" or direction != previous_direction
        ) and (opposite_confirmed or int(state.get("danger_count") or 0) >= 3)

        if should_exit:
            alert = {
                "type": "SALIDA", "level": "CRITICA",
                "message": "SALIDA · LA SEÑAL SE DEBILITÓ DE FORMA PERSISTENTE",
                "detail": (
                    str(guidance.get("guidance_note") or
                        "La confirmación dejó de sostenerse.")
                    + " El radar detectó deterioro persistente o reversión confirmada. "
                    + "Quedan " + str(seconds_left) + " segundos; revisa la salida y protege el riesgo."
                ),
                "ticker": str(ticker), "seconds_remaining": seconds_left,
                "timestamp": now
            }
            stage = "SALIDA"
            direction = None
            state["exit_latched_direction"] = previous_direction
            state["hold_announced"] = False
            guidance["entry_stage"] = "SALIDA"
            guidance["entry_direction"] = None
            guidance["signal_stage"] = "SALIDA · PROTEGER"
        elif stage == "ENTRADA" and (
            previous_stage != "ENTRADA" or direction != previous_direction
        ):
            alert = {
                "type": "ENTRADA", "level": "AVISO",
                "message": "ENTRADA CONFIRMADA · " + str(direction),
                "detail": str(guidance.get("guidance_note") or
                    "Dirección principal y movimientos cortos alineados.")
                    + " " + ("Contrato " + str(ticker) if not str(ticker).startswith("RADAR-15M-")
                             else "Ventana predictiva de 15 minutos")
                    + ". Quedan " + str(seconds_left) + " segundos."
                    ,
                "ticker": str(ticker), "seconds_remaining": seconds_left,
                "timestamp": now
            }
            state["danger_count"] = 0
            state["hold_announced"] = False
        elif stage == "PREALERTA" and previous_stage not in ("PREALERTA", "ENTRADA"):
            alert = {
                "type": "PREALERTA", "level": "AVISO",
                "message": "ESPERANDO CONFIRMACIÓN · POSIBLE " + str(direction),
                "detail": str(guidance.get("guidance_note") or
                    "Se está formando una dirección; todavía no es entrada confirmada.")
                    + " " + ("Contrato " + str(ticker) if not str(ticker).startswith("RADAR-15M-")
                             else "Ventana predictiva de 15 minutos")
                    + ". Quedan " + str(seconds_left) + " segundos.",
                "ticker": str(ticker), "seconds_remaining": seconds_left,
                "timestamp": now
            }

        state["stage"] = stage
        state["direction"] = direction
        if alert:
            state["last_alert_ts"] = now
            state["alert"] = alert

        saved_alert = state.get("alert")
        if (
            saved_alert and saved_alert.get("ticker") == ticker
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

    # Encolar el contrato vencido ANTES de limpiar sus datos locales.
    # Si se borran primero ticker/cierre, el bloque de resolución nunca puede
    # detectar el vencimiento y el resultado oficial de Kalshi no se registra.
    if (
        market_ticker
        and market_close is not None
        and float(market_close) > 0
        and now >= float(market_close)
    ):
        with lock:
            if not any(item[0] == market_ticker for item in market_resolution_queue):
                market_resolution_queue.append((market_ticker, float(market_close)))

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
    orderbook = get_cached_kalshi_orderbook(market_ticker) if contract_active else {"available": False, "error": "SIN CONTRATO"}

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

    for queued_ticker, queued_close in list(market_resolution_queue):
        if now < queued_close:
            continue

        # Guardar también la referencia BRTI del cierre para el análisis interno.
        closing_average, closing_samples = calculate_brti_window_average(
            ticks, queued_close - 60, queued_close
        )
        if closing_average is not None and closing_samples >= 45:
            pattern_memory.record_market_close(
                queued_ticker, closing_average, now
            )

        # La precisión oficial se calcula con el resultado YES/NO publicado
        # por Kalshi, no deduciéndolo únicamente de nuestra media BRTI.
        last_check = _market_result_last_check.get(queued_ticker, 0)
        if now - last_check < 10:
            continue
        _market_result_last_check[queued_ticker] = now

        try:
            result_response = session.get(
                f"{KALSHI_REST}/markets/{queued_ticker}",
                timeout=10
            )
            result_response.raise_for_status()
            market_payload = result_response.json()
            settled_market = market_payload.get("market", market_payload)
            official = str(settled_market.get("result") or "").strip().lower()
            status = str(settled_market.get("status") or "").strip().lower()

            if official in ("yes", "no"):
                last_direction = pattern_memory.get_last_contract_signal(
                    queued_ticker
                )
                saved_official = False
                if last_direction in ("ALCISTA", "BAJISTA"):
                    saved_official = pattern_memory.record_official_market_result(
                        queued_ticker,
                        "SI" if official == "yes" else "NO",
                        last_direction,
                        source="kalshi_api"
                    )
                # El resultado oficial liquida la entrada simulada aunque no exista
                # una señal direccional guardada para la métrica de precisión.
                pattern_memory.resolve_paper_trade(
                    queued_ticker, "SI" if official == "yes" else "NO"
                )
                # Si no hubo señal, no inventar evaluación; si falla la BD,
                # mantener el contrato pendiente para reintentar el registro.
                if last_direction not in ("ALCISTA", "BAJISTA") or saved_official:
                    with lock:
                        market_resolution_queue = deque(
                            (item for item in market_resolution_queue if item[0] != queued_ticker),
                            maxlen=20
                        )
                        _market_result_last_check.pop(queued_ticker, None)
            elif status in ("finalized", "determined", "settled") and official:
                # Un mercado anulado no suma como victoria ni derrota simulada.
                if official not in ("yes", "no"):
                    pattern_memory.void_paper_trade(queued_ticker)
                with lock:
                    market_resolution_queue = deque(
                        (item for item in market_resolution_queue if item[0] != queued_ticker),
                        maxlen=20
                    )
                    _market_result_last_check.pop(queued_ticker, None)
        except Exception:
            # Mantenerlo pendiente y reintentar; no marcar resultado por suposición.
            continue

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

    # Si Kalshi no tiene un contrato abierto, el radar sigue proyectando
    # los próximos 15 minutos para aprender y mostrar pronóstico; solo se
    # bloquean las alertas de entrada. Con contrato activo usa su cierre real.
    prediction_horizon = countdown if contract_active else 900
    indicators = calculate_prediction(
        analysis_price,
        ticks,
        prediction_horizon,
        market_target if contract_active else None,
        analysis_candles
    )
    indicators["forecast_horizon_seconds"] = prediction_horizon
    # Modelo GBM independiente: referencia probabilística, sin alterar la señal actual.
    indicators.update(calculate_gbm_baseline(
        analysis_price,
        market_target if contract_active else None,
        prediction_horizon,
        ticks
    ))

    indicators["signal_source"] = signal_source
    indicators["candle_source"] = candle_source
    if not brti_live:
        indicators["direction_method"] = "BRTI_WAITING_FOR_DATA"

    global last_pattern_snapshot_minute, last_contract_snapshot_minute
    snapshot_minute = int(now // 60)
    base_snapshot_slot = (
        str(snapshot_minute) + ":" +
        (market_ticker if contract_active else "GLOBAL")
    )

    if bp is not None and brti_live and valid_price(bp):
        # Pronóstico independiente y fijo a +15 minutos. No usa el strike
        # de Kalshi ni modifica el suavizador de la señal que ve el usuario.
        fifteen_minute_prediction = calculate_prediction(
            analysis_price,
            ticks,
            900,
            None,
            analysis_candles,
            stabilize=False
        )
        fifteen_minute_prediction["forecast_horizon_seconds"] = 900
        fifteen_minute_prediction["signal_source"] = signal_source
        fifteen_minute_prediction["candle_source"] = candle_source

        fifteen_slot = base_snapshot_slot + ":15M"
        save_fifteen = False
        with lock:
            if last_pattern_snapshot_minute != fifteen_slot:
                last_pattern_snapshot_minute = fifteen_slot
                save_fifteen = True

        if save_fifteen:
            pattern_memory.save_snapshot({
                "signal_ts": now,
                "price": bp,
                "market_ticker": market_ticker if contract_active else None,
                # Sin market_close_ts: esta muestra evalúa exclusivamente +900s.
                "market_close_ts": None,
                "snapshot_kind": "15M",
                "direction": fifteen_minute_prediction.get("direction"),
                "bullish_score": fifteen_minute_prediction.get("bullish_score"),
                "bearish_score": fifteen_minute_prediction.get("bearish_score"),
                "predicted_price": fifteen_minute_prediction.get("predicted_price"),
                "ema9": fifteen_minute_prediction.get("ema9"),
                "ema21": fifteen_minute_prediction.get("ema21"),
                "rsi": fifteen_minute_prediction.get("rsi"),
                "momentum_pct": fifteen_minute_prediction.get("momentum_pct"),
                "pattern": fifteen_minute_prediction.get("pattern"),
                "features": {
                    "target_influence": "SIN OBJETIVO KALSHI · PRONÓSTICO FIJO 15M",
                    "technical_strength": fifteen_minute_prediction.get("technical_strength"),
                    "direction_method": fifteen_minute_prediction.get("direction_method"),
                    "signal_source": signal_source,
                    "candle_source": candle_source,
                    "forecast_horizon_seconds": 900,
                    "raw_bullish_score": fifteen_minute_prediction.get("raw_bullish_score"),
                    "raw_bearish_score": fifteen_minute_prediction.get("raw_bearish_score")
                }
            })

        # La señal separada al cierre conserva el comportamiento anterior
        # y sigue siendo la referencia para evaluar el resultado oficial Kalshi.
        if contract_active and market_close is not None:
            contract_slot = base_snapshot_slot + ":CONTRACT"
            save_contract = False
            with lock:
                if last_contract_snapshot_minute != contract_slot:
                    last_contract_snapshot_minute = contract_slot
                    save_contract = True

            if save_contract:
                pattern_memory.save_snapshot({
                    "signal_ts": now,
                    "price": bp,
                    "market_ticker": market_ticker,
                    "market_close_ts": market_close,
                    "snapshot_kind": "CONTRACT",
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
                        "market_target": market_target,
                        "forecast_horizon_seconds": prediction_horizon,
                        "raw_bullish_score": indicators.get("raw_bullish_score"),
                        "raw_bearish_score": indicators.get("raw_bearish_score")
                    }
                })

        
            # Registro independiente del guardado de snapshots visuales.
            # La función limita a una muestra por minuto y contrato.
            pattern_memory.save_model_comparison_snapshot(
                market_ticker=market_ticker,
                signal_ts=now,
                market_close_ts=market_close,
                price=bp,
                market_target=market_target,
                main_direction=indicators.get("direction"),
                gbm_yes_probability=indicators.get("gbm_yes_probability")
            )

        
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
    # Las alertas de entrada/salida también funcionan cuando Kalshi aún
    # no publica un contrato activo: se usa una ventana móvil de 15 minutos.
    guidance_ticker = (
        market_ticker if contract_active
        else "RADAR-15M-" + str(int(now // 900))
    )
    guidance_close = (
        market_close if contract_active
        else (int(now // 900) + 1) * 900
    )
    entry_validation = get_entry_validation()
    guidance = build_forward_guidance(
        ticks, bp if brti_live else None, indicators, candles,
        contract_active=contract_active,
        ticker=market_ticker if contract_active else None,
        close_ts=guidance_close,
        validation=entry_validation
    )
    entry_economics = calculate_entry_economics(
        indicators.get("direction"), indicators.get("bullish_score"),
        indicators.get("bearish_score"), orderbook
    )
    # Una entrada confirmada exige fuentes frescas y alineadas, libro ejecutable
    # y valor esperado neto positivo. Sin estas condiciones, solo se informa.
    if guidance.get("entry_stage") == "ENTRADA":
        entry_blocks = []
        if not coinbase_live or not brti_live:
            entry_blocks.append("Coinbase/BRTI no están ambos en vivo")
        if divergence_pct is None or abs(float(divergence_pct)) >= 0.05:
            entry_blocks.append("Coinbase y BRTI no confirman el mismo precio")
        if not entry_economics.get("available"):
            entry_blocks.append("libro de Kalshi no disponible")
        elif not entry_economics.get("is_positive_estimated_ev"):
            entry_blocks.append("valor esperado neto no positivo")
        if entry_blocks:
            guidance["entry_stage"] = "ENTRADA BLOQUEADA · CONFIRMACIÓN"
            guidance["entry_direction"] = None
            guidance["signal_stage"] = guidance["entry_stage"]
            guidance["guidance_note"] = (
                str(guidance.get("guidance_note") or "")
                + " Entrada bloqueada: " + "; ".join(entry_blocks) + "."
            )
    guidance_alert = update_guidance_alert(
        guidance, market_ticker if contract_active else None,
        contract_active=contract_active
    )
    if guidance_alert is not None:
        alert_data = guidance_alert
    indicators.update(guidance)
    indicators["entry_validation"] = entry_validation
    # Registro idempotente: solo contratos reales con ENTRADA confirmada
    # y precio/fee disponibles. La clave única impide duplicar el mismo ticker.
    if (
        contract_active
        and guidance.get("entry_stage") == "ENTRADA"
        and entry_economics.get("available")
    ):
        pattern_memory.record_paper_trade(
            market_ticker=market_ticker,
            side=entry_economics.get("side"),
            direction=guidance.get("entry_direction"),
            entry_ts=now,
            entry_price=entry_economics.get("entry_ask"),
            estimated_fee=entry_economics.get("estimated_taker_fee"),
            total_cost=entry_economics.get("estimated_total_cost"),
            model_probability_pct=entry_economics.get("model_probability_pct"),
            estimated_ev=entry_economics.get("estimated_net_ev_per_contract")
        )

    return {

        "ok": True,
        "server_time": now,

        "brti_price": (
            round(bp, 2)
            if bp is not None else None
        ),

        "brti_live": brti_live,
        "brti_age": brti_age,
        "brti_samples": len(ticks),
        "brti_tick_history_samples": len(ticks),
        "brti_tick_history_seconds": (
            round(float(ticks[-1][0]) - float(ticks[0][0]), 1)
            if len(ticks) >= 2 else 0
        ),

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
        "manual_position": pattern_memory.get_manual_position(market_ticker if contract_active else None),
        "kalshi_market_close": market_close,
        "seconds_to_close": countdown,
        "kalshi_market_error": market_error,
        "kalshi_orderbook": orderbook,
        "entry_economics": entry_economics,

        "countdown": countdown,

        "brti_average_60s": (
            round(average, 2)
            if average is not None else None
        ),

        "brti_average_samples": average_count,
        "brti_tick_count": len(ticks),
        "signal_source": signal_source,
        "news_intelligence": market_intelligence.snapshot(),
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
        "entry_validation": data.get("entry_validation"),
        "entry_ticker": data.get("entry_ticker"),
        "kalshi_orderbook": data.get("kalshi_orderbook"),
        "entry_economics": data.get("entry_economics"),
        "pattern": data.get("pattern"),
        "pattern_samples": data.get("pattern_samples", 0),
        "pattern_learning_used": data.get("pattern_learning_used", False),
        "pattern_historical_bullish_pct": data.get("pattern_historical_bullish_pct"),
        "pattern_historical_bearish_pct": data.get("pattern_historical_bearish_pct"),
        "pattern_learning_weight_pct": data.get("pattern_learning_weight_pct", 0),
        "pattern_learning_source": data.get("pattern_learning_source"),
        "pattern_learning_min_samples": 12,
        "pattern_learning_status": (
            "ACTIVO" if data.get("pattern_learning_used") else
            "MUESTRAS_INSUFICIENTES" if int(data.get("pattern_samples") or 0) < 12 else
            "SIN_VENTAJA_HISTORICA_SUFICIENTE"
        ),
        "news_intelligence": data.get("news_intelligence"),

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
        "gbm_status": data.get("gbm_status"),
        "gbm_yes_probability": data.get("gbm_yes_probability"),
        "gbm_no_probability": data.get("gbm_no_probability"),
        "gbm_volatility_annualized_pct": data.get("gbm_volatility_annualized_pct"),
        "gbm_volatility_samples": data.get("gbm_volatility_samples"),
        "prediction_data_quality": data.get("prediction_data_quality"),
        "contiguous_candles_used": data.get("contiguous_candles_used"),
        "candle_gap_detected": data.get("candle_gap_detected"),
        "entry_stage": data.get("entry_stage"),
        "signal_source": data["signal_source"],
        "brti_average_60s": data["brti_average_60s"],
        "brti_average_samples": data["brti_average_samples"],
        "direction": data["direction"],
        "predicted_price": data["predicted_price"],
        "kalshi_market_target": data["kalshi_market_target"],
        "kalshi_market_ticker": data["kalshi_market_ticker"],

        "database_status": pattern_memory.database_status(),
        "brti_candle_history": pattern_memory.get_brti_candle_diagnostics(),
        "database_counts": pattern_memory.get_database_counts(),
        "paper_trade_audit": pattern_memory.get_paper_trade_stats(),
        "pattern_learning_diagnostics":
            pattern_memory.get_pattern_learning_diagnostics(),
        "prediction_accuracy": pattern_memory.get_prediction_accuracy(),
        "prediction_accuracy_by_source": pattern_memory.get_prediction_accuracy_by_source(),
        "official_market_accuracy":
            pattern_memory.get_official_market_accuracy(),
        "model_comparison_accuracy":
            pattern_memory.get_model_comparison_accuracy()

    })

@app.route("/intelligence")
def intelligence():
    start_services()
    return jsonify(market_intelligence.snapshot())


@app.route("/stats")
def stats():

    start_services()

    return jsonify({

        "database_status":
            pattern_memory.database_status(),

        "entry_validation":
            get_entry_validation(),

        "database_counts":
            pattern_memory.get_database_counts(),

        "resolution_diagnostics":
            pattern_memory.get_resolution_diagnostics(),

         "prediction_accuracy":
            pattern_memory.get_prediction_accuracy(),

         "prediction_accuracy_by_source":
            pattern_memory.get_prediction_accuracy_by_source(),

         "prediction_accuracy_15m":
            pattern_memory.get_15m_prediction_accuracy(),

         "prediction_accuracy_15m_by_source":
            pattern_memory.get_15m_prediction_accuracy_by_source(),

         "prediction_accuracy_15m_by_contract":
            pattern_memory.get_15m_prediction_accuracy_by_contract(),

         "prediction_15m_error_diagnostics":
            pattern_memory.get_15m_error_diagnostics(),

         "probability_calibration_bands":
            get_cached_probability_calibration(),

         "probability_calibration_ready":
            any(
                int(row.get("samples") or 0) >= 30
                for row in get_cached_probability_calibration()
            ),

         "official_market_accuracy":
            pattern_memory.get_official_market_accuracy(),

         "model_comparison_accuracy":
            pattern_memory.get_model_comparison_accuracy(),

         "paper_trade_audit":
            pattern_memory.get_paper_trade_stats()

    })

@app.route("/candle-diagnostics")
def candle_diagnostics():
    start_services()
    with lock:
        feed_diagnostics = dict(_brti_feed_diag)
        feed_diagnostics["kalshi_connected"] = kalshi_connected
        feed_diagnostics["kalshi_error"] = kalshi_error
    now = time.time()
    for key in (
        "last_ws_open_ts",
        "last_ws_message_ts",
        "last_brti_parsed_ts",
        "last_db_save_ts"
    ):
        ts = feed_diagnostics.get(key)
        feed_diagnostics[key.replace("_ts", "_age_seconds")] = (
            round(max(0, now - ts), 2) if ts is not None else None
        )
    return jsonify({
        "database_status": pattern_memory.database_status(),
        "brti_candle_history": pattern_memory.get_brti_candle_diagnostics(),
        "brti_feed_diagnostics": feed_diagnostics,
        "loaded_in_memory": len(brti_candles),
        "memory_limit": MAX_CANDLES
    })



@app.route("/api/manual-position", methods=["POST"])
def save_manual_position():
    start_services()
    payload = request.get_json(silent=True) or {}
    side = str(payload.get("side") or "").strip().upper()
    ticker = str(payload.get("ticker") or "").strip()
    raw_price = payload.get("entry_price")
    if side not in ("YES", "NO") or not ticker:
        return jsonify({"ok": False, "error": "Selecciona YES o NO para el contrato activo."}), 400
    data = get_data()
    if not data.get("kalshi_market_active") or ticker != str(data.get("kalshi_market_ticker") or ""):
        return jsonify({"ok": False, "error": "El contrato cambió. Actualiza el radar antes de registrar tu posición."}), 409
    entry_price = None
    if raw_price not in (None, ""):
        try:
            entry_price = float(raw_price)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "El precio debe ser un número entre 0 y 1."}), 400
        if not math.isfinite(entry_price) or not 0 < entry_price < 1:
            return jsonify({"ok": False, "error": "El precio debe estar entre 0 y 1 (por ejemplo, 0.54)."}), 400
    saved = pattern_memory.record_manual_position(
        ticker, side, entry_price, time.time()
    )
    if not saved:
        return jsonify({"ok": False, "error": "Ya existe una posición registrada para este contrato o no se pudo guardar."}), 409
    return jsonify({"ok": True, "position": pattern_memory.get_manual_position(ticker)})


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
*{box-sizing:border-box}
html{background:#080b14}
body{margin:0;padding:12px;background:#080b14;color:#f3f5ff;font-family:Arial,sans-serif}
main{width:100%;max-width:760px;margin:0 auto}
.header{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:5px 0 14px}
.logo{font-size:22px;font-weight:900;letter-spacing:1px}.purple{color:#b88aff}
.status{font-size:10px;line-height:1.6;text-align:right;color:#9ba9c4}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:#ff536b;margin-right:4px}.dot.live{background:#00e69a;box-shadow:0 0 9px #00e69a}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;margin-bottom:10px}
.card{min-width:0;background:#111827;border:1px solid #26334b;border-radius:15px;padding:14px;margin-bottom:10px}
.label{color:#93a2bf;font-size:10px;letter-spacing:.4px;font-weight:700;line-height:1.3}
.price{font-size:clamp(18px,4.8vw,28px);font-weight:900;margin:9px 0 5px;overflow-wrap:anywhere}
.brti-card{border:1px solid #278cff;background:linear-gradient(145deg,#10243c,#111827 75%);box-shadow:0 0 14px rgba(39,140,255,.13)}
.kalshi-card{border:1px solid #00d995;background:linear-gradient(145deg,#103329,#111827 75%);box-shadow:0 0 14px rgba(0,217,149,.13)}
.forecast-card{border:1px solid #bd75ff;background:linear-gradient(145deg,#29183f,#111827 75%);box-shadow:0 0 14px rgba(189,117,255,.16)}
.brand-mark{display:inline-flex;align-items:center;justify-content:center;min-width:25px;height:25px;padding:0 7px;border-radius:7px;font-size:12px;font-weight:900;margin-bottom:8px}
.brti-card .brand-mark{color:#55aaff;background:#12365c}.kalshi-card .brand-mark{color:#00e6a0;background:#104b38}.forecast-card .brand-mark{color:#d4a2ff;background:#43245e}
.brti-card .label{color:#69b7ff}.kalshi-card .label{color:#49e9b1}.forecast-card .label{color:#d4a2ff}
.brti-card .price{color:#70bdff}.kalshi-card .price{color:#4af0b3}.forecast-card .price{color:#d5a5ff}
.forecast-card .price.predicted.bullish{color:#00e69a}.forecast-card .price.predicted.bearish{color:#ff536b}.forecast-card .price.predicted.uncertain{color:#f4c95d}
.small{font-size:10px;color:#8d9bb6;line-height:1.5}.green{color:#00e69a}.red{color:#ff536b}.yellow{color:#f4c95d}
.signal{font-size:27px;font-weight:900;margin:10px 0}
.bar{height:12px;display:flex;border-radius:20px;overflow:hidden;background:#29344a;margin:10px 0 8px}.bull{background:#00d995;width:50%}.bear{background:#ff5269;width:50%}
.row{display:flex;justify-content:space-between;align-items:center;gap:8px;min-width:0}
.section-title{color:#a1afca;font-size:11px;font-weight:800;margin-bottom:10px;letter-spacing:.35px}
.chart-wrap{width:100%;height:330px;min-height:220px;position:relative;overflow:hidden}#chart{display:block;width:100%;height:100%;touch-action:pan-y}.kalshi-axis-tag{display:none;position:absolute;right:0;z-index:5;transform:translateY(-50%);width:82px;min-height:31px;box-sizing:border-box;padding:3px 4px;border:1px solid #00d995;border-radius:5px;background:#0b241e;color:#49e9b1;text-align:center;line-height:1.05;pointer-events:none;box-shadow:0 0 8px rgba(0,217,149,.22)}.kalshi-axis-name{font-size:8px;font-weight:900;letter-spacing:.6px}.kalshi-axis-value{font-size:10px;font-weight:900;margin-top:3px;white-space:nowrap}.current-axis-tag{border-color:#45a9ff;background:#10243a;color:#78c5ff;box-shadow:0 0 8px rgba(69,169,255,.24)}
.chart-legend{display:flex;gap:13px;flex-wrap:wrap;margin:8px 0;font-size:10px;color:#9aa9c4}.legend-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px}
.pattern-guide{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:6px 10px;align-items:center;width:100%;box-sizing:border-box;margin:7px 0 9px;padding:12px 14px;border:1px solid #465777;border-left:5px solid #b98aff;border-radius:12px;background:linear-gradient(100deg,rgba(185,138,255,.2),rgba(17,24,39,.96));box-shadow:0 3px 14px rgba(0,0,0,.24)}
.pattern-guide.none{border-left-color:#9aa9c4}.pattern-guide.bull{border-left-color:#00e6a0;background:linear-gradient(100deg,rgba(0,217,149,.17),rgba(17,24,39,.97))}.pattern-guide.bear{border-left-color:#ff5269;background:linear-gradient(100deg,rgba(255,82,105,.17),rgba(17,24,39,.97))}.pattern-name{font-size:16px;font-weight:1000;line-height:1.2;overflow-wrap:anywhere;color:#fff;letter-spacing:.25px}.pattern-state{font-size:10px;font-weight:950;letter-spacing:.45px;color:#ffe078;text-align:right;max-width:145px}.pattern-description{grid-column:1/-1;color:#f0f4ff;font-size:13px;line-height:1.4;font-weight:600}.pattern-history{grid-column:1/-1;color:#b8c6df;font-size:10px;line-height:1.3}.pattern-guide.conflict{border-color:#ffd34e;box-shadow:0 0 0 1px rgba(244,201,93,.22),0 3px 14px rgba(0,0,0,.24)}
button{border:1px solid #35476b;background:#182640;color:white;border-radius:9px;padding:10px 12px;font-size:10px;font-weight:800}button:active{opacity:.7}button:disabled{opacity:.5}
.alert-stack{display:grid;grid-template-columns:1fr;gap:7px;margin:10px 0}
.alert-card{display:flex;align-items:center;gap:11px;min-height:66px;padding:10px 12px;border:1px solid #34425b;border-radius:13px;background:rgba(8,12,28,.88);opacity:.42;filter:saturate(.55);transition:opacity .2s ease,box-shadow .2s ease}
.alert-card.active{opacity:1;filter:none}.alert-card .alert-icon{flex:0 0 34px;width:34px;height:34px;display:grid;place-items:center;border-radius:50%;font-size:20px;font-weight:1000}
.alert-card .alert-copy{min-width:0}.alert-card .alert-title{font-size:13px;font-weight:900;line-height:1.15;margin-bottom:4px}
.alert-card .alert-detail{color:#dce4f4;font-size:11px;line-height:1.3;font-weight:500;overflow-wrap:anywhere}
.alert-card.prealert{border-color:#8e7620}.alert-card.prealert .alert-icon{color:#111;background:#ffd21f}.alert-card.prealert.active{box-shadow:0 0 15px rgba(255,210,31,.18)}
.alert-card.entry{border-color:#167c50}.alert-card.entry .alert-icon{color:#042112;background:#25ed77}.alert-card.entry.active{box-shadow:0 0 15px rgba(37,237,119,.18)}
.alert-card.protect{border-color:#8c3041}.alert-card.protect .alert-icon{color:white;background:#ff405b}.alert-card.protect.active{box-shadow:0 0 15px rgba(255,64,91,.2)}
.technical-card{display:none!important}.footer{text-align:center;color:#66758f;font-size:9px;padding:12px 0 20px}
.signal.green{animation:signalPulseGreen 1.5s ease-in-out infinite}.signal.red{animation:signalPulseRed 1.5s ease-in-out infinite}
@keyframes signalPulseGreen{0%,100%{text-shadow:0 0 4px rgba(0,230,154,.25)}50%{text-shadow:0 0 18px rgba(0,230,154,.75)}}
@keyframes signalPulseRed{0%,100%{text-shadow:0 0 4px rgba(255,83,107,.25)}50%{text-shadow:0 0 18px rgba(255,83,107,.75)}}
@media(max-width:600px){
 html,body{width:100%;height:100%;min-height:0;overflow:hidden}
 body{padding:0}
 main{width:100%;max-width:none;height:100vh;height:100dvh;min-height:0;margin:0;padding:5px;display:grid;grid-template-rows:auto auto auto auto minmax(150px,1fr) auto;gap:5px;overflow:hidden}
 .header{min-height:24px;margin:0;padding:0 2px}.logo{font-size:19px}.status{font-size:8px}
 .grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:5px;margin:0;min-height:0}
 .grid .card{min-width:0;margin:0;padding:7px 5px;border-radius:10px}
 .grid .brand-mark{display:none}.grid .label{font-size:clamp(7px,2vw,9px);line-height:1.12;overflow-wrap:anywhere}
 .grid .price{font-size:clamp(11px,3.5vw,16px);line-height:1.1;margin:6px 0 3px;overflow-wrap:anywhere}
 .grid .small{font-size:7px;line-height:1.15}
 main>section.card{min-height:0;margin:0;padding:7px 9px;border-radius:11px}
 main>section.card:nth-of-type(1) .section-title{font-size:8px;margin:0 0 3px}
 main>section.card:nth-of-type(1) .signal{font-size:clamp(16px,4vw,20px);margin:2px 0 4px;line-height:1.05}
 main>section.card:nth-of-type(1) .price{font-size:clamp(13px,3.5vw,17px);margin:1px 0}
 main>section.card:nth-of-type(1) .bar{height:7px;margin:4px 0}
 main>section.card:nth-of-type(1) .small{font-size:7px;line-height:1.12}
 .alert-stack{min-height:0;margin:0;gap:4px;grid-template-columns:repeat(3,minmax(0,1fr))}
 .alert-card{height:72px;min-height:0;padding:5px 3px;gap:3px;border-radius:9px;border-width:1px;display:flex;flex-direction:column;justify-content:flex-start;align-items:center;text-align:center;overflow:hidden}
 .alert-card.active{transform:none}.alert-card .alert-icon{flex:0 0 20px;width:20px;height:20px;font-size:13px}
 .alert-card .alert-copy{width:100%;min-width:0}.alert-card .alert-title{font-size:clamp(8px,2.3vw,10px);line-height:1.08;margin:0 0 2px;overflow-wrap:anywhere}
 .alert-card .alert-detail{font-size:clamp(7px,1.8vw,8px);line-height:1.1;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
 main>section.card:nth-of-type(3){height:100%;display:flex;flex-direction:column;overflow:hidden;padding:6px}
 main>section.card:nth-of-type(3) .row{flex:0 0 auto}
 main>section.card:nth-of-type(3) .section-title{font-size:8px;margin:0 0 2px}
 main>section.card:nth-of-type(3) .chart-legend{margin:3px 0;gap:7px;font-size:8px}
 main>section.card:nth-of-type(3) .pattern-guide{margin:4px 0 6px;padding:9px 11px;gap:5px 8px;flex:0 0 auto}
 main>section.card:nth-of-type(3) .pattern-name{font-size:clamp(13px,3.7vw,16px);line-height:1.2}
 main>section.card:nth-of-type(3) .pattern-state{font-size:9px;line-height:1.2;max-width:112px}
 main>section.card:nth-of-type(3) .pattern-description{font-size:clamp(11px,3vw,13px);line-height:1.35;font-weight:600}
 main>section.card:nth-of-type(3) .pattern-history{font-size:9px;line-height:1.25}
 main>section.card:nth-of-type(3) .chart-wrap{flex:1 1 auto;min-height:0;height:auto}
 main>section.card:nth-of-type(3) .row:last-of-type{flex:0 0 auto}
 main>section.card:nth-of-type(3)>.small{font-size:7px;flex:0 0 auto}
 main>section.card:nth-of-type(5){padding:5px 8px}
 main>section.card:nth-of-type(5) .signal{font-size:clamp(15px,4vw,18px);margin:1px 0}
 main>section.card:nth-of-type(5) .label,main>section.card:nth-of-type(5) .small{font-size:7px}
 .technical-card,.footer{display:none!important}
 button{padding:7px 8px;font-size:8px}
}
@media(max-width:380px){
 main{padding:4px;gap:4px}
 .grid .card{padding:6px 3px}
 .grid .label{font-size:7px}
 .grid .price{font-size:11px}
 .alert-card{height:66px}
 .alert-card .alert-title{font-size:8px}
 .alert-card .alert-detail{font-size:7px}
}
@media(max-height:720px) and (max-width:600px){
 main{grid-template-rows:auto auto auto auto minmax(120px,1fr) auto;gap:3px;padding:4px}
 .header{min-height:21px}
 .grid .card{padding:5px 3px}
 main>section.card{padding:5px 7px}
 .alert-card{height:58px;padding:3px 2px}
 .chart-legend{margin:2px 0!important}
}

.radar-alert-panel{border:1px solid #35415b;background:linear-gradient(135deg,rgba(20,27,48,.98),rgba(9,14,29,.98))}
.radar-alert-heading{gap:7px;align-items:center}
.radar-alert-heading button{white-space:nowrap;font-size:9px;padding:6px 7px}
.alert-sound-toggle{border:1px solid #65728b!important;background:#172238!important;color:#dce7f8!important;transition:box-shadow .2s,border-color .2s,background .2s}
.alert-sound-toggle.alerts-enabled{border:1px solid #00e6a0!important;background:#063c2b!important;color:#8bffd0!important;box-shadow:0 0 6px rgba(0,230,160,.8),0 0 16px rgba(0,230,160,.35);animation:radarSoundGlow 1.8s ease-in-out infinite}
@keyframes radarSoundGlow{0%,100%{box-shadow:0 0 5px rgba(0,230,160,.55),0 0 12px rgba(0,230,160,.2)}50%{box-shadow:0 0 10px rgba(0,230,160,1),0 0 20px rgba(0,230,160,.5)}}
.compact-radar-alert{padding:7px 8px!important;margin:3px 0!important}
.compact-radar-alert .radar-alert-heading .section-title{margin:0;font-size:11px}
.compact-radar-alert .radar-alert-heading .small{font-size:8px;line-height:1.15}
.accuracy-mini{display:flex;align-items:center;gap:12px;width:max-content;max-width:100%;margin:5px 0 4px;padding:4px 7px;border:1px solid #34415b;border-radius:7px;background:#101a2b;font-size:8px;font-weight:850;white-space:nowrap}
.accuracy-mini b{font-size:9px;color:#fff;font-variant-numeric:tabular-nums}
.accuracy-mini .accuracy-correct{color:#55e0a6}.accuracy-mini .accuracy-wrong{color:#ff7c8d}
.radar-stage-list{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:5px;margin:6px 0 0}
.radar-stage-item{display:flex;align-items:center;justify-content:center;gap:4px;min-width:0;min-height:31px;padding:5px 3px;border:1px solid #34425b;border-radius:8px;background:#101a2b;color:#aab8ce;font-size:clamp(7px,2.1vw,9px);line-height:1.1;text-align:center;font-weight:850}
.radar-stage-item b{display:grid;place-items:center;flex:0 0 15px;width:15px;height:15px;border-radius:50%;background:#29344a;color:#dce4f4;font-size:9px}
#radarStepEntry.active{border-color:#00d995;background:#103b2b;color:#79ffc0}
#radarStepEntry.active b{background:#00d995;color:#062519}
#radarStepHold.active{border-color:#f4c95d;background:#493914;color:#ffe08a}
#radarStepHold.active b{background:#f4c95d;color:#251900}
#radarStepExit.active{border-color:#ff5269;background:#4a1825;color:#ffb1bf}
#radarStepExit.active b{background:#ff5269;color:#2b0710}
@media(max-width:380px){.radar-alert-heading button{font-size:7px;padding:5px 5px}.accuracy-mini{gap:8px;padding:3px 5px;font-size:7px}.accuracy-mini b{font-size:8px}.radar-stage-list{gap:3px}.radar-stage-item{font-size:7px;padding:4px 2px;gap:2px}}
.compact-pattern{grid-template-columns:minmax(0,1fr) auto;gap:3px 8px;margin:3px 0 5px!important;padding:6px 9px!important;cursor:pointer;min-height:0}
.compact-pattern .pattern-name{font-size:11px!important;line-height:1.15}
.compact-pattern .pattern-state{font-size:8px!important;line-height:1.1}
.compact-pattern .pattern-description,.compact-pattern .pattern-history{display:block;font-size:10px!important;line-height:1.25;margin-top:3px}
.compact-pattern.expanded .pattern-description,.compact-pattern.expanded .pattern-history{display:block;font-size:10px!important;line-height:1.25;margin-top:3px}
@media(max-width:380px){.radar-alert-heading button{font-size:8px;padding:6px 7px}.radar-alert-message{font-size:11px}}


.accuracy-strip{margin-top:8px;padding:8px 10px;border:1px solid #34415b;border-radius:10px;background:linear-gradient(110deg,rgba(19,31,52,.92),rgba(13,18,34,.96))}
.accuracy-head{display:flex;justify-content:space-between;gap:8px;color:#aebbd1;font-size:9px;font-weight:850;letter-spacing:.45px}
.accuracy-main{display:flex;align-items:baseline;gap:8px;margin:3px 0}
.accuracy-main strong{font-size:clamp(20px,5vw,27px);line-height:1;color:#f4f7ff;font-variant-numeric:tabular-nums}
.accuracy-main span{font-size:8px;color:#9eabc2;font-weight:800}
.accuracy-counts{display:flex;gap:12px;flex-wrap:wrap;font-size:9px;font-weight:800}
.accuracy-counts b{font-variant-numeric:tabular-nums;color:#fff}
.accuracy-correct{color:#55e0a6}.accuracy-wrong{color:#ff7c8d}.accuracy-pending{color:#f2ce72}
.accuracy-note{margin-top:4px;color:#8f9bb1;font-size:8px;line-height:1.25}
@media(max-width:600px){.accuracy-strip{margin-top:5px;padding:6px 8px}.accuracy-head{font-size:8px}.accuracy-main strong{font-size:21px}.accuracy-counts{gap:9px;font-size:8px}.accuracy-note{font-size:7px}}


.manual-position{margin-top:14px;padding:12px;border:1px solid #29344b;border-radius:12px;background:#0b1220}
.manual-position-title{font-weight:800;letter-spacing:.5px;margin-bottom:9px;color:#cbd5ef}
.manual-position-buttons{display:flex;gap:10px;margin-bottom:10px}
.manual-position-buttons button{flex:1;min-height:42px;border:1px solid #40506c;border-radius:9px;background:#141f33;color:#f4f6ff;font-weight:800}
.manual-position-buttons button.selected{border-color:#00d995;background:#103b2b;color:#79ffc0;box-shadow:0 0 12px #00d99533}
.manual-entry-price{display:block;width:100%;margin:6px 0 8px;padding:10px;border:1px solid #34415a;border-radius:8px;background:#080d18;color:#fff;font-size:16px}
#manualPositionStatus{line-height:1.5;color:#aab6cc}

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
            KALSHI
        </div>

        <div class="small">PRECIO OBJETIVO DEL CONTRATO</div>
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
    <div class="small" id="newsIntel">INTELIGENCIA EXTERNA · RECOPILANDO</div>
    <div class="small" id="newsHeadlines">Esperando titulares de fuentes públicas.</div>
    <div class="small" id="entryEconomics">Precio ejecutable Kalshi: esperando libro de órdenes</div>

    <div id="accuracyStrip" hidden>
        <span id="accuracyUpdated">CARGANDO...</span>
        <strong id="accuracyPercent">--</strong>
        <span id="accuracyBasis">POR CONTRATO</span>
        <span id="accuracyNote">Se calcula con contratos de 15 minutos ya resueltos.</span>
    </div>

</section>

<section class="card radar-alert-panel compact-radar-alert" id="radarAlertPanel" aria-live="polite">
    <div class="row radar-alert-heading">
        <div>
            <div class="section-title">ALERTAS</div>
            <div class="small" id="radarAlertStatus">BRTI · 15 MIN</div>
        </div>
        <button id="alertsEnableButton" class="alert-sound-toggle" onclick="enableRadarAlerts()" aria-pressed="false">ACTIVAR SONIDO</button>
    </div>
    <div class="accuracy-mini" aria-label="Precisión real: aciertos y errores">
        <span class="accuracy-correct">ACIERTOS <b id="accuracyCorrect">--</b></span>
        <span class="accuracy-wrong">ERRORES <b id="accuracyIncorrect">--</b></span>
    </div>
    <div class="radar-stage-list" aria-label="Estados de alerta">
        <div class="radar-stage-item" id="radarStepEntry"><b>✓</b><span id="radarEntryStepLabel">ENTRADA CONFIRMADA</span></div>
        <div class="radar-stage-item" id="radarStepHold"><b>Ⅱ</b><span>MANTENER</span></div>
        <div class="radar-stage-item" id="radarStepExit"><b>×</b><span>SALIDA</span></div>
    </div>
    <div class="radar-alert-state neutral" id="radarAlertState" hidden>OBSERVAR</div>
    <div class="radar-alert-message" id="radarAlertMessage" hidden></div>
    <div class="radar-alert-detail" id="radarAlertDetail" hidden></div>
    <div class="manual-position" id="manualPosition">
        <div class="small manual-position-title">¿QUÉ COMPRASTE EN ESTE CONTRATO?</div>
        <div class="manual-position-buttons">
            <button type="button" id="manualYes" onclick="registerManualPosition('YES')">COMPRÉ YES</button>
            <button type="button" id="manualNo" onclick="registerManualPosition('NO')">COMPRÉ NO</button>
        </div>
        <div class="small" id="manualPositionStatus">Registra tu posición cuando aparezca una entrada confirmada.</div>
    </div>
</section>


<section class="card">

    <div class="row">

        <div>

            <div class="section-title">
                GRÁFICA BTC/USD
            </div>

            <div class="small">
                COINBASE · Velas reales de 1 minuto · EMA 9 / EMA 21
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

    <div class="pattern-guide none compact-pattern" id="patternGuide" role="button" tabindex="0" aria-expanded="false" onclick="togglePatternGuide()" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();togglePatternGuide()}">
        <div class="pattern-name" id="patternName">PATRÓN: ANALIZANDO VELAS</div>
        <div class="pattern-state" id="patternState">ESPERANDO</div>
        <div class="pattern-description" id="patternDescription">El radar señalará aquí las velas que forman el patrón y explicará qué puede indicar.</div>
        <div class="pattern-history" id="patternHistory">Historial oficial del patrón: recopilando</div>
    </div>

    <div class="chart-wrap">

        <div id="chart" role="img" aria-label="Gráfica de velas BTC/USD con patrones señalados"></div>
        <div class="kalshi-axis-tag" id="kalshiAxisTag"><div class="kalshi-axis-name">KALSHI</div><div class="kalshi-axis-value" id="kalshiAxisValue">--</div></div>
        <div class="kalshi-axis-tag current-axis-tag" id="currentAxisTag"><div class="kalshi-axis-name">PRECIO ACTUAL</div><div class="kalshi-axis-value" id="currentAxisValue">--</div></div>

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
let chartCurrentPrice = null;
let chart = null;
let candleSeries = null;
let ema9Line = null;
let ema21Line = null;
let chartTargetLine = null;
let patternMarkers = [];
let chartFollowLive = true;
let chartInitialized = false;
let chartTimeLookup = new Map();
function realChartTime(displayTime){
    const real=chartTimeLookup.get(Number(displayTime));
    const timestamp=Number(real==null?displayTime:real);
    return new Date(timestamp*1000);
}
function displayChartTime(realTime){
    const target=Number(realTime);
    for(const [display,real] of chartTimeLookup.entries()){
        if(Number(real)===target) return Number(display);
    }
    return target;
}
function formatChartTime(displayTime){
    const date=realChartTime(displayTime);
    return new Intl.DateTimeFormat("es-US",{hour:"2-digit",minute:"2-digit",month:"2-digit",day:"2-digit"}).format(date);
}
let busy = false;
let serverClockOffsetMs = 0;
let contractCloseEpochMs = null;

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
    const safe = Math.max(0, Math.ceil(Number(seconds) || 0));
    const m = Math.floor(safe / 60);
    const s = Math.floor(safe % 60);
    return String(m).padStart(2,"0") + ":" + String(s).padStart(2,"0");
}

function updateContractCountdown(){
    const el = $("countdown");
    if(!el) return;
    if(!Number.isFinite(contractCloseEpochMs)){
        el.textContent = "—";
        return;
    }
    const synchronizedNow = Date.now() + serverClockOffsetMs;
    const secondsLeft = Math.max(0, (contractCloseEpochMs - synchronizedNow) / 1000);
    el.textContent = clock(secondsLeft);
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
            minimumWidth:82,
            autoScale:true,
            scaleMargins:{ top:0.12, bottom:0.12 }
        },
        timeScale:{
            borderColor:"#2a3850",
            timeVisible:true,
            secondsVisible:false,
            rightOffset:5,
            barSpacing:5,
            minBarSpacing:1,
            maxBarSpacing:14,
            lockVisibleTimeRangeOnResize:true,
            tickMarkFormatter:(time)=>formatChartTime(time)
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
            timeFormatter: time => formatChartTime(time),
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
        priceLineColor:"#45a9ff",
        priceLineVisible:true,
        lastValueVisible:false,
        // Incluye el objetivo de Kalshi en el rango vertical para que
        // la línea y su etiqueta no desaparezcan fuera de la escala visible.
        autoscaleInfoProvider: original => {
            const info = original();
            if(!info || !info.priceRange) return info;
            const values = [
                Number(info.priceRange.minValue),
                Number(info.priceRange.maxValue),
                chartTargetValue,
                chartCurrentPrice
            ].filter(value => Number.isFinite(value) && value > 0);
            if(values.length < 2) return info;
            const minValue = Math.min(...values);
            const maxValue = Math.max(...values);
            const padding = Math.max((maxValue-minValue)*0.04, 0.01);
            return {
                ...info,
                priceRange:{
                    ...info.priceRange,
                    minValue:minValue-padding,
                    maxValue:maxValue+padding
                }
            };
        }
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
                updateKalshiAxisTag();
            }
        });
        observer.observe(container);
    } else {
        window.addEventListener("resize", () => {
            if(chart) chart.applyOptions({
                width:Math.max(1,container.clientWidth),
                height:Math.max(180,container.clientHeight)
            });
            updateKalshiAxisTag();
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

    // Comprime solo el eje visual: cada vela conserva su timestamp real en chartTimeLookup.
    // No se crean velas ni se modifica el historial usado por el predictor.
    const gaps = rows.reduce((total,c,i)=>total + (i>0 && c.time-rows[i-1].time>90 ? 1 : 0),0);
    chartTimeLookup = new Map();
    const chartRows = rows.map((c,i)=>{
        const displayTime = rows.length ? rows[0].time + i*60 : c.time;
        chartTimeLookup.set(displayTime,c.time);
        return {...c,time:displayTime};
    });
    const countLabel = $("candleCount");
    if(countLabel){
        countLabel.textContent = rows.length + " velas Coinbase" +
            (gaps ? " · " + gaps + " interrupciones comprimidas" : "");
    }
    if(!chartRows.length){
        candleSeries.setData([]);
        ema9Line.setData([]);
        ema21Line.setData([]);
        return;
    }

    candleSeries.setData(chartRows);
    candleSeries.setMarkers(patternMarkers);

    const closes = chartRows.map(c=>c.close);
    const e9 = emaSeries(closes,9);
    const e21 = emaSeries(closes,21);

    const ema9Data = [];
    const ema21Data = [];
    chartRows.forEach((c,i)=>{
        ema9Data.push({time:c.time,value:e9[i]});
        ema21Data.push({time:c.time,value:e21[i]});
    });
    ema9Line.setData(ema9Data);
    ema21Line.setData(ema21Data);

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
            axisLabelVisible:false,
            title:""
        });
    }

    if(chartFollowLive || !previousRange){
        chart.timeScale().setVisibleLogicalRange({
            from:Math.max(0,chartRows.length-55),
            to:chartRows.length+4
        });
    } else {
        const from = Math.max(-5, Math.min(previousRange.from, chartRows.length-1));
        const to = Math.max(from+1, Math.min(previousRange.to, chartRows.length+8));
        chart.timeScale().setVisibleLogicalRange({from,to});
    }
    updateKalshiAxisTag();
    updateCurrentPriceAxisTag();
}

function updateKalshiAxisTag(){
    const tag=$("kalshiAxisTag");
    const value=$("kalshiAxisValue");
    if(!tag || !value || !chart || !candleSeries || !Number.isFinite(chartTargetValue) || chartTargetValue<=0){
        if(tag) tag.style.display="none";
        return;
    }
    const y=candleSeries.priceToCoordinate(chartTargetValue);
    const height=$("chart")?.clientHeight || 0;
    if(y==null || !Number.isFinite(y) || y<0 || y>height){
        tag.style.display="none";
        return;
    }
    value.textContent=money(chartTargetValue);
    tag.style.top=y+"px";
    tag.style.display="block";
}

function updateCurrentPriceAxisTag(){
    const tag=$("currentAxisTag");
    const value=$("currentAxisValue");
    if(!tag || !value || !chart || !candleSeries || !Number.isFinite(chartCurrentPrice) || chartCurrentPrice<=0){
        if(tag) tag.style.display="none";
        return;
    }
    const y=candleSeries.priceToCoordinate(chartCurrentPrice);
    const height=$("chart")?.clientHeight || 0;
    if(y==null || !Number.isFinite(y) || y<0 || y>height){
        tag.style.display="none";
        return;
    }
    value.textContent=money(chartCurrentPrice);
    tag.style.top=y+"px";
    tag.style.display="block";
}


function describePattern(pattern){
    const key=String(pattern || "SIN PATRÓN CLARO").toUpperCase();
    const lessons={
        "ENVOLVENTE ALCISTA":{
            direction:"bull",bars:2,label:"Envolvente alcista",
            text:"La vela verde cubre el cuerpo de la vela roja anterior. Puede mostrar que los compradores están ganando fuerza.",
            next:"Comprueba que aparezca después de una caída y que la siguiente vela cierre al alza. Sin confirmación puede ser una falsa señal."
        },
        "ENVOLVENTE BAJISTA":{
            direction:"bear",bars:2,label:"Envolvente bajista",
            text:"La vela roja cubre el cuerpo de la vela verde anterior. Puede mostrar que los vendedores están tomando el control.",
            next:"Busca que aparezca después de una subida y que la siguiente vela siga cerrando a la baja."
        },
        "POSIBLE MARTILLO":{
            direction:"bull",bars:1,label:"GIRO ALZA",
            text:"Tiene una mecha inferior larga y un cuerpo pequeño. El precio cayó, pero los compradores lo empujaron de nuevo hacia arriba.",
            next:"Es más relevante después de una caída. Espera una vela posterior alcista; aislado no confirma un giro."
        },
        "POSIBLE ESTRELLA FUGAZ":{
            direction:"bear",bars:1,label:"Estrella fugaz",
            text:"Tiene una mecha superior larga y un cuerpo pequeño. El precio subió, pero encontró vendedores en la zona alta.",
            next:"Es más relevante después de una subida. Espera una vela posterior bajista para confirmar el rechazo."
        },
        "DOJI":{
            direction:"none",bars:1,label:"Doji · indecisión",
            text:"La apertura y el cierre son casi iguales. Compradores y vendedores terminaron prácticamente empatados.",
            next:"No elijas dirección solo por el Doji. Observa si la siguiente vela rompe su máximo o su mínimo."
        },
        "TRES VELAS ALCISTAS":{
            direction:"bull",bars:3,label:"Tres velas al alza",
            text:"Tres velas verdes consecutivas con cierres cada vez más altos. Reflejan impulso comprador.",
            next:"Comprueba que los cuerpos mantengan fuerza y que no estén chocando contra una resistencia cercana."
        },
        "TRES VELAS BAJISTAS":{
            direction:"bear",bars:3,label:"Tres velas a la baja",
            text:"Tres velas rojas consecutivas con cierres cada vez más bajos. Reflejan impulso vendedor.",
            next:"Comprueba que los cierres sigan descendiendo y que no haya un soporte cercano frenando la caída."
        },
        "RUPTURA ALCISTA DE RANGO":{
            direction:"bull",bars:1,label:"Ruptura al alza",
            text:"La vela cerró por encima del máximo de las cinco velas anteriores. El precio intenta salir del rango hacia arriba.",
            next:"Vigila que el precio permanezca por encima del antiguo máximo. Si vuelve al rango, la ruptura puede haber fallado."
        },
        "RUPTURA BAJISTA DE RANGO":{
            direction:"bear",bars:1,label:"Ruptura a la baja",
            text:"La vela cerró por debajo del mínimo de las cinco velas anteriores. El precio intenta salir del rango hacia abajo.",
            next:"Vigila que el precio permanezca por debajo del antiguo mínimo. Si recupera el rango, la ruptura puede haber fallado."
        },
        "ESTRUCTURA ALCISTA · HH/HL":{
            direction:"bull",bars:3,label:"Tendencia al alza",
            text:"Los máximos y mínimos van subiendo. HH significa máximo más alto y HL significa mínimo más alto.",
            next:"La estructura se mantiene mientras los mínimos relevantes sigan subiendo. Una pérdida del último mínimo puede debilitarla."
        },
        "ESTRUCTURA BAJISTA · LH/LL":{
            direction:"bear",bars:3,label:"Tendencia a la baja",
            text:"Los máximos y mínimos van bajando. LH significa máximo más bajo y LL significa mínimo más bajo.",
            next:"La estructura se mantiene mientras los máximos relevantes sigan bajando. Superar el último máximo puede debilitarla."
        },
        "VELA INTERNA · COMPRESIÓN":{
            direction:"none",bars:2,label:"Compresión · pausa",
            text:"La última vela quedó dentro del máximo y el mínimo de la vela anterior. El movimiento se está comprimiendo.",
            next:"Marca el máximo y el mínimo de la vela grande. La ruptura confirmada de uno de esos límites puede mostrar el siguiente movimiento."
        }
    };
    return lessons[key] || {
        direction:"none",bars:0,label:"",
        text:"Todavía no hay una figura reconocible con suficiente claridad.",
        next:"Sigue observando las velas; no fuerces una interpretación cuando el patrón no está claro."
    };
}
function togglePatternGuide(){
    const guide=$("patternGuide");
    if(!guide) return;
    const expanded=guide.classList.toggle("expanded");
    guide.setAttribute("aria-expanded",expanded?"true":"false");
}

function updatePatternGuide(pattern, candleRows, mainDirection, samples){
    const guide=$("patternGuide");
    const name=$("patternName");
    const state=$("patternState");
    const description=$("patternDescription");
    if(!guide || !name || !state || !description) return;

    const key=String(pattern || "SIN PATRÓN CLARO").toUpperCase();
    const lesson=describePattern(key);
    const isNone=lesson.bars===0;
    const wasExpanded=guide.classList.contains("expanded");
    guide.className="pattern-guide compact-pattern "+(isNone?"none":lesson.direction)+(wasExpanded?" expanded":"");
    name.textContent=isNone?"PATRÓN: SIN PATRÓN CLARO":key;
    description.textContent=lesson.text+(lesson.next?" Qué observar: "+lesson.next:"");
    const history=$("patternHistory");
    const sampleCount=Math.max(0,Number(samples)||0);
    history.textContent="Muestras oficiales: "+sampleCount+" / 20 para habilitar influencia estadística.";
    const conflicts=(lesson.direction==="bull" && mainDirection==="BAJISTA") ||
        (lesson.direction==="bear" && mainDirection==="ALCISTA");
    if(conflicts){
        guide.classList.add("conflict");
        state.textContent="CONTRADICE LA DIRECCIÓN PRINCIPAL";
    } else {
        guide.classList.remove("conflict");
    }

    const rows=Array.isArray(candleRows)?candleRows:[];
    const latest=rows.length?Number(rows[rows.length-1].time):0;
    const currentMinute=Math.floor(Date.now()/60000)*60;
    const forming=latest>0 && latest>=currentMinute;
    if(!conflicts) state.textContent=isNone?"OBSERVANDO":(forming?"EN FORMACIÓN":"VELA CERRADA");

    patternMarkers=[];
    if(!isNone && rows.length){
        // Una sola etiqueta breve y en español sobre la última vela del patrón.
        // Así se identifica la figura sin llenar ni tapar la gráfica.
        const c=rows[rows.length-1];
        const bullish=lesson.direction==="bull";
        const bearish=lesson.direction==="bear";
        patternMarkers.push({
            time:displayChartTime(c.time),
            position:"aboveBar",
            color:bullish?"#00d995":(bearish?"#ff5269":"#f4c95d"),
            shape:bullish?"arrowUp":(bearish?"arrowDown":"circle"),
            text:lesson.label
        });
    }
    if(chart && candleSeries) candleSeries.setMarkers(patternMarkers);
}

function goLive(){
    chartFollowLive = true;
    if(chart && candles.length){
        chart.timeScale().scrollToRealTime();
    }
}

// ==========================================
// AVISOS DEL RADAR · NOTIFICACIÓN, SONIDO Y VIBRACIÓN
// ==========================================
let radarAudioContext = null;
let radarAlertsEnabled = false;
let lastRadarAlertKey = "";
let lastExitTicker = null;

function paintRadarAlertToggle(){
    const button = $("alertsEnableButton");
    const status = $("radarAlertStatus");
    if(button){
        button.textContent = radarAlertsEnabled ? "SONIDO ACTIVADO" : "ACTIVAR SONIDO";
        button.classList.toggle("alerts-enabled",radarAlertsEnabled);
        button.setAttribute("aria-pressed",radarAlertsEnabled ? "true" : "false");
    }
    if(status){
        status.textContent = radarAlertsEnabled
            ? "SONIDO ENCENDIDO · avisos de entrada y salida"
            : "SONIDO APAGADO · pulsa para activar";
    }
}

async function enableRadarAlerts(){
    if(radarAlertsEnabled){
        radarAlertsEnabled = false;
        paintRadarAlertToggle();
        return;
    }
    try{
        const AudioContextClass = window.AudioContext || window.webkitAudioContext;
        if(!AudioContextClass) throw new Error("Este navegador no permite sonido web.");
        radarAudioContext = radarAudioContext || new AudioContextClass();
        if(radarAudioContext.state === "suspended") await radarAudioContext.resume();
        if(radarAudioContext.state !== "running") throw new Error("Android no habilitó el audio. Vuelve a pulsar el botón.");
        radarAlertsEnabled = true;
        paintRadarAlertToggle();
        playRadarTone("entry");
        if("Notification" in window && Notification.permission === "default"){
            const permission = await Notification.requestPermission();
            if(permission !== "granted"){
                const status = $("radarAlertStatus");
                if(status) status.textContent = "SONIDO ENCENDIDO · permite notificaciones para avisos del sistema";
            }
        }
    }catch(error){
        radarAlertsEnabled = false;
        paintRadarAlertToggle();
        const status = $("radarAlertStatus");
        if(status) status.textContent = error.message || "No se pudo activar el sonido.";
    }
}

function playRadarTone(kind){
    if(!radarAlertsEnabled || !radarAudioContext) return;
    try{
        const now = radarAudioContext.currentTime;
        const frequencies = kind === "exit" ? [330,247] : kind === "hold" ? [784] : [660,988];
        frequencies.forEach((frequency,index)=>{
            const oscillator = radarAudioContext.createOscillator();
            const gain = radarAudioContext.createGain();
            oscillator.frequency.value = frequency;
            oscillator.type = "sine";
            const spacing = kind === "hold" ? 0 : index * 0.16;
            gain.gain.setValueAtTime(0.0001,now + spacing);
            gain.gain.exponentialRampToValueAtTime(0.12,now + spacing + 0.025);
            gain.gain.exponentialRampToValueAtTime(0.0001,now + spacing + 0.14);
            oscillator.connect(gain);
            gain.connect(radarAudioContext.destination);
            oscillator.start(now + spacing);
            oscillator.stop(now + spacing + 0.15);
        });
    }catch(error){ console.warn("No se pudo reproducir el aviso",error); }
}


async function registerManualPosition(side){
    const ticker = window.currentRadarTicker;
    const status = $("manualPositionStatus");
    if(!ticker){
        status.textContent = "No hay un contrato activo de Kalshi para registrar.";
        return;
    }
    const payload = {side, ticker, entry_price: null};
    status.textContent = "Guardando tu posición...";
    try{
        const response = await fetch("/api/manual-position",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify(payload)
        });
        const result = await response.json();
        if(!response.ok || !result.ok) throw new Error(result.error || "No se pudo guardar.");
        $("manualYes").classList.toggle("selected",side==="YES");
        $("manualNo").classList.toggle("selected",side==="NO");
        status.textContent = "TU POSICIÓN REGISTRADA: " + side + " · sin precio de compra";
    }catch(error){
        status.textContent = error.message || "Error al registrar la posición.";
    }
}

function handleRadarAlert(data){
    const alertData = data.alert || {};
    const type = String(alertData.type || "").toUpperCase();
    const stage = String(data.entry_stage || "").toUpperCase();
    const hasFreshEvent = !!alertData.timestamp &&
        (Date.now() / 1000 - Number(alertData.timestamp)) >= 0 &&
        (Date.now() / 1000 - Number(alertData.timestamp)) <= 300;
    const currentTicker = String(data.entry_ticker || alertData.ticker || "");
    if(lastExitTicker && currentTicker && currentTicker !== lastExitTicker){
        lastExitTicker = null;
    }
    if(hasFreshEvent && (type === "SALIDA" || type === "PROTEGER")){
        lastExitTicker = String(alertData.ticker || currentTicker);
    }
    const exitRemainsActive = !!lastExitTicker && !!currentTicker &&
        lastExitTicker === currentTicker;

    // La etapa vigente manda; un aviso viejo no puede contradecirla.
    const stageIsEntry = stage === "ENTRADA";
    const isExit = !stageIsEntry && (exitRemainsActive || (hasFreshEvent && (type === "SALIDA" || type === "PROTEGER")));
    const isNewEntry = hasFreshEvent && type === "ENTRADA" && stageIsEntry;
    const isHold = stageIsEntry && !isNewEntry;
    const isPrealert = stage === "PREALERTA";
    const entryStepLabel = $("radarEntryStepLabel");
    if(entryStepLabel) entryStepLabel.textContent = isPrealert ? "ESPERANDO CONFIRMACIÓN" : "ENTRADA CONFIRMADA";
    const isDirectionChange = hasFreshEvent && type === "CAMBIO_DIRECCION";

    const state = $("radarAlertState");
    const message = $("radarAlertMessage");
    const detail = $("radarAlertDetail");
    const panel = $("radarAlertPanel");
    if(!state || !message || !detail || !panel) return;

    const visibleKind = isExit ? "exit" : isNewEntry ? "entry" :
        isHold ? "hold" : isPrealert ? "prealert" : "neutral";
    state.className = "radar-alert-state " + visibleKind;
    state.textContent = isExit ? "SALIDA" :
        isNewEntry ? "ENTRADA CONFIRMADA" :
        isHold ? "MANTENER · " + (data.entry_direction || data.direction || "") :
        isPrealert ? "PREALERTA · SIN ENTRADA" :
        isDirectionChange ? "CAMBIO DE DIRECCIÓN · OBSERVAR" : "OBSERVAR";
    panel.classList.toggle("has-event", isExit || isNewEntry || isPrealert || isDirectionChange);

    // Entrada y mantener permanecen iluminadas mientras la entrada siga vigente.
    const activeSteps = isExit ? ["exit"] : isNewEntry || isHold ? ["entry","hold"] : isPrealert ? ["entry"] : [];
    ["entry","hold","exit"].forEach(step=>{
        const item = $("radarStep" + step.charAt(0).toUpperCase() + step.slice(1));
        if(item) item.classList.toggle("active", activeSteps.includes(step));
    });

    message.textContent = alertData.message || (
        isNewEntry ? "ENTRADA CONFIRMADA · " + (data.entry_direction || data.direction || "") :
        isHold ? "La entrada sigue confirmada. No es una nueva entrada." :
        isPrealert ? "Hay una posible señal, pero todavía no está confirmada." :
        isExit ? "La señal perdió confirmación. Revisa la salida." :
        isDirectionChange ? "La dirección cambió. Un cambio de dirección por sí solo no confirma una entrada." :
        "Sin entrada confirmada. Espera a que el radar complete sus filtros."
    );
    detail.textContent = alertData.detail || data.guidance_note ||
        "La dirección y el porcentaje por sí solos no confirman una entrada.";

    // Sonido distinto al entrar, mantener y salir; solo una vez por cambio.
    const eventType = isExit ? "SALIDA" : isNewEntry ? "ENTRADA" :
        (isHold && hasFreshEvent && type === "MANTENER") ? "MANTENER" : "";
    if(!eventType || !hasFreshEvent) return;
    const eventKey = [eventType,alertData.ticker || data.entry_ticker || "",alertData.timestamp].join("|");
    if(eventKey === lastRadarAlertKey) return;
    lastRadarAlertKey = eventKey;

    if(radarAlertsEnabled){
        playRadarTone(isExit ? "exit" : eventType === "MANTENER" ? "hold" : "entry");
        if(window.navigator && typeof window.navigator.vibrate === "function"){
            window.navigator.vibrate(isExit ? [180,80,180] : eventType === "MANTENER" ? [70] : [100,60,100]);
        }
    }
    if(radarAlertsEnabled && "Notification" in window && Notification.permission === "granted"){
        try{
            new Notification(isExit ? "BTC Radar · SALIDA" : eventType === "MANTENER" ? "BTC Radar · MANTENER" : "BTC Radar · ENTRADA",{
                body: (alertData.message || "") + (alertData.detail ? "\n" + alertData.detail : ""),
                tag: eventKey,
                renotify: true
            });
        }catch(error){ console.warn("Notificación no disponible",error); }
    }
}

// ==========================================
// ACTUALIZAR INFORMACIÓN DEL RADAR
// ==========================================

async function refreshAccuracy(){
    try{
        const response=await fetch("/stats?t="+Date.now(),{cache:"no-store"});
        if(!response.ok) throw new Error("HTTP "+response.status);
        const stats=await response.json();
        const result=stats.prediction_accuracy_15m_by_contract || {};
        const pct=result.accuracy_pct;
        const correct=Number(result.correct || 0);
        const incorrect=Number(result.incorrect || 0);
        const evaluated=Number(result.predictions_evaluated || 0);
        const percentEl=$("accuracyPercent"), correctEl=$("accuracyCorrect"), incorrectEl=$("accuracyIncorrect");
        if(percentEl) percentEl.textContent=pct==null?"--":Number(pct).toFixed(1)+"%";
        if(correctEl) correctEl.textContent=String(correct);
        if(incorrectEl) incorrectEl.textContent=String(incorrect);
        const basisEl=$("accuracyBasis"), updatedEl=$("accuracyUpdated"), noteEl=$("accuracyNote");
        if(basisEl) basisEl.textContent="POR CONTRATO · "+Number(result.contracts_evaluated || 0)+" EVALUADOS";
        if(updatedEl) updatedEl.textContent="ACTUALIZADO";
        if(noteEl) noteEl.textContent=evaluated>0
            ? "Último pronóstico resuelto por contrato · "+evaluated+" pronósticos direccionales. Sin cambio cuenta como fallo."
            : "Aún no hay suficientes contratos resueltos para calcular la precisión.";
        if(percentEl) percentEl.style.color=pct==null?"#f4f7ff":(Number(pct)>=50?"#55e0a6":"#ff7c8d");
    }catch(error){
        $("accuracyUpdated").textContent="SIN DATOS";
        $("accuracyNote").textContent="No se pudo leer /stats: "+String(error.message || error);
    }
}

function updateRadar(d){

    const measuredOffset = Number(d._client_clock_offset_ms);
    const serverTimeSeconds = Number(d.server_time);
    if(Number.isFinite(measuredOffset)){
        serverClockOffsetMs = measuredOffset;
    } else if(Number.isFinite(serverTimeSeconds) && serverTimeSeconds > 0){
        serverClockOffsetMs = serverTimeSeconds * 1000 - Date.now();
    }
    const closeSeconds = Number(d.kalshi_market_close);
    contractCloseEpochMs = d.kalshi_market_active && Number.isFinite(closeSeconds) && closeSeconds > 0
        ? closeSeconds * 1000
        : null;
    updateContractCountdown();

    $("brtiPrice").textContent =
        money(d.brti_price);
    chartCurrentPrice = Number(d.coinbase_price);
    if(!Number.isFinite(chartCurrentPrice) || chartCurrentPrice<=0) chartCurrentPrice=null;
    updateCurrentPriceAxisTag();

    const direction=d.direction || "RECOPILANDO";
    // La dirección estabilizada puede tardar unos segundos en cambiar.
    // Si la puntuación actual ya apunta al lado contrario, mostrar transición
    // sin alterar la señal interna ni las condiciones de entrada.
    const rawBullish=Number(d.raw_bullish_score ?? d.bullish_score ?? 50);
    const directionTransition=(
        (direction==="ALCISTA" && rawBullish < 50) ||
        (direction==="BAJISTA" && rawBullish > 50)
    );
    const predictedElement=$("predictedPrice");
    predictedElement.textContent=money(d.predicted_price);
    predictedElement.className="price predicted "+(
        directionTransition ? "uncertain" :
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


    $("direction").textContent=directionTransition
        ? "TRANSICIÓN · OBSERVAR"
        : direction;

    $("direction").className="signal "+
        (
            directionTransition ? "yellow" :
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

    const news = d.news_intelligence || {};
    const newsStatus = $("newsIntel");
    const newsHeadlines = $("newsHeadlines");
    if(newsStatus){
        const age = news.age_seconds == null ? "sin actualización" : Math.round(Number(news.age_seconds)/60) + " min";
        newsStatus.textContent = "INTELIGENCIA EXTERNA · " + (news.bias || "SIN DATOS") +
            " · " + Number(news.headline_count || 0) + " TITULARES · " + age +
            " · SOLO CONTEXTO, NO ALTERA LA PREDICCIÓN";
    }
    if(newsHeadlines){
        const items = Array.isArray(news.headlines) ? news.headlines.slice(0,3) : [];
        newsHeadlines.textContent = items.length
            ? items.map(item => "[" + item.source + "] " + item.title).join(" | ")
            : (news.last_error || "Esperando titulares de fuentes públicas.");
    }

    const economics = d.entry_economics || {};
    const book = d.kalshi_orderbook || {};
    if (economics.available) {
        const ev = Number(economics.estimated_net_ev_per_contract);
        const evText = (ev >= 0 ? "+" : "") + "$" + ev.toFixed(4);
        $("entryEconomics").textContent =
            "KALSHI " + economics.side + " · entrada " +
            "$" + Number(economics.entry_ask).toFixed(4) +
            " · tarifa " + "$" + Number(economics.estimated_taker_fee).toFixed(4) +
            " · EV estimado " + evText + "/contrato";
        $("entryEconomics").style.color = ev > 0 ? "#55e0a6" : "#ff7c8d";
    } else {
        $("entryEconomics").textContent =
            "ECONOMÍA KALSHI · " + (economics.reason || book.error || "esperando precio ejecutable");
        $("entryEconomics").style.color = "#aab2c5";
    }

    window.currentRadarTicker = d.kalshi_market_active ? d.kalshi_market_ticker : null;
    const manualPanel = $("manualPosition");
    const manualStatus = $("manualPositionStatus");
    const manualPosition = d.manual_position || null;
    const canRegister = String(d.entry_stage || "").toUpperCase() === "ENTRADA";
    if(manualPanel) manualPanel.hidden = !canRegister && !manualPosition;
    if(manualPosition){
        $("manualYes").classList.toggle("selected",manualPosition.side==="YES");
        $("manualNo").classList.toggle("selected",manualPosition.side==="NO");
        if(manualStatus) manualStatus.textContent = "TU POSICIÓN: " + manualPosition.side +
            " · sin precio de compra" +
            " · " + manualPosition.status;
        if($("manualYes")) $("manualYes").disabled = true;
        if($("manualNo")) $("manualNo").disabled = true;
    }else{
        if($("manualYes")) $("manualYes").disabled = !canRegister;
        if($("manualNo")) $("manualNo").disabled = !canRegister;
        if(manualStatus) manualStatus.textContent = canRegister
            ? "Entrada confirmada. Selecciona YES o NO. No necesitas introducir el precio."
            : window.currentRadarTicker
                ? "Se habilitará cuando el radar confirme la entrada."
                : "Esperando un contrato activo de Kalshi.";
    }
    handleRadarAlert(d);


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
        "Gráfica: velas Coinbase · Confirmación de precio: BRTI";


    $("errors").textContent=
        [
            d.kalshi_error
        ]
        .filter(Boolean)
        .join(" | ");


    drawChart(d.coinbase_candles || []);
    updatePatternGuide(d.pattern, d.coinbase_candles || [], d.direction, d.pattern_samples);

    // El contador se actualiza dentro de drawChart con las velas realmente visibles.

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

        const requestStartedAt = Date.now();
        const response=await fetch(
            "/api?t="+requestStartedAt,
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
        const serverTimeMs = Number(data.server_time) * 1000;
        if(Number.isFinite(serverTimeMs) && serverTimeMs > 0){
            data._client_clock_offset_ms = serverTimeMs - ((requestStartedAt + Date.now()) / 2);
        }

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
refreshAccuracy();

setInterval(
    ()=>refreshData(false),
    1000
);
setInterval(refreshAccuracy,60000);

setInterval(updateContractCountdown,250);

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
