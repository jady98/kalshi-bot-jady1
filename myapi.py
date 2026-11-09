import os
import time
import json
import threading

import websocket
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# BITCOIN RADAR PRO + KALSHI BTC 15M
# Solo lectura de mercado. NO envia ordenes a Kalshi.
# ============================================================

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 200
EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/3.0"})

# Feed en vivo Coinbase Advanced Trade. El REST solo carga el historial inicial;
# el precio y la vela actual se actualizan con WebSocket, no esperando otra vela REST.
WS_URL = "wss://advanced-trade-ws.coinbase.com"
market_lock = threading.RLock()
market_state = {
    "df": None, "price": None, "source": "Coinbase BTC-USD",
    "last_tick": 0.0, "ws_connected": False, "last_error": None,
    "last_trade": 0.0,
}
ws_started = False


def load_history():
    response = session.get(
        "https://api.exchange.coinbase.com/products/BTC-USD/candles",
        params={"granularity": 60}, timeout=12,
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list) or len(rows) < 30:
        raise ValueError("Coinbase devolvio historial insuficiente")
    df = pd.DataFrame(rows, columns=["time", "low", "high", "open", "close", "volume"])
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna().sort_values("time").drop_duplicates("time").tail(CANDLE_LIMIT).reset_index(drop=True)


def _update_live_candle(price, size=0.0, event_time=None):
    now = float(event_time or time.time())
    minute = int(now // 60) * 60
    with market_lock:
        df = market_state.get("df")
        if df is None or df.empty:
            return
        market_state["price"] = float(price)
        market_state["last_tick"] = time.time()
        stamp = pd.to_datetime(minute, unit="s", utc=True)
        last_time = int(df.iloc[-1]["time"].timestamp())
        if minute < last_time:
            return
        if minute > last_time:
            row = {"time": stamp, "open": float(price), "high": float(price),
                   "low": float(price), "close": float(price), "volume": max(0.0, float(size))}
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True).tail(CANDLE_LIMIT).reset_index(drop=True)
        else:
            idx = df.index[-1]
            # Solo modifica la vela en curso. No altera velas ya cerradas.
            df.at[idx, "high"] = max(float(df.at[idx, "high"]), float(price))
            df.at[idx, "low"] = min(float(df.at[idx, "low"]), float(price))
            df.at[idx, "close"] = float(price)
            df.at[idx, "volume"] = float(df.at[idx, "volume"]) + max(0.0, float(size))
        market_state["df"] = df


def _coinbase_message(ws, raw):
    try:
        msg = json.loads(raw)
        channel = msg.get("channel")
        if channel == "subscriptions":
            return
        if channel == "ticker":
            for event in msg.get("events", []):
                for item in event.get("tickers", []):
                    if item.get("product_id") == "BTC-USD":
                        price = float(item["price"])
                        _update_live_candle(price, 0.0, time.time())
        elif channel == "market_trades":
            for event in msg.get("events", []):
                for trade in event.get("trades", []):
                    if trade.get("product_id") != "BTC-USD":
                        continue
                    price = float(trade["price"])
                    size = float(trade.get("size", 0) or 0)
                    try:
                        from datetime import datetime
                        stamp = datetime.fromisoformat(trade["time"].replace("Z", "+00:00")).timestamp()
                    except Exception:
                        stamp = time.time()
                    _update_live_candle(price, size, stamp)
                    with market_lock:
                        market_state["last_trade"] = time.time()
    except Exception as exc:
        with market_lock:
            market_state["last_error"] = str(exc)


def _ws_open(ws):
    with market_lock:
        market_state["ws_connected"] = True
        market_state["last_error"] = None
    # ticker actualiza el precio con cada match; market_trades aporta volumen real.
    ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"}))
    ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "market_trades"}))
    ws.send(json.dumps({"type": "subscribe", "channel": "heartbeats"}))


def _ws_close(ws, *args):
    with market_lock:
        market_state["ws_connected"] = False


def _ws_error(ws, error):
    with market_lock:
        market_state["ws_connected"] = False
        market_state["last_error"] = str(error)


def _websocket_loop():
    while True:
        try:
            client = websocket.WebSocketApp(WS_URL, on_open=_ws_open,
                on_message=_coinbase_message, on_close=_ws_close, on_error=_ws_error)
            client.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            with market_lock:
                market_state["ws_connected"] = False
                market_state["last_error"] = str(exc)
        time.sleep(2)


def _history_loop():
    while True:
        try:
            fresh = load_history()
            with market_lock:
                old = market_state.get("df")
                # Reemplaza historial REST, pero conserva la vela en formación
                # y el precio vivo más reciente para evitar retroceder el gráfico.
                if old is not None and not old.empty:
                    live = old.iloc[-1].copy()
                    fresh_last = int(fresh.iloc[-1]["time"].timestamp())
                    old_last = int(live["time"].timestamp())
                    if old_last >= fresh_last:
                        fresh = fresh[fresh["time"].map(lambda x: int(x.timestamp())) < old_last]
                        fresh = pd.concat([fresh, pd.DataFrame([live])], ignore_index=True)
                market_state["df"] = fresh.tail(CANDLE_LIMIT).reset_index(drop=True)
                if market_state.get("price") is None:
                    market_state["price"] = float(fresh.iloc[-1]["close"])
        except Exception as exc:
            with market_lock:
                market_state["last_error"] = "Historial: " + str(exc)
        time.sleep(180)


def start_market_feeds():
    global ws_started
    if ws_started:
        return
    ws_started = True
    # Carga historial sin bloquear el arranque del servidor.
    threading.Thread(target=_history_loop, daemon=True, name="btc-history").start()
    threading.Thread(target=_websocket_loop, daemon=True, name="coinbase-ws").start()


def get_market_data():
    """Coinbase BTC-USD es la fuente principal; Binance.US queda solo como respaldo."""
    errors = []

    # Fuente principal: Coinbase Exchange, velas BTC-USD de 1 minuto.
    try:
        response = session.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            params={"granularity": 60},
            timeout=10,
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list) or not rows:
            raise ValueError("Coinbase no devolvio velas")
        df = pd.DataFrame(rows, columns=[
            "time", "low", "high", "open", "close", "volume"
        ])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        for column in ["open", "high", "low", "close", "volume"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna().sort_values("time").drop_duplicates("time")
        df = df.tail(CANDLE_LIMIT).reset_index(drop=True)
        if len(df) < 30:
            raise ValueError(f"Coinbase devolvio solo {len(df)} velas validas")
        return df, "Coinbase BTC-USD"
    except Exception as exc:
        errors.append("Coinbase: " + str(exc))

    # Respaldo: mismo mercado BTC, pero de Binance.US. La fuente queda etiquetada.
    try:
        response = session.get(
            "https://api.binance.us/api/v3/klines",
            params={"symbol": SYMBOL, "interval": INTERVAL, "limit": CANDLE_LIMIT},
            timeout=10,
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list) or not rows:
            raise ValueError("Binance.US no devolvio velas")
        df = pd.DataFrame(rows, columns=[
            "time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy",
            "taker_quote", "ignore"
        ])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        for column in ["open", "high", "low", "close", "volume"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna().sort_values("time").tail(CANDLE_LIMIT).reset_index(drop=True)
        if len(df) < 30:
            raise ValueError(f"Binance.US devolvio solo {len(df)} velas validas")
        return df, "Binance.US (respaldo)"
    except Exception as exc:
        errors.append("Binance.US: " + str(exc))

    raise RuntimeError("No se pudieron obtener velas BTC. " + " | ".join(errors))


def calculate_indicators(df):
    df = df.copy()
    close = df["close"]

    df["EMA9"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["EMA21"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    typical = (df["high"] + df["low"] + close) / 3
    cumulative_volume = df["volume"].cumsum().replace(0, np.nan)
    df["VWAP"] = (typical * df["volume"]).cumsum() / cumulative_volume

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, adjust=False,
                        min_periods=RSI_PERIOD).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, adjust=False,
                        min_periods=RSI_PERIOD).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))
    df.loc[(avg_loss == 0) & (avg_gain > 0), "RSI"] = 100
    df.loc[(avg_gain == 0) & (avg_loss > 0), "RSI"] = 0
    df.loc[(avg_gain == 0) & (avg_loss == 0), "RSI"] = 50

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    df["SUPPORT"] = df["low"].rolling(50, min_periods=1).min()
    df["RESISTANCE"] = df["high"].rolling(50, min_periods=1).max()
    return df


def generate_signal(df):
    last = df.iloc[-1]
    if pd.isna(last["RSI"]) or pd.isna(last["VWAP"]):
        return "ESPERAR"

    bullish = (
        last["EMA9"] > last["EMA21"]
        and last["RSI"] > 50
        and last["MACD"] > last["MACD_SIGNAL"]
        and last["close"] > last["VWAP"]
    )
    bearish = (
        last["EMA9"] < last["EMA21"]
        and last["RSI"] < 50
        and last["MACD"] < last["MACD_SIGNAL"]
        and last["close"] < last["VWAP"]
    )
    if bullish:
        return "SUBE"
    if bearish:
        return "BAJA"
    return "ESPERAR"


def forecast_15m(df):
    """
    Proyección heurística para 15 minutos, no predicción garantizada.
    Usa momentum reciente, EMA, RSI, MACD y VWAP; las probabilidades
    son un score normalizado, no probabilidades estadísticas calibradas.
    """
    last = df.iloc[-1]
    close = float(last["close"])
    recent = df.tail(6)
    prior = float(recent.iloc[0]["close"])
    momentum_pct = ((close - prior) / prior) * 100 if prior else 0.0
    ema_gap_pct = ((float(last["EMA9"]) - float(last["EMA21"])) / close) * 100
    vwap_gap_pct = ((close - float(last["VWAP"])) / close) * 100 if pd.notna(last["VWAP"]) else 0.0
    rsi_component = (float(last["RSI"]) - 50.0) / 50.0 if pd.notna(last["RSI"]) else 0.0
    macd_component = float(last["MACD_HIST"]) / close * 100 if close else 0.0

    # Escala conservadora para BTC intraminuto; recorta movimientos extremos.
    score = (
        momentum_pct * 0.34
        + ema_gap_pct * 0.28
        + vwap_gap_pct * 0.18
        + rsi_component * 0.12
        + macd_component * 0.08
    )
    score = max(-1.0, min(1.0, score * 18.0))

    # Indecisión crece cuando el score se acerca a cero.
    bullish_raw = max(0.05, 0.5 + score * 0.42)
    bearish_raw = max(0.05, 0.5 - score * 0.42)
    indecisive_raw = max(0.12, 1.0 - abs(score) * 0.72)
    total = bullish_raw + bearish_raw + indecisive_raw
    bullish = round(100 * bullish_raw / total)
    bearish = round(100 * bearish_raw / total)
    indecisive = 100 - bullish - bearish

    # Estima desplazamiento durante 15 minutos, limitado para evitar extrapolaciones absurdas.
    projected_change_pct = max(-0.75, min(0.75, momentum_pct * 2.0 + ema_gap_pct * 1.5))
    projected_price = close * (1 + projected_change_pct / 100)

    # Fin del bloque de 15 minutos UTC en curso.
    now = datetime.now(timezone.utc)
    minute = (now.minute // 15) * 15
    close_time = now.replace(minute=minute, second=0, microsecond=0)
    close_time = close_time.timestamp() + 900
    seconds_left = max(0, close_time - now.timestamp())

    return {
        "projected_price_15m": float(projected_price),
        "projected_change_pct": float(projected_change_pct),
        "probabilities": {
            "bearish": int(bearish),
            "bullish": int(bullish),
            "indecisive": int(indecisive),
        },
        "forecast_seconds_left": int(seconds_left),
        "forecast_close_ts": int(close_time),
    }


def as_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def kalshi_market_data():
    """Busca el contrato abierto de la serie BTC 15m y devuelve cotizaciones."""
    response = session.get(
        f"{KALSHI_API}/markets",
        params={"series_ticker": KALSHI_SERIES, "status": "open", "limit": 100},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    markets = payload.get("markets", []) or []

    now = datetime.now(timezone.utc)
    candidates = []
    for market in markets:
        close_text = market.get("close_time") or market.get("expected_expiration_time")
        close_dt = None
        if close_text:
            try:
                close_dt = datetime.fromisoformat(close_text.replace("Z", "+00:00"))
                if close_dt.tzinfo is None:
                    close_dt = close_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                close_dt = None

        # Solo mercados que no hayan cerrado, priorizando el que cierra primero.
        if close_dt is None or close_dt > now:
            candidates.append((close_dt or datetime.max.replace(tzinfo=timezone.utc), market))

    if not candidates:
        return {
            "available": False,
            "message": "Kalshi no devolvio un contrato BTC 15M abierto en este momento.",
            "series": KALSHI_SERIES,
        }

    candidates.sort(key=lambda item: item[0])
    market = candidates[0][1]

    yes_bid = as_float(market.get("yes_bid_dollars"))
    yes_ask = as_float(market.get("yes_ask_dollars"))
    no_bid = as_float(market.get("no_bid_dollars"))
    no_ask = as_float(market.get("no_ask_dollars"))

    # Si la API no entrega un ask explicito, se estima desde el bid opuesto.
    # Se identifica como estimado en la respuesta.
    yes_ask_estimated = False
    no_ask_estimated = False
    if yes_ask is None and no_bid is not None:
        yes_ask = 1.0 - no_bid
        yes_ask_estimated = True
    if no_ask is None and yes_bid is not None:
        no_ask = 1.0 - yes_bid
        no_ask_estimated = True

    close_text = market.get("close_time") or market.get("expected_expiration_time")
    close_ts = None
    if close_text:
        try:
            close_dt = datetime.fromisoformat(close_text.replace("Z", "+00:00"))
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=timezone.utc)
            close_ts = int(close_dt.timestamp())
        except ValueError:
            pass

    return {
        "available": True,
        "series": KALSHI_SERIES,
        "ticker": market.get("ticker"),
        "title": market.get("title") or market.get("subtitle") or "Bitcoin 15 minutos",
        "status": market.get("status"),
        "close_ts": close_ts,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "last_price": as_float(market.get("last_price_dollars")),
        "yes_ask_estimated": yes_ask_estimated,
        "no_ask_estimated": no_ask_estimated,
        "floor_strike": as_float(market.get("floor_strike")),
        "cap_strike": as_float(market.get("cap_strike")),
        "rules": market.get("rules_primary") or "",
    }


HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="theme-color" content="#080d18">
<title>Bitcoin Radar PRO | Kalshi BTC 15M</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#070b16;--panel:#101a2b;--panel2:#17263b;--line:#293d59;--muted:#91a5c0;--white:#edf5ff;--green:#00df9a;--red:#ff536b;--yellow:#ffd04a;--blue:#32aaff;--purple:#b58cff}
*{box-sizing:border-box}body{margin:0;padding:11px;background:radial-gradient(ellipse at top,#14213a 0,#070b16 55%);color:var(--white);font-family:Arial,sans-serif}main{max-width:1100px;margin:auto}
header{display:flex;justify-content:space-between;align-items:center;gap:8px;margin:5px 0 12px}h1{font-size:19px;letter-spacing:1.3px;margin:0 0 5px}.sub{font-size:10px;color:var(--green)}
button{border:1px solid #385474;background:#1b2c46;color:white;border-radius:10px;padding:11px 13px;font-weight:bold;cursor:pointer}button:active{transform:scale(.98)}
.panel{background:linear-gradient(145deg,#121f33,#0e1727);border:1px solid var(--line);border-radius:14px;padding:12px;margin-bottom:10px;box-shadow:0 8px 24px #0004}.section{font-size:10px;color:#b9cbe2;letter-spacing:1.2px;margin:1px 0 10px;font-weight:bold}
.top-grid{display:grid;grid-template-columns:1fr 1.2fr 1fr;gap:7px;margin-bottom:10px}.top-card{min-width:0;background:var(--panel2);border:1px solid #314966;border-radius:12px;padding:11px 8px;text-align:center}.top-card small{font-size:9px;color:var(--muted);font-weight:bold;letter-spacing:.5px}.top-card strong{display:block;font-size:clamp(17px,4vw,23px);margin:8px 0 6px;overflow-wrap:anywhere}.top-card.center{border-color:#98752c;background:linear-gradient(145deg,#2a251b,#17263b)}.top-card.bull{border-color:#087b62}.top-card.bear{border-color:#873748}
.bar{height:6px;background:#26364d;border-radius:9px;overflow:hidden}.bar span{display:block;height:100%;width:0%;border-radius:9px;transition:width .25s}.bull .bar span{background:var(--green)}.bear .bar span{background:var(--red)}
.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}.muted{font-size:10px;color:var(--muted)}
.chart-panel{padding:10px}.legend{display:flex;flex-wrap:wrap;gap:12px;font-size:9px;color:#aabbd0;margin-bottom:8px}.legend span{display:flex;align-items:center;gap:5px}.dot{width:7px;height:7px;border-radius:50%}.chart{width:100%;height:310px}.mini-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px}.mini-panel{min-width:0;padding:9px 7px}.mini-panel .section{font-size:9px;margin-bottom:5px}.small-chart{height:105px}.mini-value{font-size:13px;font-weight:bold;margin-bottom:4px}
.bottom-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:10px 0}.bottom-card{background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:12px;text-align:center}.bottom-card small{font-size:9px;color:var(--muted);letter-spacing:.7px}.bottom-card strong{display:block;font-size:22px;margin-top:6px}.trade-buttons{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin:8px 0 12px}.trade-buttons button{font-size:17px;padding:16px 8px;border-radius:12px;letter-spacing:1px}.btn-up{background:linear-gradient(135deg,#087d61,#00bd83);border-color:#00e2a0}.btn-down{background:linear-gradient(135deg,#8d263c,#e3425b);border-color:#ff6378}.note{font-size:10px;line-height:1.5;color:#8293a9}.two{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.quote{background:var(--panel2);border-radius:10px;padding:11px}.quote b{font-size:10px;color:var(--muted)}.quote strong{display:block;font-size:23px;margin-top:6px}.formgrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}label{display:block;font-size:10px;color:var(--muted);margin-bottom:5px}input,select{width:100%;background:#0b1422;color:var(--white);border:1px solid #344963;border-radius:8px;padding:11px;font-size:14px}.actionrow{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.actionrow button{flex:1}table{width:100%;border-collapse:collapse;font-size:12px}td{padding:8px 3px;border-bottom:1px solid #24344b}td:last-child{text-align:right;font-weight:bold}.alertrow{display:flex;gap:9px;align-items:center;padding:8px 0;border-bottom:1px solid #24344b;font-size:12px}.pill{font-size:10px;border-radius:20px;padding:4px 7px;background:#263750;color:#b9cbe0}footer{text-align:center;color:#71839b;font-size:10px;padding:12px}
@media(min-width:760px){body{padding:20px}.chart{height:430px}.small-chart{height:125px}.mini-panel{padding:11px}.top-card{padding:14px}.lower-layout{display:grid;grid-template-columns:1.25fr .9fr;gap:10px}.lower-layout .panel{height:fit-content}}
@media(max-width:370px){body{padding:7px}.top-card{padding:8px 5px}.top-card small{font-size:8px}.top-card strong{font-size:16px}.chart{height:270px}.small-chart{height:90px}.mini-panel{padding:7px 4px}}
</style>
</head>
<body>
<main>
<header><div><h1>BITCOIN RADAR PRO</h1><div class="sub" id="connection">● CONECTANDO DATOS...</div></div><button onclick="updateAll()">ACTUALIZAR</button></header>

<div class="top-grid">
 <div class="top-card bull"><small>▲ ALCISTA</small><strong class="green" id="bullProb">--%</strong><div class="bar"><span id="bullBar"></span></div></div>
 <div class="top-card center"><small>PREDICCIÓN · 15 MIN</small><strong class="yellow" id="projectedPrice">--</strong><small id="projectedChange">PRECIO ESTIMADO</small></div>
 <div class="top-card bear"><small>▼ BAJISTA</small><strong class="red" id="bearProb">--%</strong><div class="bar"><span id="bearBar"></span></div></div>
</div>

<section class="panel chart-panel">
 <div class="section">BTC / USD · VELAS JAPONESAS · 1 MINUTO <span id="mainSignal" class="yellow">· INDECISO</span></div>
 <div class="legend"><span><i class="dot" style="background:#ffd04a"></i>EMA 9</span><span><i class="dot" style="background:#32aaff"></i>EMA 21</span><span><i class="dot" style="background:#b58cff"></i>VWAP</span></div>
 <div id="priceChart" class="chart"></div>
 <p class="note" id="explanation">Esperando confirmación de indicadores.</p>
</section>

<div class="mini-grid">
 <section class="panel mini-panel"><div class="section">RSI 14</div><div class="mini-value purple" id="rsiValue">--</div><div id="rsiChart" class="chart small-chart"></div></section>
 <section class="panel mini-panel"><div class="section">MACD</div><div class="mini-value blue">MACD / SEÑAL</div><div id="macdChart" class="chart small-chart"></div></section>
 <section class="panel mini-panel"><div class="section">VOLUMEN</div><div class="mini-value yellow">BTC</div><div id="volumeChart" class="chart small-chart"></div></section>
</div>

<div class="bottom-grid">
 <div class="bottom-card"><small>TIEMPO RESTANTE · KALSHI 15M</small><strong class="yellow" id="timer">--:--</strong></div>
 <div class="bottom-card"><small>PRECIO ACTUAL BTC/USD</small><strong class="blue" id="btcPrice">--</strong></div>
</div>
<div class="trade-buttons"><button class="btn-up" type="button" onclick="selectDirection('SUBE')">▲ SUBE</button><button class="btn-down" type="button" onclick="selectDirection('BAJA')">▼ BAJA</button></div>
<div class="note" id="directionNote">Los botones son indicadores visuales; no envían órdenes a Kalshi.</div>

<div class="lower-layout">
<div>
 <section class="panel">
  <div class="section">MERCADO KALSHI · BTC 15 MINUTOS</div>
  <div id="marketTitle" style="font-size:13px;font-weight:bold;margin-bottom:8px">Buscando contrato...</div><div class="muted" id="marketTicker">Ticker: --</div>
  <div class="two" style="margin-top:10px"><div class="quote"><b>YES · COMPRA</b><strong class="green" id="yesAsk">--¢</strong><span class="muted" id="yesBid">Bid: --</span></div><div class="quote"><b>NO · COMPRA</b><strong class="red" id="noAsk">--¢</strong><span class="muted" id="noBid">Bid: --</span></div></div>
  <p class="note" id="marketMessage">Los precios se consultan directamente en la API pública de Kalshi.</p><div class="muted" id="strikeInfo"></div>
 </section>
 <section class="panel">
  <div class="section">MI OPERACIÓN · SEGUIMIENTO MANUAL</div>
  <div class="formgrid"><div><label>LADO COMPRADO</label><select id="side"><option value="yes">YES</option><option value="no">NO</option></select></div><div><label>CONTRATOS</label><input id="quantity" type="number" min="1" step="1" value="1"></div><div><label>PRECIO DE ENTRADA (¢)</label><input id="entry" type="number" min="1" max="99" step="1" placeholder="Ej. 52"></div><div><label>OBJETIVO DE SALIDA (¢)</label><input id="target" type="number" min="1" max="99" step="1" placeholder="Ej. 70"></div><div><label>LÍMITE DE PÉRDIDA (¢)</label><input id="stop" type="number" min="1" max="99" step="1" placeholder="Ej. 40"></div><div><label>ALERTA SONORA</label><select id="sound"><option value="on">Activada</option><option value="off">Desactivada</option></select></div></div>
  <div class="actionrow"><button onclick="saveTrade()">GUARDAR OPERACIÓN</button><button onclick="clearTrade()">CERRAR / BORRAR</button></div>
  <table style="margin-top:10px"><tr><td>Precio actual de salida estimado</td><td id="currentExit">--</td></tr><tr><td>Ganancia / pérdida estimada</td><td id="pnl">--</td></tr><tr><td>Estado</td><td id="tradeStatus">Sin operación guardada</td></tr></table>
  <p class="note">Estimación antes de comisiones y deslizamiento. El panel no compra ni vende por ti.</p>
 </section>
</div>
<div>
 <section class="panel"><div class="section">ALERTAS INTELIGENTES</div><div class="alertrow"><span class="pill">SEÑAL</span> Aviso cuando cambia SUBE / BAJA / ESPERAR</div><div class="alertrow"><span class="pill">OBJETIVO</span> Aviso al alcanzar el precio objetivo</div><div class="alertrow"><span class="pill">STOP</span> Aviso al alcanzar el límite de pérdida</div><div class="alertrow"><span class="pill">SALIDA</span> Aviso si se debilita la señal técnica</div><div class="actionrow"><button onclick="enableSound()">ACTIVAR SONIDO</button></div><p class="note" id="alertStatus">Las alertas iguales no se repiten en cada actualización.</p></section>
</div>
</div>
<footer>BTC desde Coinbase (Binance.US solo como respaldo). Kalshi se consulta por su API pública. Precio proyectado y porcentajes heurísticos, no garantías. Coinbase puede diferir del índice BRTI usado para liquidar Kalshi.</footer>
</main>

<script>
const $ = id => document.getElementById(id);
const fmtUSD = n => n == null ? "--" : "$" + Number(n).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2});
const cents = n => n == null ? "--¢" : (Number(n)*100).toFixed(1)+"¢";
const priceOptions = (el,h) => ({
 width:el.clientWidth,height:h,
 layout:{background:{color:"#111a2a"},textColor:"#aebdd0",fontSize:10},
 grid:{vertLines:{color:"#1d2b40"},horzLines:{color:"#1d2b40"}},
 rightPriceScale:{borderColor:"#34445a"},
 timeScale:{borderColor:"#34445a",timeVisible:true,secondsVisible:false,
  rightOffset:3,barSpacing:7,minBarSpacing:2},
 crosshair:{mode:LightweightCharts.CrosshairMode.Normal},
 handleScroll:true,handleScale:true
});
const pc=LightweightCharts.createChart($("priceChart"),priceOptions($("priceChart"),310));
const candle=pc.addCandlestickSeries({upColor:"#00d69a",downColor:"#ff5268",borderUpColor:"#00d69a",borderDownColor:"#ff5268",wickUpColor:"#00d69a",wickDownColor:"#ff5268"});
const e9=pc.addLineSeries({color:"#ffc857",lineWidth:2,title:"EMA 9"});
const e21=pc.addLineSeries({color:"#36a3ff",lineWidth:2,title:"EMA 21"});
const vw=pc.addLineSeries({color:"#c58cff",lineWidth:2,title:"VWAP"});
const vc=LightweightCharts.createChart($("volumeChart"),priceOptions($("volumeChart"),105));
const vol=vc.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:""});
vc.priceScale("").applyOptions({scaleMargins:{top:.1,bottom:0}});
const rc=LightweightCharts.createChart($("rsiChart"),priceOptions($("rsiChart"),105));
const rsi=rc.addLineSeries({color:"#bd8aff",lineWidth:2,title:"RSI"});
[70,50,30].forEach((p,i)=>rsi.createPriceLine({price:p,color:i===0?"#ff5268":i===1?"#71839c":"#00d69a",lineStyle:LightweightCharts.LineStyle.Dashed,title:String(p)}));
rc.priceScale("right").applyOptions({autoScale:false,scaleMargins:{top:.1,bottom:.1}});
const mc=LightweightCharts.createChart($("macdChart"),priceOptions($("macdChart"),105));
const ml=mc.addLineSeries({color:"#36a3ff",lineWidth:2,title:"MACD"});
const ms=mc.addLineSeries({color:"#ffc857",lineWidth:2,title:"Signal"});
const mh=mc.addHistogramSeries({lastValueVisible:false});
ml.createPriceLine({price:0,color:"#71839c",lineStyle:LightweightCharts.LineStyle.Dashed});

let busy=false, initialized=false, btcData=null, marketData=null, lastAlertKey="", alertedConditions={}, lastKalshiFetch=0;
let audioContext=null;
function soundAlert(){
 if($("sound").value==="off")return;
 try{
  const Ctx=window.AudioContext||window.webkitAudioContext;
  if(!Ctx)return;
  audioContext=audioContext||new Ctx();
  if(audioContext.state==="suspended")audioContext.resume();
  const osc=audioContext.createOscillator(), gain=audioContext.createGain();
  osc.connect(gain);gain.connect(audioContext.destination);
  osc.frequency.value=880;gain.gain.value=.08;
  osc.start();osc.stop(audioContext.currentTime+.22);
 }catch(e){}
}
function notifyOnce(key,title,message){
 if(alertedConditions[key])return;
 alertedConditions[key]=true;
 $("alertStatus").textContent=title+": "+message;
 soundAlert();
 if("Notification" in window && Notification.permission==="granted"){
  try{new Notification(title,{body:message});}catch(e){}
 }
}
function enableSound(){
 soundAlert();
 if("Notification" in window && Notification.permission==="default"){
  Notification.requestPermission().then(p=>$("alertStatus").textContent="Permiso de notificaciones: "+p);
 }else{$("alertStatus").textContent="Sonido de prueba activado."}
}
function renderSignal(signal, probabilities){
 const label=signal==="SUBE"?"ALCISTA":signal==="BAJA"?"BAJISTA":"INDECISO";
 $("mainSignal").textContent="· "+label;
 $("mainSignal").className=signal==="SUBE"?"green":signal==="BAJA"?"red":"yellow";
 if(probabilities){
  $("bullProb").textContent=probabilities.bullish+"%";
  $("bearProb").textContent=probabilities.bearish+"%";
  $("bullBar").style.width=probabilities.bullish+"%";
  $("bearBar").style.width=probabilities.bearish+"%";
 }
 if(lastAlertKey && signal!==lastAlertKey){notifyOnce("signal:"+signal,"Cambio de señal","El radar cambió a "+label+".");}
 if(signal!==lastAlertKey){lastAlertKey=signal;}
}
function selectDirection(side){ $("directionNote").textContent="Dirección seleccionada: "+side+". Es una referencia visual, no ejecuta ninguna operación."; }
function parsePriceForSide(side){
 if(!marketData||!marketData.available)return null;
 return side==="yes"?marketData.yes_bid:marketData.no_bid;
}
function bestExitForSide(side){
 if(!marketData||!marketData.available)return null;
 // Para salir de una posición larga se usa el bid del lado comprado.
 return side==="yes"?marketData.yes_bid:marketData.no_bid;
}
function updateTrade(){
 const side=$("side").value, entry=Number($("entry").value), qty=Number($("quantity").value||1);
 const target=Number($("target").value), stop=Number($("stop").value);
 const current=bestExitForSide(side);
 $("currentExit").textContent=current==null?"--":cents(current);
 if(!$("entry").value || !Number.isFinite(entry) || entry<=0 || current==null){
  $("pnl").textContent="--";$("tradeStatus").textContent="Introduce tu entrada real";return;
 }
 const pnl=(current*100-entry)*qty/100;
 $("pnl").textContent=(pnl>=0?"+":"")+pnl.toFixed(2)+" USD aprox.";
 $("pnl").className=pnl>=0?"green":"red";
 $("tradeStatus").textContent="Siguiendo "+qty+" contrato(s) "+side.toUpperCase();
 if(target>0 && current*100>=target){
  notifyOnce("target:"+side+":"+target,"Objetivo alcanzado","El bid de "+side.toUpperCase()+" llegó a "+cents(current)+". Revisa posible salida.");
 }
 if(stop>0 && current*100<=stop){
  notifyOnce("stop:"+side+":"+stop,"Límite de pérdida alcanzado","El bid de "+side.toUpperCase()+" bajó a "+cents(current)+". Revisa posible salida.");
 }
}
function saveTrade(){
 ["side","quantity","entry","target","stop"].forEach(id=>localStorage.setItem("radar_"+id,$(id).value));
 alertedConditions={};
 $("alertStatus").textContent="Operación guardada en este teléfono.";
 updateTrade();
}
function clearTrade(){
 ["entry","target","stop"].forEach(id=>$(id).value="");
 ["side","quantity","entry","target","stop"].forEach(id=>localStorage.removeItem("radar_"+id));
 alertedConditions={};$("pnl").textContent="--";$("tradeStatus").textContent="Sin operación guardada";$("currentExit").textContent="--";
}
["side","quantity","entry","target","stop"].forEach(id=>{const v=localStorage.getItem("radar_"+id);if(v!==null)$(id).value=v;});
function setMarket(m){
 marketData=m;
 if(!m||!m.available){
  $("marketTitle").textContent="Contrato no disponible";
  $("marketTicker").textContent="Serie: "+(m?.series||"KXBTC15M");
  $("yesAsk").textContent="--¢";$("noAsk").textContent="--¢";
  $("yesBid").textContent="Bid: --";$("noBid").textContent="Bid: --";
  $("marketMessage").textContent=m?.message||"No se pudo consultar Kalshi.";
  $("timer").textContent="--:--";updateTrade();return;
 }
 $("marketTitle").textContent=m.title||"Bitcoin 15 minutos";
 $("marketTicker").textContent="Ticker: "+(m.ticker||"--");
 $("yesAsk").textContent=cents(m.yes_ask);
 $("noAsk").textContent=cents(m.no_ask);
 $("yesBid").textContent="Bid: "+cents(m.yes_bid);
 $("noBid").textContent="Bid: "+cents(m.no_bid);
 const estimated=[];
 if(m.yes_ask_estimated)estimated.push("YES ask estimado desde NO bid");
 if(m.no_ask_estimated)estimated.push("NO ask estimado desde YES bid");
 $("marketMessage").textContent=estimated.length?estimated.join(" · "):"Cotizaciones de Kalshi. Los asks pueden estar vacíos si no hay liquidez.";
 $("strikeInfo").textContent=m.floor_strike!=null?"Nivel de referencia del contrato: "+fmtUSD(m.floor_strike):"";
 updateTimer();updateTrade();
}
function updateTimer(){
 if(!marketData||!marketData.close_ts){$("timer").textContent="--:--";return;}
 const seconds=Math.max(0,marketData.close_ts-Math.floor(Date.now()/1000));
 $("timer").textContent=String(Math.floor(seconds/60)).padStart(2,"0")+":"+String(seconds%60).padStart(2,"0");
}
function renderBTC(data){
 btcData=data;
 $("btcPrice").textContent=fmtUSD(data.price);
 $("projectedPrice").textContent=fmtUSD(data.projected_price_15m);
 const change=data.projected_change_pct;
 $("projectedChange").textContent=(change>=0?"+":"")+change.toFixed(3)+"% estimado · "+Math.floor(data.forecast_seconds_left/60)+"m "+String(data.forecast_seconds_left%60).padStart(2,"0")+"s restantes";
 $("projectedChange").className=change>=0?"green":"red";
 $("rsiValue").textContent=data.rsi==null?"--":data.rsi.toFixed(1);
 $("explanation").textContent=data.explanation;
 renderSignal(data.signal,data.probabilities);
 const last=data.candles[data.candles.length-1];
 if(!initialized){
  candle.setData(data.candles.map(x=>({time:x.time,open:x.open,high:x.high,low:x.low,close:x.close})));
  e9.setData(data.candles.map(x=>({time:x.time,value:x.ema9})));
  e21.setData(data.candles.map(x=>({time:x.time,value:x.ema21})));
  vw.setData(data.candles.filter(x=>x.vwap!=null).map(x=>({time:x.time,value:x.vwap})));
  vol.setData(data.candles.map(x=>({time:x.time,value:x.volume,color:x.close>=x.open?"rgba(0,214,154,.55)":"rgba(255,82,104,.55)"})));
  rsi.setData(data.candles.filter(x=>x.rsi!=null).map(x=>({time:x.time,value:x.rsi})));
  ml.setData(data.candles.map(x=>({time:x.time,value:x.macd})));
  ms.setData(data.candles.map(x=>({time:x.time,value:x.macd_signal})));
  mh.setData(data.candles.map(x=>({time:x.time,value:x.macd_hist,color:x.macd_hist>=0?"rgba(0,214,154,.65)":"rgba(255,82,104,.65)"})));
  pc.timeScale().fitContent(); initialized=true;
 }else{
  // update() modifica la vela actual sin reconstruir toda la gráfica ni mover el zoom.
  candle.update({time:last.time,open:last.open,high:last.high,low:last.low,close:last.close});
  e9.update({time:last.time,value:last.ema9}); e21.update({time:last.time,value:last.ema21});
  if(last.vwap!=null)vw.update({time:last.time,value:last.vwap});
  vol.update({time:last.time,value:last.volume,color:last.close>=last.open?"rgba(0,214,154,.55)":"rgba(255,82,104,.55)"});
  if(last.rsi!=null)rsi.update({time:last.time,value:last.rsi});
  ml.update({time:last.time,value:last.macd}); ms.update({time:last.time,value:last.macd_signal});
  mh.update({time:last.time,value:last.macd_hist,color:last.macd_hist>=0?"rgba(0,214,154,.65)":"rgba(255,82,104,.65)"});
 }
}
async function updateAll(){
 if(busy)return;busy=true;$("connection").textContent="● ACTUALIZANDO...";
 try{
  const btcPromise=fetch("/api/data",{cache:"no-store"}).then(r=>r.json().then(d=>{if(!r.ok)throw Error(d.error||"Error BTC");return d;}));
  const kalshiPromise=(Date.now()-lastKalshiFetch>5000)?fetch("/api/kalshi",{cache:"no-store"}).then(r=>r.json()).then(d=>{lastKalshiFetch=Date.now();setMarket(d);}):Promise.resolve(null);
  const btc=await btcPromise; await kalshiPromise;
  renderBTC(btc);
  $("connection").textContent="● BTC "+(btc.feed_status||"CONECTANDO")+" · "+btc.source+" · Kalshi "+(marketData&&marketData.available?"CONECTADO":"CONSULTANDO");
 }catch(e){$("connection").textContent="● ERROR: "+e.message;}
 finally{busy=false;}
}
function resizeCharts(){
 [[pc,"priceChart",310],[vc,"volumeChart",105],[rc,"rsiChart",105],[mc,"macdChart",105]].forEach(([chart,id,h])=>chart.applyOptions({width:$(id).clientWidth,height:h}));
}
window.addEventListener("resize",resizeCharts);
updateAll();
setInterval(updateAll,1000);
setInterval(updateTimer,1000);
</script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/data")
def market_data():
    try:
        start_market_feeds()
        with market_lock:
            raw = market_state.get("df")
            price = market_state.get("price")
            connected = market_state.get("ws_connected", False)
            last_tick = market_state.get("last_tick", 0.0)
            error = market_state.get("last_error")
            source = market_state.get("source", "Coinbase BTC-USD")
            df = raw.copy() if raw is not None else None
        if df is None or len(df) < 30:
            return jsonify({"error": "Cargando historial inicial de Coinbase. Espera unos segundos."}), 503
        # El ticker WebSocket es la referencia viva del precio, no el close REST.
        if price is not None:
            df.loc[df.index[-1], "close"] = float(price)
            df.loc[df.index[-1], "high"] = max(float(df.iloc[-1]["high"]), float(price))
            df.loc[df.index[-1], "low"] = min(float(df.iloc[-1]["low"]), float(price))
        df = calculate_indicators(df)
        last = df.iloc[-1]
        signal = generate_signal(df)
        forecast = forecast_15m(df)
        live_age = max(0, round(time.time() - last_tick, 1)) if last_tick else None
        if signal == "SUBE":
            explanation = "EMA 9 sobre EMA 21, RSI mayor que 50, MACD alcista y precio sobre VWAP. Confirmación técnica, no garantía."
        elif signal == "BAJA":
            explanation = "EMA 9 bajo EMA 21, RSI menor que 50, MACD bajista y precio bajo VWAP. Confirmación técnica, no garantía."
        else:
            explanation = "Los indicadores no coinciden. Esperar una confirmación más clara."
        candles = []
        for _, row in df.iterrows():
            candles.append({"time": int(row["time"].timestamp()), "open": float(row["open"]),
                "high": float(row["high"]), "low": float(row["low"]), "close": float(row["close"]),
                "volume": float(row["volume"]), "ema9": float(row["EMA9"]), "ema21": float(row["EMA21"]),
                "vwap": float(row["VWAP"]) if pd.notna(row["VWAP"]) else None,
                "rsi": float(row["RSI"]) if pd.notna(row["RSI"]) else None,
                "macd": float(row["MACD"]), "macd_signal": float(row["MACD_SIGNAL"]),
                "macd_hist": float(row["MACD_HIST"])})
        return jsonify({"price": float(price if price is not None else last["close"]),
            "signal": signal, "projected_price_15m": forecast["projected_price_15m"],
            "projected_change_pct": forecast["projected_change_pct"], "probabilities": forecast["probabilities"],
            "forecast_seconds_left": forecast["forecast_seconds_left"], "forecast_close_ts": forecast["forecast_close_ts"],
            "rsi": float(last["RSI"]) if pd.notna(last["RSI"]) else None,
            "support": float(last["SUPPORT"]), "resistance": float(last["RESISTANCE"]),
            "source": source, "feed_connected": connected, "price_age_seconds": live_age,
            "feed_status": "EN VIVO" if connected and live_age is not None and live_age < 15 else "RECIBIENDO / REVISAR",
            "feed_error": error, "explanation": explanation, "candles": candles})
    except Exception as exc:
        app.logger.exception("BTC market data error")
        return jsonify({"error": str(exc)}), 502

@app.route("/api/kalshi")
def kalshi_data():
    try:
        return jsonify(kalshi_market_data())
    except Exception as exc:
        app.logger.exception("Kalshi market data error")
        return jsonify({
            "available": False,
            "series": KALSHI_SERIES,
            "message": "No se pudo consultar Kalshi: " + str(exc),
        }), 502


start_market_feeds()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
