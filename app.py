import json
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests
import websocket
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# BITCOIN RADAR PRO + KALSHI BTC 15M
# Coinbase WebSocket · Velas de 1 segundo
# Solo lectura. NO envia ordenes a Kalshi.
# ============================================================

SYMBOL = "BTC-USD"
CANDLE_LIMIT = 300
KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/4.0"})

# Datos compartidos del flujo de Coinbase
_ws_lock = threading.Lock()
_ws_bars = deque(maxlen=CANDLE_LIMIT)
_ws_status = {"connected": False, "error": ""}
_ws_started = False


def add_trade(timestamp, price, size):
    second = int(timestamp)
    price = float(price)
    size = float(size or 0)

    with _ws_lock:
        if _ws_bars and second < _ws_bars[-1]["time"]:
            return

        if not _ws_bars:
            _ws_bars.append({
                "time": second,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size
            })
            return

        last = _ws_bars[-1]

        if second == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
            last["volume"] += size
            return

        # Completa segundos sin operaciones con el último precio
        for missing in range(last["time"] + 1, second):
            _ws_bars.append({
                "time": missing,
                "open": last["close"],
                "high": last["close"],
                "low": last["close"],
                "close": last["close"],
                "volume": 0.0
            })
            last = _ws_bars[-1]

        _ws_bars.append({
            "time": second,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": size
        })


def coinbase_message(ws, message):
    try:
        data = json.loads(message)

        if data.get("type") != "ticker":
            return

        if data.get("product_id") != SYMBOL:
            return

        price = data.get("price")
        event_time = data.get("time")

        if price is None or event_time is None:
            return

        dt = datetime.fromisoformat(
            event_time.replace("Z", "+00:00")
        )

        add_trade(
            dt.timestamp(),
            price,
            data.get("last_size", 0)
        )

    except Exception as exc:
        with _ws_lock:
            _ws_status["error"] = str(exc)


def coinbase_open(ws):
    with _ws_lock:
        _ws_status["connected"] = True
        _ws_status["error"] = ""

    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": [SYMBOL],
        "channels": ["ticker"]
    }))


def coinbase_close(ws, status_code, message):
    with _ws_lock:
        _ws_status["connected"] = False
        _ws_status["error"] = str(message or "Conexión cerrada")


def coinbase_error(ws, error):
    with _ws_lock:
        _ws_status["connected"] = False
        _ws_status["error"] = str(error)


def seed_recent_trades():
    try:
        response = session.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/trades",
            params={"limit": 1000},
            timeout=10
        )
        response.raise_for_status()

        trades = response.json()

        for trade in reversed(trades):
            dt = datetime.fromisoformat(
                trade["time"].replace("Z", "+00:00")
            )

            add_trade(
                dt.timestamp(),
                trade["price"],
                trade.get("size", 0)
            )

    except Exception as exc:
        with _ws_lock:
            _ws_status["error"] = "Histórico inicial: " + str(exc)


def coinbase_ws_loop():
    seed_recent_trades()

    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://ws-feed.exchange.coinbase.com",
                on_open=coinbase_open,
                on_message=coinbase_message,
                on_close=coinbase_close,
                on_error=coinbase_error
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            with _ws_lock:
                _ws_status["connected"] = False
                _ws_status["error"] = str(exc)

        time.sleep(3)


def start_coinbase_stream():
    global _ws_started

    if _ws_started:
        return

    _ws_started = True

    thread = threading.Thread(
        target=coinbase_ws_loop,
        name="coinbase-btc-stream",
        daemon=True
    )

    thread.start()


def get_market_data():
    with _ws_lock:
        rows = [dict(row) for row in _ws_bars]
        connected = _ws_status["connected"]
        error = _ws_status["error"]

    if len(rows) < 30:
        detail = error or (
            "Conectando a Coinbase..."
            if not connected
            else "Recibiendo operaciones..."
        )

        raise RuntimeError(
            "Aún no hay suficientes velas de 1 segundo. " + detail
        )

    df = pd.DataFrame(rows)

    df["time"] = pd.to_datetime(
        df["time"],
        unit="s",
        utc=True
    )

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return (
        df.dropna()
        .tail(CANDLE_LIMIT)
        .reset_index(drop=True),
        "Coinbase WebSocket · 1s"
    )


def calculate_indicators(df):
    df = df.copy()
    close = df["close"]

    df["EMA9"] = close.ewm(
        span=9,
        adjust=False
    ).mean()

    df["EMA21"] = close.ewm(
        span=21,
        adjust=False
    ).mean()

    typical = (df["high"] + df["low"] + close) / 3

    cumulative_volume = (
        df["volume"].cumsum().replace(0, np.nan)
    )

    df["VWAP"] = (
        (typical * df["volume"]).cumsum()
        / cumulative_volume
    )

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / 14,
        adjust=False,
        min_periods=14
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / 14,
        adjust=False,
        min_periods=14
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)

    df["RSI"] = 100 - (100 / (1 + rs))

    df.loc[
        (avg_loss == 0) & (avg_gain > 0),
        "RSI"
    ] = 100

    df.loc[
        (avg_gain == 0) & (avg_loss > 0),
        "RSI"
    ] = 0

    df.loc[
        (avg_gain == 0) & (avg_loss == 0),
        "RSI"
    ] = 50

    ema12 = close.ewm(
        span=12,
        adjust=False
    ).mean()

    ema26 = close.ewm(
        span=26,
        adjust=False
    ).mean()

    df["MACD"] = ema12 - ema26

    df["MACD_SIGNAL"] = df["MACD"].ewm(
        span=9,
        adjust=False
    ).mean()

    df["MACD_HIST"] = (
        df["MACD"] - df["MACD_SIGNAL"]
    )

    df["SUPPORT"] = df["low"].rolling(
        50,
        min_periods=1
    ).min()

    df["RESISTANCE"] = df["high"].rolling(
        50,
        min_periods=1
    ).max()

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


def as_float(value):
    try:
        if value is None or value == "":
            return None

        return float(value)

    except (TypeError, ValueError):
        return None


def kalshi_market_data():
    try:
        response = session.get(
            f"{KALSHI_API}/markets",
            params={
                "series_ticker": KALSHI_SERIES,
                "status": "open",
                "limit": 100
            },
            timeout=12
        )

        response.raise_for_status()

        payload = response.json()
        markets = payload.get("markets", []) or []

        now = datetime.now(timezone.utc)
        candidates = []

        for market in markets:
            close_text = (
                market.get("close_time")
                or market.get("expected_expiration_time")
            )

            close_dt = None

            if close_text:
                try:
                    close_dt = datetime.fromisoformat(
                        close_text.replace("Z", "+00:00")
                    )

                    if close_dt.tzinfo is None:
                        close_dt = close_dt.replace(
                            tzinfo=timezone.utc
                        )

                except ValueError:
                    close_dt = None

            if close_dt is None or close_dt > now:
                candidates.append((
                    close_dt or datetime.max.replace(
                        tzinfo=timezone.utc
                    ),
                    market
                ))

        if not candidates:
            return {
                "available": False,
                "message": "No se encontró un contrato BTC 15M abierto.",
                "series": KALSHI_SERIES
            }

        candidates.sort(key=lambda item: item[0])
        market = candidates[0][1]

        yes_bid = as_float(market.get("yes_bid_dollars"))
        yes_ask = as_float(market.get("yes_ask_dollars"))
        no_bid = as_float(market.get("no_bid_dollars"))
        no_ask = as_float(market.get("no_ask_dollars"))

        yes_estimated = False
        no_estimated = False

        if yes_ask is None and no_bid is not None:
            yes_ask = 1.0 - no_bid
            yes_estimated = True

        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid
            no_estimated = True

        close_ts = None

        close_text = (
            market.get("close_time")
            or market.get("expected_expiration_time")
        )

        if close_text:
            try:
                close_dt = datetime.fromisoformat(
                    close_text.replace("Z", "+00:00")
                )

                if close_dt.tzinfo is None:
                    close_dt = close_dt.replace(
                        tzinfo=timezone.utc
                    )

                close_ts = int(close_dt.timestamp())

            except ValueError:
                pass

        return {
            "available": True,
            "series": KALSHI_SERIES,
            "ticker": market.get("ticker"),
            "title": (
                market.get("title")
                or market.get("subtitle")
                or "Bitcoin 15 minutos"
            ),
            "status": market.get("status"),
            "close_ts": close_ts,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "last_price": as_float(
                market.get("last_price_dollars")
            ),
            "yes_ask_estimated": yes_estimated,
            "no_ask_estimated": no_estimated,
            "floor_strike": as_float(
                market.get("floor_strike")
            ),
            "cap_strike": as_float(
                market.get("cap_strike")
            ),
            "rules": market.get("rules_primary") or ""
        }

    except Exception as exc:
        return {
            "available": False,
            "message": f"Error conectando a Kalshi: {str(exc)}",
            "series": KALSHI_SERIES
        }


# Inicia Coinbase cuando Render carga la aplicación
start_coinbase_stream()


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():
    try:
        df_raw, source = get_market_data()

    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    df = calculate_indicators(df_raw)
    signal = generate_signal(df)
    last = df.iloc[-1]

    def line(col):
        out = []

        for t, v in zip(df["time"], df[col]):
            if pd.isna(v):
                continue

            out.append({
                "time": int(pd.Timestamp(t).timestamp()),
                "value": round(float(v), 4)
            })

        return out

    candles = []
    volume = []

    for t, o, h, l, c, v in zip(
        df["time"],
        df["open"],
        df["high"],
        df["low"],
        df["close"],
        df["volume"]
    ):
        ts = int(pd.Timestamp(t).timestamp())

        candles.append({
            "time": ts,
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c)
        })

        volume.append({
            "time": ts,
            "value": float(v),
            "color": "#00d69a" if c >= o else "#ff5268"
        })

    macd_hist = []

    for t, v in zip(df["time"], df["MACD_HIST"]):
        if pd.isna(v):
            continue

        macd_hist.append({
            "time": int(pd.Timestamp(t).timestamp()),
            "value": round(float(v), 2),
            "color": "#00d69a" if v >= 0 else "#ff5268"
        })

    return jsonify({
        "price": float(last["close"]),
        "signal": signal,
        "rsi": None if pd.isna(last["RSI"]) else float(last["RSI"]),
        "source": source,
        "candles": candles,
        "ema9": line("EMA9"),
        "ema21": line("EMA21"),
        "vwap": line("VWAP"),
        "volume": volume,
        "rsi_series": line("RSI"),
        "macd": line("MACD"),
        "macd_signal": line("MACD_SIGNAL"),
        "macd_hist": macd_hist,
        "support": None if pd.isna(last["SUPPORT"]) else float(last["SUPPORT"]),
        "resistance": None if pd.isna(last["RESISTANCE"]) else float(last["RESISTANCE"]),
        "market": kalshi_market_data()
    })
    HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#080d18">
<title>Bitcoin Radar PRO</title>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>
:root {
 --bg:#080d18;
 --panel:#111a2a;
 --panel2:#182438;
 --line:#263750;
 --muted:#91a4bd;
 --white:#edf4ff;
 --green:#00d69a;
 --red:#ff5268;
 --yellow:#ffc857;
 --blue:#36a3ff;
 --purple:#c58cff;
}
* {box-sizing:border-box}
body {
 margin:0;
 padding:12px;
 background:var(--bg);
 color:var(--white);
 font-family:Arial,sans-serif;
}
main {max-width:1180px;margin:auto}
header {
 display:flex;
 justify-content:space-between;
 align-items:center;
 gap:10px;
 margin:5px 0 14px;
 flex-wrap:wrap;
}
h1 {font-size:19px;letter-spacing:1.4px;margin:0 0 5px}
.sub {font-size:10px;color:var(--green)}
button {
 border:1px solid #3b526e;
 background:#1b2b42;
 color:white;
 border-radius:8px;
 padding:10px 13px;
 font-weight:bold;
 cursor:pointer;
}
.panel {
 background:var(--panel);
 border:1px solid var(--line);
 border-radius:13px;
 padding:13px;
 margin-bottom:12px;
}
.metrics {
 display:grid;
 grid-template-columns:repeat(2,minmax(0,1fr));
 gap:9px;
 margin-bottom:12px;
}
.metric {
 background:var(--panel2);
 padding:12px;
 border-radius:9px;
 min-width:0;
}
.metric small,.muted {
 font-size:10px;
 color:var(--muted);
}
.metric strong {
 display:block;
 font-size:19px;
 margin-top:7px;
 overflow-wrap:anywhere;
}
.section {
 font-size:11px;
 color:#aebed2;
 letter-spacing:1px;
 margin:2px 0 12px;
 font-weight:bold;
}
.green {color:var(--green)}
.red {color:var(--red)}
.yellow {color:var(--yellow)}
.blue {color:var(--blue)}
.signal-grid {
 display:grid;
 grid-template-columns:repeat(3,1fr);
 gap:8px;
}
.signal {
 padding:13px 5px;
 text-align:center;
 border-radius:9px;
 background:#182438;
 font-size:12px;
 font-weight:bold;
 border:1px solid #344963;
}
.active-up {
 background:#103d34;
 border-color:var(--green);
 color:var(--green);
}
.active-down {
 background:#3b202b;
 border-color:var(--red);
 color:var(--red);
}
.active-wait {
 background:#40351e;
 border-color:var(--yellow);
 color:var(--yellow);
}
.chart {width:100%;height:360px}
.small-chart {height:150px}
.legend {
 display:flex;
 flex-wrap:wrap;
 gap:12px;
 font-size:10px;
 color:#aabbd0;
 margin-bottom:10px;
}
.legend span {display:flex;align-items:center;gap:5px}
.dot {width:8px;height:8px;border-radius:50%}
.two {
 display:grid;
 grid-template-columns:repeat(2,minmax(0,1fr));
 gap:9px;
}
.quote {
 background:var(--panel2);
 border-radius:10px;
 padding:12px;
}
.quote b {font-size:10px;color:var(--muted)}
.quote strong {
 display:block;
 font-size:25px;
 margin-top:6px;
}
.formgrid {
 display:grid;
 grid-template-columns:repeat(2,minmax(0,1fr));
 gap:9px;
}
label {
 display:block;
 font-size:10px;
 color:var(--muted);
 margin-bottom:5px;
}
input,select {
 width:100%;
 background:#0b1422;
 color:var(--white);
 border:1px solid #344963;
 border-radius:8px;
 padding:11px;
 font-size:14px;
}
.actionrow {
 display:flex;
 gap:8px;
 flex-wrap:wrap;
 margin-top:10px;
}
.actionrow button {flex:1}
.note {
 font-size:10px;
 line-height:1.5;
 color:#8293a9;
}
table {
 width:100%;
 border-collapse:collapse;
 font-size:12px;
}
td {
 padding:8px 3px;
 border-bottom:1px solid #24344b;
}
td:last-child {
 text-align:right;
 font-weight:bold;
}
.alertrow {
 display:flex;
 gap:9px;
 align-items:center;
 padding:8px 0;
 border-bottom:1px solid #24344b;
 font-size:12px;
}
.pill {
 font-size:10px;
 border-radius:20px;
 padding:4px 7px;
 background:#263750;
 color:#b9cbe0;
}
footer {
 text-align:center;
 color:#71839b;
 font-size:10px;
 padding:12px;
}
@media(min-width:700px) {
 body {padding:22px}
 .metrics {grid-template-columns:repeat(4,1fr)}
 .chart {height:490px}
 .small-chart {height:170px}
 .layout {
  display:grid;
  grid-template-columns:1.4fr 1fr;
  gap:12px;
 }
 .layout .panel {height:fit-content}
}
</style>
</head>
<body>
<main>

<header>
 <div>
  <h1>BITCOIN RADAR PRO</h1>
  <div class="sub" id="connection">● CONECTANDO DATOS...</div>
 </div>
 <button onclick="updateAll()">ACTUALIZAR</button>
</header>

<div class="metrics">
 <div class="metric">
  <small>BITCOIN / USD</small>
  <strong id="btcPrice">--</strong>
 </div>
 <div class="metric">
  <small>SEÑAL TÉCNICA</small>
  <strong id="mainSignal" class="yellow">ESPERAR</strong>
 </div>
 <div class="metric">
  <small>RSI 14</small>
  <strong id="rsiValue">--</strong>
 </div>
 <div class="metric">
  <small>CONTRATO CIERRA EN</small>
  <strong id="timer">--:--</strong>
 </div>
</div>

<div class="layout">
<div>

<section class="panel">
 <div class="section">RADAR DE DIRECCIÓN</div>
 <div class="signal-grid">
  <div class="signal" id="upSignal">▲ SUBE</div>
  <div class="signal" id="downSignal">▼ BAJA</div>
  <div class="signal active-wait" id="waitSignal">Ⅱ ESPERAR</div>
 </div>
 <p class="note" id="explanation">
  Esperando confirmación de indicadores.
 </p>
</section>

<section class="panel">
 <div class="section">BTC · VELAS JAPONESAS · 1 SEGUNDO</div>
 <div class="legend">
  <span><i class="dot" style="background:#ffc857"></i>EMA 9</span>
  <span><i class="dot" style="background:#36a3ff"></i>EMA 21</span>
  <span><i class="dot" style="background:#c58cff"></i>VWAP</span>
 </div>
 <div id="priceChart" class="chart"></div>
</section>

<section class="panel">
 <div class="section">VOLUMEN</div>
 <div id="volumeChart" class="chart small-chart"></div>
</section>

<section class="panel">
 <div class="section">RSI 14</div>
 <div id="rsiChart" class="chart small-chart"></div>
</section>

<section class="panel">
 <div class="section">MACD</div>
 <div id="macdChart" class="chart small-chart"></div>
</section>

</div>

<div>

<section class="panel">
 <div class="section">MERCADO KALSHI · BTC 15 MINUTOS</div>
 <div id="marketTitle" style="font-size:13px;font-weight:bold;margin-bottom:8px">
  Buscando contrato...
 </div>
 <div class="muted" id="marketTicker">Ticker: --</div>

 <div class="two" style="margin-top:10px">
  <div class="quote">
   <b>YES · COMPRA</b>
   <strong class="green" id="yesAsk">--¢</strong>
   <span class="muted" id="yesBid">Bid: --</span>
  </div>
  <div class="quote">
   <b>NO · COMPRA</b>
   <strong class="red" id="noAsk">--¢</strong>
   <span class="muted" id="noBid">Bid: --</span>
  </div>
 </div>

 <p class="note" id="marketMessage">
  Consultando la API pública de Kalshi.
 </p>
 <div class="muted" id="strikeInfo"></div>
</section>

<section class="panel">
 <div class="section">MI OPERACIÓN · SEGUIMIENTO MANUAL</div>

 <div class="formgrid">
  <div>
   <label>LADO COMPRADO</label>
   <select id="side">
    <option value="yes">YES</option>
    <option value="no">NO</option>
   </select>
  </div>

  <div>
   <label>CONTRATOS</label>
   <input id="quantity" type="number" min="1" step="1" value="1">
  </div>

  <div>
   <label>PRECIO DE ENTRADA (¢)</label>
   <input id="entry" type="number" min="1" max="99" placeholder="Ej. 52">
  </div>

  <div>
   <label>OBJETIVO DE SALIDA (¢)</label>
   <input id="target" type="number" min="1" max="99" placeholder="Ej. 70">
  </div>

  <div>
   <label>LÍMITE DE PÉRDIDA (¢)</label>
   <input id="stop" type="number" min="1" max="99" placeholder="Ej. 40">
  </div>

  <div>
   <label>ALERTA SONORA</label>
   <select id="sound">
    <option value="on">Activada</option>
    <option value="off">Desactivada</option>
   </select>
  </div>
 </div>

 <div class="actionrow">
  <button onclick="saveTrade()">GUARDAR OPERACIÓN</button>
  <button onclick="clearTrade()">CERRAR / BORRAR</button>
 </div>

 <table style="margin-top:10px">
  <tr>
   <td>Precio actual de salida estimado</td>
   <td id="currentExit">--</td>
  </tr>
  <tr>
   <td>Ganancia / pérdida estimada</td>
   <td id="pnl">--</td>
  </tr>
  <tr>
   <td>Estado</td>
   <td id="tradeStatus">Sin operación guardada</td>
  </tr>
 </table>

 <p class="note">
  Estimación antes de comisiones y deslizamiento. El panel no compra
  ni vende por ti.
 </p>
</section>

<section class="panel">
 <div class="section">ALERTAS INTELIGENTES</div>

 <div class="alertrow">
  <span class="pill">SEÑAL</span>
  Aviso cuando cambia SUBE / BAJA / ESPERAR
 </div>
 <div class="alertrow">
  <span class="pill">OBJETIVO</span>
  Aviso al alcanzar el precio objetivo
 </div>
 <div class="alertrow">
  <span class="pill">STOP</span>
  Aviso al alcanzar el límite de pérdida
 </div>
 <div class="alertrow">
  <span class="pill">SALIDA</span>
  Aviso si se debilita la señal técnica
 </div>

 <div class="actionrow">
  <button onclick="enableSound()">ACTIVAR SONIDO</button>
 </div>

 <p class="note" id="alertStatus">
  Las alertas iguales no se repiten en cada actualización.
 </p>
</section>

</div>
</div>

<footer>
 Datos informativos. Las señales técnicas no garantizan resultados.
 Los contratos YES/NO dependen de las reglas del mercado seleccionado.
</footer>

</main>

<script>
const $ = id => document.getElementById(id);

const fmtUSD = n => n == null
 ? "--"
 : "$" + Number(n).toLocaleString("en-US", {
     minimumFractionDigits:2,
     maximumFractionDigits:2
   });

const cents = n => n == null
 ? "--¢"
 : (Number(n)*100).toFixed(1)+"¢";

function chartOptions(el, height) {
 return {
  width:el.clientWidth,
  height:height,
  layout:{
   background:{color:"#111a2a"},
   textColor:"#aebdd0",
   fontSize:10
  },
  grid:{
   vertLines:{color:"#1d2b40"},
   horzLines:{color:"#1d2b40"}
  },
  rightPriceScale:{borderColor:"#34445a"},
  timeScale:{
   borderColor:"#34445a",
   timeVisible:true,
   secondsVisible:true,
   rightOffset:3,
   barSpacing:7,
   minBarSpacing:2
  },
  crosshair:{mode:LightweightCharts.CrosshairMode.Normal},
  handleScroll:true,
  handleScale:true
 };
}

const pc = LightweightCharts.createChart(
 $("priceChart"), chartOptions($("priceChart"),360)
);

const candle = pc.addCandlestickSeries({
 upColor:"#00d69a",
 downColor:"#ff5268",
 borderUpColor:"#00d69a",
 borderDownColor:"#ff5268",
 wickUpColor:"#00d69a",
 wickDownColor:"#ff5268"
});

const e9 = pc.addLineSeries({
 color:"#ffc857",
 lineWidth:2,
 title:"EMA 9"
});

const e21 = pc.addLineSeries({
 color:"#36a3ff",
 lineWidth:2,
 title:"EMA 21"
});

const vw = pc.addLineSeries({
 color:"#c58cff",
 lineWidth:2,
 title:"VWAP"
});

const vc = LightweightCharts.createChart(
 $("volumeChart"), chartOptions($("volumeChart"),150)
);

const vol = vc.addHistogramSeries({
 priceFormat:{type:"volume"},
 priceScaleId:""
});

vc.priceScale("").applyOptions({
 scaleMargins:{top:.1,bottom:0}
});

const rc = LightweightCharts.createChart(
 $("rsiChart"), chartOptions($("rsiChart"),150)
);

const rsi = rc.addLineSeries({
 color:"#bd8aff",
 lineWidth:2,
 title:"RSI"
});

[70,50,30].forEach((p,i) => {
 rsi.createPriceLine({
  price:p,
  color:i===0 ? "#ff5268" : i===1 ? "#71839c" : "#00d69a",
  lineStyle:LightweightCharts.LineStyle.Dashed,
  title:String(p)
 });
});

rc.priceScale("right").applyOptions({
 autoScale:false,
 scaleMargins:{top:.1,bottom:.1}
});

const mc = LightweightCharts.createChart(
 $("macdChart"), chartOptions($("macdChart"),150)
);

const ml = mc.addLineSeries({
 color:"#36a3ff",
 lineWidth:2,
 title:"MACD"
});

const ms = mc.addLineSeries({
 color:"#ffc857",
 lineWidth:2,
 title:"Signal"
});

const mh = mc.addHistogramSeries({
 lastValueVisible:false
});

ml.createPriceLine({
 price:0,
 color:"#71839c",
 lineStyle:LightweightCharts.LineStyle.Dashed
});

let btcData = null;
let marketData = null;
let previousSignal = null;
let savedTrade = null;
let audioCtx = null;
let lastAlertKey = "";
let marketCloseTs = null;
let firstLoad = true;

try {
  savedTrade = JSON.parse(localStorage.getItem("radarTrade") || "null");
} catch (e) {
  savedTrade = null;
}

if (savedTrade) {
  if (savedTrade.side) $("side").value = savedTrade.side;
  if (savedTrade.quantity) $("quantity").value = savedTrade.quantity;
  if (savedTrade.entry) $("entry").value = savedTrade.entry;
  if (savedTrade.target) $("target").value = savedTrade.target;
  if (savedTrade.stop) $("stop").value = savedTrade.stop;
  if (savedTrade.sound) $("sound").value = savedTrade.sound;
}

function soundOn() {
 return $("sound").value === "on";
}

function beep(freq, dur) {
 if (!soundOn()) return;

 try {
  if (!audioCtx) {
   audioCtx = new (
    window.AudioContext || window.webkitAudioContext
   )();
  }

  if (audioCtx.state === "suspended") {
   audioCtx.resume();
  }

  const o = audioCtx.createOscillator();
  const g = audioCtx.createGain();

  o.connect(g);
  g.connect(audioCtx.destination);

  o.frequency.value = freq || 880;
  o.type = "sine";

  g.gain.setValueAtTime(0.12, audioCtx.currentTime);

  o.start();
  o.stop(audioCtx.currentTime + (dur || 0.15));

 } catch (e) {}
}

function enableSound() {
 $("sound").value = "on";

 beep(660, 0.18);

 setTimeout(() => beep(880, 0.18), 200);

 $("alertStatus").textContent =
  "Sonido activado. Sonará cuando cambie la señal o se alcance objetivo/stop.";
}

function setSignal(sig) {
 const map = {
  SUBE: ["upSignal", "active-up"],
  BAJA: ["downSignal", "active-down"],
  ESPERAR: ["waitSignal", "active-wait"]
 };

 ["upSignal", "downSignal", "waitSignal"].forEach(id =>
  $(id).classList.remove(
   "active-up",
   "active-down",
   "active-wait"
  )
 );

 const m = map[sig] || map["ESPERAR"];

 $(m[0]).classList.add(m[1]);

 const el = $("mainSignal");

 el.textContent = sig;

 el.className =
  sig === "SUBE" ? "green" :
  sig === "BAJA" ? "red" : "yellow";

 $("explanation").textContent =
  sig === "SUBE"
   ? "EMA9 sobre EMA21, RSI sobre 50, MACD sobre su señal y precio sobre VWAP."
   : sig === "BAJA"
   ? "EMA9 bajo EMA21, RSI bajo 50, MACD bajo su señal y precio bajo VWAP."
   : "Los indicadores no están alineados. La señal usa velas de 1 segundo y puede ser más sensible al ruido.";
}

function fmtMoney(n) {
 if (n == null || isNaN(n)) return "--";

 const sign = n < 0 ? "-" : "";

 return sign + "$" + Math.abs(n).toFixed(2);
}

function updateTradePanel() {
 if (!savedTrade) {
  $("currentExit").textContent = "--";
  $("pnl").textContent = "--";
  $("tradeStatus").textContent = "Sin operación guardada";
  return;
 }

 if (!marketData || !marketData.available) {
  $("tradeStatus").textContent =
   "Guardada, esperando cotización de Kalshi";
  return;
 }

 const side = savedTrade.side || "yes";
 const entry = parseFloat(savedTrade.entry);
 const qty = parseFloat(savedTrade.quantity) || 1;
 const target = parseFloat(savedTrade.target);
 const stop = parseFloat(savedTrade.stop);

 const exitDollars =
  side === "yes" ? marketData.yes_ask : marketData.no_ask;

 if (exitDollars == null || isNaN(entry)) {
  $("currentExit").textContent = "--";
  $("pnl").textContent = "--";
  $("tradeStatus").textContent =
   "Falta precio de entrada o cotización";
  return;
 }

 const exitCents = exitDollars * 100;
 const pnl = (exitDollars - entry / 100) * qty;

 $("currentExit").textContent =
  exitCents.toFixed(1) + "¢";

 const pnlEl = $("pnl");

 pnlEl.textContent = fmtMoney(pnl);
 pnlEl.className = pnl >= 0 ? "green" : "red";

 let status = "Abierta";
 let alertKey = "";

 if (!isNaN(target) && exitCents >= target) {
  status = "¡Objetivo alcanzado!";
  alertKey = "target";
 } else if (!isNaN(stop) && exitCents <= stop) {
  status = "¡Límite de pérdida alcanzado!";
  alertKey = "stop";
 } else if (pnl >= 0) {
  status = "Ganando";
 } else {
  status = "Perdiendo";
 }

 $("tradeStatus").textContent = status;

 if (
  alertKey &&
  lastAlertKey !== alertKey + ":" + (savedTrade.entry || "")
 ) {
  lastAlertKey =
   alertKey + ":" + (savedTrade.entry || "");

  $("alertStatus").textContent = "Alerta: " + status;

  beep(alertKey === "target" ? 990 : 440, 0.25);
 }
}

function saveTrade() {
 savedTrade = {
  side: $("side").value,
  quantity: $("quantity").value,
  entry: $("entry").value,
  target: $("target").value,
  stop: $("stop").value,
  sound: $("sound").value
 };

 try {
  localStorage.setItem(
   "radarTrade",
   JSON.stringify(savedTrade)
  );
 } catch (e) {}

 lastAlertKey = "";

 updateTradePanel();

 $("tradeStatus").textContent =
  "Operación guardada (seguimiento manual, no compra ni vende).";
}

function clearTrade() {
 savedTrade = null;

 try {
  localStorage.removeItem("radarTrade");
 } catch (e) {}

 ["entry", "target", "stop"].forEach(id => {
  $(id).value = "";
 });

 $("quantity").value = 1;

 updateTradePanel();
}

function renderMarket(m) {
 marketData = m;

 if (!m || !m.available) {
  $("marketTitle").textContent = "Sin contrato abierto";
  $("marketTicker").textContent = "Ticker: --";
  $("yesAsk").textContent = "--¢";
  $("noAsk").textContent = "--¢";
  $("yesBid").textContent = "Bid: --";
  $("noBid").textContent = "Bid: --";

  $("marketMessage").textContent =
   (m && m.message) || "No hay datos de Kalshi.";

  $("strikeInfo").textContent = "";
  marketCloseTs = null;

  return;
 }

 $("marketTitle").textContent =
  m.title || "Bitcoin 15 minutos";

 $("marketTicker").textContent =
  "Ticker: " + (m.ticker || "--");

 $("yesAsk").textContent = cents(m.yes_ask);
 $("noAsk").textContent = cents(m.no_ask);

 $("yesBid").textContent =
  "Bid: " + cents(m.yes_bid);

 $("noBid").textContent =
  "Bid: " + cents(m.no_bid);

 $("marketMessage").textContent =
  "Precio del mercado Kalshi (API pública). El precio YES/NO es lo que manda, no la flecha de la señal.";

 const f = m.floor_strike;
 const c = m.cap_strike;

 $("strikeInfo").textContent =
  (f != null && c != null)
   ? (
      "Rango del contrato: $" +
      Number(f).toLocaleString("en-US") +
      " – $" +
      Number(c).toLocaleString("en-US")
     )
   : "";

 marketCloseTs = m.close_ts || null;
}

function tickTimer() {
 if (!marketCloseTs) {
  $("timer").textContent = "--:--";
  return;
 }

 const left =
  marketCloseTs - Math.floor(Date.now() / 1000);

 if (left <= 0) {
  $("timer").textContent = "CERRADO";
  return;
 }

 const mm = String(
  Math.floor(left / 60)
 ).padStart(2, "0");

 const ss = String(
  left % 60
 ).padStart(2, "0");

 $("timer").textContent = mm + ":" + ss;
}

async function updateAll() {
 try {
  const r = await fetch("/api/data");
  const d = await r.json();

  if (!r.ok || d.error) {
   throw new Error(d.error || ("HTTP " + r.status));
  }

  btcData = d;

  $("connection").textContent =
   "● CONECTADO · " + (d.source || "BTC");

  $("btcPrice").textContent = fmtUSD(d.price);

  $("rsiValue").textContent =
   d.rsi == null ? "--" : d.rsi.toFixed(1);

  setSignal(d.signal);

  if (d.candles && d.candles.length) {
   candle.setData(d.candles);
   e9.setData(d.ema9);
   e21.setData(d.ema21);
   vw.setData(d.vwap);
   vol.setData(d.volume);
   rsi.setData(d.rsi_series);
   ml.setData(d.macd);
   ms.setData(d.macd_signal);
   mh.setData(d.macd_hist);
  }

  if (
   firstLoad &&
   d.candles &&
   d.candles.length > 60
  ) {
   pc.timeScale().setVisibleLogicalRange({
    from: d.candles.length - 60,
    to: d.candles.length - 1
   });

   firstLoad = false;
  }

  renderMarket(d.market);
  updateTradePanel();

  if (
   previousSignal !== null &&
   previousSignal !== d.signal
  ) {
   $("alertStatus").textContent =
    "La señal cambió de " +
    previousSignal +
    " a " +
    d.signal +
    ".";

   beep(
    d.signal === "SUBE" ? 880 :
    d.signal === "BAJA" ? 520 : 660,
    0.2
   );
  }

  previousSignal = d.signal;

 } catch (e) {
  $("connection").textContent =
   "● SIN CONEXIÓN · " + e.message;
 }
}

function fitCharts() {
 pc.applyOptions({
  width: $("priceChart").clientWidth
 });

 vc.applyOptions({
  width: $("volumeChart").clientWidth
 });

 rc.applyOptions({
  width: $("rsiChart").clientWidth
 });

 mc.applyOptions({
  width: $("macdChart").clientWidth
 });
}

window.addEventListener("resize", fitCharts);

updateAll();

setInterval(updateAll, 1000);
setInterval(tickTimer, 1000);
</script>

</body>
</html>
"""


if __name__ == "__main_":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
