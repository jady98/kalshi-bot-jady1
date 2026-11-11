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

# ==========================================
# BITCOIN RADAR PRO - SOLO COINBASE
# ==========================================

SYMBOL = "BTC-USD"
CANDLE_LIMIT = 300

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/6.0"})

lock = threading.RLock()
bars = deque(maxlen=CANDLE_LIMIT)

ws_status = {
    "connected": False,
    "socket_open": False,
    "error": "",
    "last_message": None,
    "last_price": None
}

worker_started = False


def utc_now():
    return datetime.now(timezone.utc)


def add_trade(timestamp, price, size=0):
    try:
        if isinstance(timestamp, str):
            dt = datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            )
            ts = int(dt.timestamp())
        else:
            ts = int(float(timestamp))

        price = float(price)
        size = float(size or 0)

        if not np.isfinite(price) or price <= 0:
            return

        minute = ts - ts % 60

        with lock:
            if not bars:
                bars.append({
                    "time": minute,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": size
                })
                return

            last = bars[-1]

            if minute < last["time"]:
                return

            if minute == last["time"]:
                last["high"] = max(last["high"], price)
                last["low"] = min(last["low"], price)
                last["close"] = price
                last["volume"] += size
            else:
                bars.append({
                    "time": minute,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": size
                })

    except Exception as exc:
        print("Error agregando precio:", repr(exc), flush=True)


def seed_candles():
    try:
        response = session.get(
            f"{COINBASE_REST}/products/{SYMBOL}/candles",
            params={"granularity": 60},
            timeout=15
        )

        response.raise_for_status()
        data = response.json()

        loaded = []

        for item in reversed(data):
            if len(item) < 6:
                continue

            ts, low, high, open_, close, volume = item[:6]

            loaded.append({
                "time": int(ts),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume)
            })

        with lock:
            if not bars:
                for candle in loaded[-CANDLE_LIMIT:]:
                    bars.append(candle)

        print(
            f"Coinbase: {len(loaded)} velas cargadas",
            flush=True
        )

    except Exception as exc:
        print("Error cargando velas:", repr(exc), flush=True)


def set_error(message):
    with lock:
        ws_status["connected"] = False
        ws_status["error"] = str(message)

    print("Coinbase ERROR:", message, flush=True)


def on_coinbase_open(ws):
    print("Coinbase: WebSocket abierto", flush=True)

    with lock:
        ws_status["socket_open"] = True
        ws_status["connected"] = False
        ws_status["error"] = ""

    subscriptions = [
        {
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": [SYMBOL]
        },
        {
            "type": "subscribe",
            "channel": "ticker",
            "product_ids": [SYMBOL]
        },
        {
            "type": "subscribe",
            "channel": "heartbeats"
        }
    ]

    try:
        for item in subscriptions:
            ws.send(json.dumps(item))
            time.sleep(0.2)

        print("Coinbase: suscripciones enviadas", flush=True)

    except Exception as exc:
        set_error(exc)
        ws.close()


def on_coinbase_message(ws, raw_message):
    try:
        data = json.loads(raw_message)

        if data.get("type") == "error":
            set_error(data.get("message", str(data)))
            return

        channel = data.get("channel")

        if channel == "market_trades":
            for event in data.get("events", []):
                for trade in event.get("trades", []):

                    if trade.get("product_id") != SYMBOL:
                        continue

                    price = trade.get("price")
                    stamp = trade.get("time")

                    if price is not None and stamp:
                        add_trade(
                            stamp,
                            price,
                            trade.get("size", 0)
                        )

                        with lock:
                            ws_status["connected"] = True
                            ws_status["last_price"] = float(price)
                            ws_status["last_message"] = utc_now().isoformat()
                            ws_status["error"] = ""

        elif channel == "ticker":
            for event in data.get("events", []):
                for ticker in event.get("tickers", []):

                    if ticker.get("product_id") != SYMBOL:
                        continue

                    price = ticker.get("price")
                    stamp = data.get("timestamp")

                    if price is not None and stamp:
                        add_trade(stamp, price, 0)

                        with lock:
                            ws_status["connected"] = True
                            ws_status["last_price"] = float(price)
                            ws_status["last_message"] = utc_now().isoformat()
                            ws_status["error"] = ""

    except Exception as exc:
        print("Error leyendo Coinbase:", repr(exc), flush=True)


def on_coinbase_error(ws, error):
    set_error(error)


def on_coinbase_close(ws, code, message):
    detail = f"Socket cerrado: codigo={code}, mensaje={message}"

    with lock:
        ws_status["socket_open"] = False
        ws_status["connected"] = False
        ws_status["error"] = detail

    print("Coinbase:", detail, flush=True)


def coinbase_worker():
    while True:
        try:
            print("Coinbase: intentando conectar...", flush=True)

            with lock:
                ws_status["socket_open"] = False
                ws_status["connected"] = False

            ws = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=on_coinbase_open,
                on_message=on_coinbase_message,
                on_error=on_coinbase_error,
                on_close=on_coinbase_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            set_error(repr(exc))

        with lock:
            ws_status["socket_open"] = False
            ws_status["connected"] = False

        print("Coinbase: reconectando en 5 segundos", flush=True)
        time.sleep(5)


def start_coinbase_stream():
    global worker_started

    with lock:
        if worker_started:
            return
        worker_started = True

    seed_candles()

    threading.Thread(
        target=coinbase_worker,
        daemon=True,
        name="coinbase-worker"
    ).start()


def get_market_data():
    with lock:
        snapshot = [dict(item) for item in bars]

    if not snapshot:
        raise RuntimeError("Esperando datos de Coinbase")

    df = pd.DataFrame(snapshot)

    df["time"] = pd.to_datetime(
        df["time"],
        unit="s",
        utc=True
    )

    return (
        df.sort_values("time")
        .drop_duplicates("time", keep="last")
        .tail(CANDLE_LIMIT)
        .reset_index(drop=True)
    )


def calculate_indicators(df):
    df = df.copy()

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    df["EMA9"] = close.ewm(
        span=9,
        adjust=False
    ).mean()

    df["EMA21"] = close.ewm(
        span=21,
        adjust=False
    ).mean()

    typical = (high + low + close) / 3
    cumulative_volume = volume.cumsum()

    df["VWAP"] = (
        (typical * volume).cumsum()
        / cumulative_volume.replace(0, np.nan)
    ).fillna(close)

    delta = close.diff()

    gain = delta.clip(lower=0).ewm(
        alpha=1 / 14,
        adjust=False,
        min_periods=14
    ).mean()

    loss = (-delta.clip(upper=0)).ewm(
        alpha=1 / 14,
        adjust=False,
        min_periods=14
    ).mean()

    rs = gain / loss.replace(0, np.nan)

    df["RSI"] = (
        100 - (100 / (1 + rs))
    ).fillna(50)

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

    df["SUPPORT"] = low.rolling(
        20,
        min_periods=1
    ).min()

    df["RESISTANCE"] = high.rolling(
        20,
        min_periods=1
    ).max()

    return df


def generate_signal(df):
    last = df.iloc[-1]
    score = 0

    if last["EMA9"] > last["EMA21"]:
        score += 1
    elif last["EMA9"] < last["EMA21"]:
        score -= 1

    if last["MACD"] > last["MACD_SIGNAL"]:
        score += 1
    elif last["MACD"] < last["MACD_SIGNAL"]:
        score -= 1

    if last["close"] > last["VWAP"]:
        score += 1
    elif last["close"] < last["VWAP"]:
        score -= 1

    if last["RSI"] > 55:
        score += 1
    elif last["RSI"] < 45:
        score -= 1

    if score >= 3:
        direction = "SUBE"
        description = "Confluencia tecnica alcista"
    elif score <= -3:
        direction = "BAJA"
        description = "Confluencia tecnica bajista"
    else:
        direction = "NEUTRAL"
        description = "Sin confirmacion suficiente"

    return {
        "direction": direction,
        "strength": abs(score),
        "description": description
    }


def build_line(df, column):
    result = []

    for timestamp, value in zip(df["time"], df[column]):
        if pd.isna(value):
            continue

        result.append({
            "time": int(timestamp.timestamp()),
            "value": float(value)
        })

    return result
    # ==========================================
# INTERFAZ Y RUTAS
# ==========================================

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
*{box-sizing:border-box}
body{margin:0;padding:12px;background:#080d18;color:#e8edf7;font-family:Arial,sans-serif}
header{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
h1{font-size:21px;margin:0}
.subtitle,.label,.status{color:#91a0b8;font-size:12px}
.subtitle{margin-top:5px}
.panel{background:#111a2a;border:1px solid #26344c;border-radius:12px;padding:13px;margin-bottom:11px}
.price{font-size:29px;font-weight:bold;margin:7px 0}
.cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}
.value{font-size:19px;font-weight:bold;margin-top:7px}
.green{color:#00d69a}
.red{color:#ff5268}
.yellow{color:#ffc857}
#chart{height:350px;width:100%}
.chart-title{font-size:14px;font-weight:bold;margin-bottom:10px}
.footer{text-align:center;color:#718098;font-size:11px;margin:16px 0}
@media(max-width:420px){body{padding:8px}#chart{height:320px}.price{font-size:25px}}
</style>
</head>

<body>

<header>
    <div>
        <h1>BITCOIN RADAR PRO</h1>
        <div class="subtitle">BTC-USD · Coinbase · Velas de 1 minuto</div>
    </div>
    <div id="connection" class="status yellow">
        Conectando Coinbase...
    </div>
</header>

<div class="panel">
    <div class="label">PRECIO BITCOIN</div>
    <div id="price" class="price">Cargando...</div>
    <div id="source" class="status">Esperando datos...</div>
</div>

<div class="cards">
    <div class="panel">
        <div class="label">SEÑAL DEL RADAR</div>
        <div id="signal" class="value yellow">NEUTRAL</div>
        <div id="signalDescription" class="status">Esperando indicadores</div>
    </div>

    <div class="panel">
        <div class="label">RSI 14</div>
        <div id="rsi" class="value">--</div>
    </div>

    <div class="panel">
        <div class="label">EMA 9</div>
        <div id="ema9" class="value">--</div>
    </div>

    <div class="panel">
        <div class="label">EMA 21</div>
        <div id="ema21" class="value">--</div>
    </div>
</div>

<div class="panel">
    <div class="chart-title">GRÁFICO BTC-USD · 1 MINUTO</div>
    <div id="chart"></div>
</div>

<div class="cards">
    <div class="panel">
        <div class="label">VWAP</div>
        <div id="vwap" class="value">--</div>
    </div>

    <div class="panel">
        <div class="label">MACD</div>
        <div id="macd" class="value">--</div>
    </div>

    <div class="panel">
        <div class="label">SOPORTE</div>
        <div id="support" class="value">--</div>
    </div>

    <div class="panel">
        <div class="label">RESISTENCIA</div>
        <div id="resistance" class="value">--</div>
    </div>
</div>

<div class="footer">
    Datos de Coinbase · Actualización de pantalla cada segundo.
    Las señales no garantizan resultados.
</div>

<script>
const $ = id => document.getElementById(id);

const chart = LightweightCharts.createChart(
    $("chart"),
    {
        width: $("chart").clientWidth,
        height: 350,
        layout: {
            background: {color: "#111a2a"},
            textColor: "#aab7ca"
        },
        grid: {
            vertLines: {color: "#202c40"},
            horzLines: {color: "#202c40"}
        },
        rightPriceScale: {
            borderColor: "#34435a"
        },
        timeScale: {
            borderColor: "#34435a",
            timeVisible: true,
            secondsVisible: false
        },
        crosshair: {mode: 1}
    }
);

const candleSeries = chart.addCandlestickSeries({
    upColor: "#00d69a",
    downColor: "#ff5268",
    borderUpColor: "#00d69a",
    borderDownColor: "#ff5268",
    wickUpColor: "#00d69a",
    wickDownColor: "#ff5268"
});

const ema9Series = chart.addLineSeries({
    color: "#ffc857",
    lineWidth: 2,
    title: "EMA 9"
});

const ema21Series = chart.addLineSeries({
    color: "#4c9aff",
    lineWidth: 2,
    title: "EMA 21"
});

const vwapSeries = chart.addLineSeries({
    color: "#c084fc",
    lineWidth: 2,
    title: "VWAP"
});

function money(value) {
    if (value === null || value === undefined) return "--";

    const n = Number(value);
    if (!Number.isFinite(n)) return "--";

    return "$" + n.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function number(value) {
    if (value === null || value === undefined) return "--";

    const n = Number(value);
    if (!Number.isFinite(n)) return "--";

    return n.toFixed(2);
}

function updateConnection(connection) {
    if (connection && connection.connected) {
        $("connection").textContent = "Coinbase conectado";
        $("connection").className = "status green";
    } else {
        $("connection").textContent = "Coinbase reconectando";
        $("connection").className = "status yellow";
    }
}

async function updateAll() {
    try {
        const response = await fetch(
            "/api/data?_=" + Date.now(),
            {cache: "no-store"}
        );

        if (!response.ok) {
            throw new Error("Error del servidor: " + response.status);
        }

        const data = await response.json();

        if (data.error) {
            throw new Error(data.error);
        }

        $("price").textContent = money(data.price);
        $("source").textContent = "Fuente: Coinbase BTC-USD";

        updateConnection(data.connection);

        const signal = data.signal || {};

        $("signal").textContent = signal.direction || "NEUTRAL";
        $("signalDescription").textContent = signal.description || "";

        $("signal").className = "value";

        if (signal.direction === "SUBE") {
            $("signal").classList.add("green");
        } else if (signal.direction === "BAJA") {
            $("signal").classList.add("red");
        } else {
            $("signal").classList.add("yellow");
        }

        $("rsi").textContent = number(data.rsi);
        $("ema9").textContent = money(data.ema9_value);
        $("ema21").textContent = money(data.ema21_value);
        $("vwap").textContent = money(data.vwap_value);
        $("macd").textContent = number(data.macd_value);
        $("support").textContent = money(data.support);
        $("resistance").textContent = money(data.resistance);

        candleSeries.setData(data.candles || []);
        ema9Series.setData(data.ema9 || []);
        ema21Series.setData(data.ema21 || []);
        vwapSeries.setData(data.vwap || []);

    } catch (error) {
        $("source").textContent = error.message;
        $("connection").textContent = "Error de conexión";
        $("connection").className = "status red";
        console.error(error);
    }
}

function fitCharts() {
    chart.applyOptions({
        width: $("chart").clientWidth
    });
}

window.addEventListener("resize", fitCharts);

updateAll();
setInterval(updateAll, 1000);
</script>

</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/health")
def health():
    with lock:
        status = dict(ws_status)
        candle_count = len(bars)

    return jsonify({
        "ok": True,
        "coinbase_connected": status["connected"],
        "socket_open": status["socket_open"],
        "error": status["error"],
        "candles": candle_count,
        "last_message": status["last_message"],
        "last_price": status["last_price"]
    })


@app.route("/api/data")
def api_data():
    try:
        df = get_market_data()
        df = calculate_indicators(df)

        last = df.iloc[-1]

        candles = []

        for _, row in df.iterrows():
            candles.append({
                "time": int(row["time"].timestamp()),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"])
            })

        with lock:
            connection = dict(ws_status)

        return jsonify({
            "price": float(last["close"]),
            "signal": generate_signal(df),
            "rsi": float(last["RSI"]),
            "ema9_value": float(last["EMA9"]),
            "ema21_value": float(last["EMA21"]),
            "vwap_value": float(last["VWAP"]),
            "macd_value": float(last["MACD"]),
            "support": float(last["SUPPORT"]),
            "resistance": float(last["RESISTANCE"]),
            "candles": candles,
            "ema9": build_line(df, "EMA9"),
            "ema21": build_line(df, "EMA21"),
            "vwap": build_line(df, "VWAP"),
            "connection": connection
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


# ==========================================
# INICIO
# ==========================================

start_coinbase_stream()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
