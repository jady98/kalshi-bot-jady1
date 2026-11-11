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
# Coinbase WebSocket | Velas de 1 minuto
# Solo lectura: NO envia ordenes a Kalshi
# ============================================================

SYMBOL = "BTC-USD"
CANDLE_LIMIT = 300

COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"

KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers.update({
    "User-Agent": "BitcoinRadarPRO/5.0"
})

lock = threading.Lock()
bars = deque(maxlen=CANDLE_LIMIT)

ws_status = {
    "connected": False,
    "error": "",
    "last_message": None
}

ws_started = False


def utc_now():
    return datetime.now(timezone.utc)


def add_trade(timestamp, price, size):
    """Agrega una operacion a su vela de un minuto."""
    try:
        ts = int(float(timestamp))
        price = float(price)
        size = float(size or 0)

        minute = ts - (ts % 60)

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
        print("Error agregando operacion:", exc)


def seed_candles():
    """Carga velas historicas iniciales desde Coinbase."""
    try:
        url = f"{COINBASE_REST}/products/{SYMBOL}/candles"

        response = session.get(
            url,
            params={"granularity": 60},
            timeout=12
        )
        response.raise_for_status()

        data = response.json()

        with lock:
            if bars:
                return

            for item in reversed(data):
                if len(item) < 6:
                    continue

                ts, low, high, open_, close, volume = item

                bars.append({
                    "time": int(ts),
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume)
                })

        print("Coinbase: velas historicas cargadas")

    except Exception as exc:
        print("Error cargando velas Coinbase:", exc)


def on_coinbase_open(ws):
    with lock:
        ws_status["connected"] = True
        ws_status["error"] = ""

    subscribe = {
        "type": "subscribe",
        "product_ids": [SYMBOL],
        "channels": ["ticker"]
    }

    ws.send(json.dumps(subscribe))
    print("Coinbase WebSocket conectado")


def on_coinbase_message(ws, message):
    try:
        data = json.loads(message)

        if data.get("type") != "ticker":
            return

        if data.get("product_id") != SYMBOL:
            return

        price = data.get("price")
        timestamp = data.get("time")

        if price is None or timestamp is None:
            return

        dt = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

        add_trade(
            dt.timestamp(),
            float(price),
            data.get("last_size", 0)
        )

        with lock:
            ws_status["last_message"] = utc_now().isoformat()

    except Exception as exc:
        print("Error leyendo Coinbase:", exc)


def on_coinbase_error(ws, error):
    with lock:
        ws_status["error"] = str(error)
        ws_status["connected"] = False

    print("Coinbase WebSocket error:", error)


def on_coinbase_close(ws, code, message):
    with lock:
        ws_status["connected"] = False

    print("Coinbase WebSocket desconectado:", code, message)


def coinbase_worker():
    while True:
        try:
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
            with lock:
                ws_status["connected"] = False
                ws_status["error"] = str(exc)

            print("Error en worker Coinbase:", exc)

        time.sleep(3)


def start_coinbase_stream():
    global ws_started

    if ws_started:
        return

    ws_started = True

    seed_candles()

    thread = threading.Thread(
        target=coinbase_worker,
        daemon=True
    )
    thread.start()


def get_market_data():
    """Devuelve las velas disponibles."""
    with lock:
        data = [dict(item) for item in bars]

    if not data:
        raise RuntimeError(
            "Esperando datos de Coinbase..."
        )

    df = pd.DataFrame(data)

    df["time"] = pd.to_datetime(
        df["time"],
        unit="s",
        utc=True
    )

    df = df.sort_values("time")
    df = df.drop_duplicates("time", keep="last")
    df = df.tail(CANDLE_LIMIT).reset_index(drop=True)

    return df


def calculate_indicators(df):
    df = df.copy()

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    # Medias exponenciales
    df["EMA9"] = close.ewm(
        span=9,
        adjust=False
    ).mean()

    df["EMA21"] = close.ewm(
        span=21,
        adjust=False
    ).mean()

    # VWAP
    typical = (high + low + close) / 3
    cumulative_volume = volume.cumsum()

    df["VWAP"] = (
        (typical * volume).cumsum()
        / cumulative_volume.replace(0, np.nan)
    )

    df["VWAP"] = df["VWAP"].fillna(close)

    # RSI 14
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
    df["RSI"] = df["RSI"].fillna(50)

    # MACD
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

    # Soporte y resistencia aproximados
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
        return {
            "direction": "SUBE",
            "strength": score,
            "description": "Tendencia alcista"
        }

    if score <= -3:
        return {
            "direction": "BAJA",
            "strength": abs(score),
            "description": "Tendencia bajista"
        }

    return {
        "direction": "NEUTRAL",
        "strength": abs(score),
        "description": "Sin confirmacion suficiente"
    }


def as_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def kalshi_market_data():
    """Consulta mercados de Kalshi. No envia ordenes."""
    try:
        response = session.get(
            f"{KALSHI_API}/markets",
            params={
                "series_ticker": KALSHI_SERIES,
                "limit": 5,
                "status": "open"
            },
            timeout=10
        )

        response.raise_for_status()
        payload = response.json()

        markets = payload.get("markets", [])

        if not markets:
            return {
                "available": False,
                "message": "No se encontraron mercados abiertos",
                "series": KALSHI_SERIES
            }

        market = markets[0]

        return {
            "available": True,
            "ticker": market.get("ticker"),
            "title": market.get("title"),
            "status": market.get("status"),
            "yes_bid": as_float(
                market.get("yes_bid_dollars")
            ),
            "yes_ask": as_float(
                market.get("yes_ask_dollars")
            ),
            "no_bid": as_float(
                market.get("no_bid_dollars")
            ),
            "no_ask": as_float(
                market.get("no_ask_dollars")
            ),
            "last_price": as_float(
                market.get("last_price_dollars")
            ),
            "close_time": market.get("close_time"),
            "series": KALSHI_SERIES
        }

    except Exception as exc:
        return {
            "available": False,
            "message": f"Error conectando a Kalshi: {exc}",
            "series": KALSHI_SERIES
        }


# Inicia el flujo de Coinbase al cargar la aplicacion
start_coinbase_stream()
# ============================================================
# INTERFAZ WEB
# ============================================================

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
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 14px;
    background: #080d18;
    color: #e8edf7;
    font-family: Arial, sans-serif;
}

header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 15px;
}

h1 {
    font-size: 21px;
    margin: 0;
}

.subtitle {
    color: #8795ad;
    font-size: 12px;
    margin-top: 5px;
}

.panel {
    background: #111a2a;
    border: 1px solid #233149;
    border-radius: 12px;
    padding: 14px;
    margin-bottom: 12px;
}

.label {
    color: #8795ad;
    font-size: 12px;
}

.price {
    font-size: 30px;
    font-weight: bold;
    margin-top: 7px;
}

.green {
    color: #00d69a;
}

.red {
    color: #ff5268;
}

.yellow {
    color: #ffc857;
}

.status {
    font-size: 12px;
    color: #8795ad;
}

.cards {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
}

.value {
    font-size: 19px;
    font-weight: bold;
    margin-top: 7px;
}

#chart {
    width: 100%;
    height: 390px;
}

.chart-title {
    font-size: 14px;
    font-weight: bold;
    margin-bottom: 10px;
}

.market-row {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 8px 0;
    border-bottom: 1px solid #263248;
    font-size: 13px;
}

.market-row:last-child {
    border-bottom: none;
}

.footer {
    color: #718098;
    font-size: 11px;
    text-align: center;
    margin: 18px 0;
}

@media (max-width: 420px) {
    body {
        padding: 9px;
    }

    #chart {
        height: 330px;
    }

    .price {
        font-size: 25px;
    }
}
</style>
</head>

<body>

<header>
    <div>
        <h1>BITCOIN RADAR PRO</h1>
        <div class="subtitle">
            BTC-USD · Coinbase · Velas de 1 minuto
        </div>
    </div>

    <div id="connection" class="status">
        Conectando Coinbase...
    </div>
</header>

<div class="panel">
    <div class="label">PRECIO BITCOIN</div>
    <div id="price" class="price">Cargando...</div>
    <div id="source" class="status">
        Esperando datos...
    </div>
</div>

<div class="cards">

    <div class="panel">
        <div class="label">SEÑAL DEL RADAR</div>
        <div id="signal" class="value yellow">
            NEUTRAL
        </div>
        <div id="signalDescription" class="status">
            Esperando indicadores
        </div>
    </div>

    <div class="panel">
        <div class="label">RSI 14</div>
        <div id="rsi" class="value">--</div>
        <div class="status">Periodo 14</div>
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
    <div class="chart-title">
        GRÁFICO BTC-USD · 1 MINUTO
    </div>
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

<div class="panel">
    <div class="chart-title">
        KALSHI BTC 15 MINUTOS
    </div>

    <div class="market-row">
        <span>Estado</span>
        <strong id="kalshiStatus">Consultando...</strong>
    </div>

    <div class="market-row">
        <span>Mercado</span>
        <strong id="kalshiTicker">--</strong>
    </div>

    <div class="market-row">
        <span>YES BID</span>
        <strong id="yesBid">--</strong>
    </div>

    <div class="market-row">
        <span>YES ASK</span>
        <strong id="yesAsk">--</strong>
    </div>

    <div class="market-row">
        <span>NO BID</span>
        <strong id="noBid">--</strong>
    </div>

    <div class="market-row">
        <span>NO ASK</span>
        <strong id="noAsk">--</strong>
    </div>

    <div id="kalshiMessage" class="status">
        Solo lectura. No se envian ordenes.
    </div>
</div>

<div class="footer">
    Actualizacion de datos cada segundo ·
    Señales tecnicas informativas, no garantizan resultados.
</div>

<script>
const $ = id => document.getElementById(id);

const chart = LightweightCharts.createChart(
    $("chart"),
    {
        width: $("chart").clientWidth,
        height: 390,

        layout: {
            background: {
                color: "#111a2a"
            },
            textColor: "#aab7ca"
        },

        grid: {
            vertLines: {
                color: "#202c40"
            },
            horzLines: {
                color: "#202c40"
            }
        },

        rightPriceScale: {
            borderColor: "#34435a"
        },

        timeScale: {
            borderColor: "#34435a",
            timeVisible: true,
            secondsVisible: false
        },

        crosshair: {
            mode: 1
        }
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
    if (value === null || value === undefined) {
        return "--";
    }

    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "--";
    }

    return "$" + n.toLocaleString(
        "en-US",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    );
}


function number(value) {
    if (value === null || value === undefined) {
        return "--";
    }

    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "--";
    }

    return n.toFixed(2);
}


function percent(value) {
    if (value === null || value === undefined) {
        return "--";
    }

    return Number(value).toFixed(2);
}


function setLine(series, values) {
    if (!Array.isArray(values)) {
        series.setData([]);
        return;
    }

    series.setData(values);
}


function updateKalshi(market) {
    if (!market) {
        $("kalshiStatus").textContent = "Sin datos";
        return;
    }

    if (!market.available) {
        $("kalshiStatus").textContent = "Desconectado";
        $("kalshiMessage").textContent =
            market.message || "No disponible";
        return;
    }

    $("kalshiStatus").textContent = "Disponible";
    $("kalshiTicker").textContent =
        market.ticker || "--";

    $("yesBid").textContent =
        money(market.yes_bid);

    $("yesAsk").textContent =
        money(market.yes_ask);

    $("noBid").textContent =
        money(market.no_bid);

    $("noAsk").textContent =
        money(market.no_ask);

    $("kalshiMessage").textContent =
        "Solo lectura. No se envian ordenes.";
}


async function updateAll() {
    try {
        const response = await fetch(
            "/api/data?_=" + Date.now(),
            {
                cache: "no-store"
            }
        );

        if (!response.ok) {
            throw new Error(
                "Error del servidor: " + response.status
            );
        }

        const data = await response.json();

        if (data.error) {
            throw new Error(data.error);
        }

        // Precio
        $("price").textContent = money(data.price);

        $("source").textContent =
            "Fuente: Coinbase · BTC-USD";

        // Conexion
        if (data.connection && data.connection.connected) {
            $("connection").textContent =
                "Coinbase conectado";

            $("connection").className =
                "status green";
        } else {
            $("connection").textContent =
                "Coinbase reconectando";

            $("connection").className =
                "status yellow";
        }

        // Señal
        const signal = data.signal || {};

        $("signal").textContent =
            signal.direction || "NEUTRAL";

        $("signalDescription").textContent =
            signal.description || "";

        $("signal").className = "value";

        if (signal.direction === "SUBE") {
            $("signal").classList.add("green");
        } else if (signal.direction === "BAJA") {
            $("signal").classList.add("red");
        } else {
            $("signal").classList.add("yellow");
        }

        // Indicadores
        $("rsi").textContent = percent(data.rsi);
        $("ema9").textContent = money(data.ema9_value);
        $("ema21").textContent = money(data.ema21_value);
        $("vwap").textContent = money(data.vwap_value);
        $("macd").textContent = number(data.macd_value);
        $("support").textContent = money(data.support);
        $("resistance").textContent = money(data.resistance);

        // Velas
        if (Array.isArray(data.candles)) {
            candleSeries.setData(data.candles);
        }

        setLine(ema9Series, data.ema9);
        setLine(ema21Series, data.ema21);
        setLine(vwapSeries, data.vwap);

        chart.timeScale().fitContent();

        // Kalshi
        updateKalshi(data.market);

    } catch (error) {
        $("source").textContent = error.message;

        $("connection").textContent =
            "Error de conexion";

        $("connection").className =
            "status red";

        console.error(error);
    }
}


function fitCharts() {
    chart.applyOptions({
        width: $("chart").clientWidth
    });
}


window.addEventListener("resize", fitCharts);

// Primera actualizacion
updateAll();

// Actualizacion cada segundo
setInterval(updateAll, 1000);

</script>

</body>
</html>
"""


# ============================================================
# RUTAS FLASK
# ============================================================

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/health")
def health():
    with lock:
        connected = ws_status["connected"]
        error = ws_status["error"]

    return jsonify({
        "ok": True,
        "coinbase_connected": connected,
        "error": error,
        "candles": len(bars)
    })


@app.route("/api/data")
def api_data():
    try:
        df = get_market_data()
        df = calculate_indicators(df)

        last = df.iloc[-1]

        def line(column):
            result = []

            for timestamp, value in zip(
                df["time"],
                df[column]
            ):
                if pd.isna(value):
                    continue

                result.append({
                    "time": int(timestamp.timestamp()),
                    "value": float(value)
                })

            return result

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

            "ema9": line("EMA9"),
            "ema21": line("EMA21"),
            "vwap": line("VWAP"),

            "connection": connection,

            "market": kalshi_market_data()
        })

    except Exception as exc:
        return jsonify({
            "error": str(exc)
        }), 502


# ============================================================
# INICIO
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
        )
