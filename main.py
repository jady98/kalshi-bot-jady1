import os
import json
import time
import threading
import base64
import logging
from collections import deque
from datetime import datetime, timezone

import requests
import websocket

from flask import Flask, jsonify, render_template_string

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ============================================================
# BITCOIN RADAR PRO BY JADY
# Datos BRTI + Kalshi BTC 15M
# SOLO LECTURA. NO ENVIA ORDENES.
# ============================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n").strip()

KALSHI_REST = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

SERIES_TICKER = "KXBTC15M"
INDEX_ID = "BRTI"

HTTP_TIMEOUT = 12
MAX_TICKS = 5000
MAX_CANDLES = 1500

lock = threading.RLock()

state = {
    "connected": False,
    "authenticated": False,
    "subscribed": False,
    "index_available": None,
    "last_price": None,
    "last_tick_time": None,
    "last_message_time": None,
    "average_60s": None,
    "average_15m": None,
    "error": None,
    "warning": None,
    "market": None,
    "signal": "ESPERAR",
    "signal_reason": "Esperando datos suficientes",
    "signal_strength": 0,
}

ticks = deque(maxlen=MAX_TICKS)
candles = deque(maxlen=MAX_CANDLES)

ws_thread_started = False
ws_app = None
ws_id_counter = 0


# ============================================================
# UTILIDADES
# ============================================================

def utc_now_ms():
    return int(time.time() * 1000)


def safe_float(value):
    try:
        if value is None:
            return None
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            return None
        return number
    except (TypeError, ValueError):
        return None


def parse_timestamp(value):
    if value is None:
        return utc_now_ms()

    try:
        if isinstance(value, (int, float)):
            number = int(value)
            return number if number > 10**11 else number * 1000

        if isinstance(value, str):
            value = value.strip()

            if value.isdigit():
                number = int(value)
                return number if number > 10**11 else number * 1000

            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(parsed.timestamp() * 1000)

    except Exception:
        pass

    return utc_now_ms()


def update_state(**values):
    with lock:
        state.update(values)


# ============================================================
# AUTENTICACION RSA PARA KALSHI WEBSOCKET
# ============================================================

def create_ws_headers():
    if not KALSHI_API_KEY_ID or not KALSHI_PRIVATE_KEY:
        raise RuntimeError(
            "Faltan KALSHI_API_KEY_ID o KALSHI_PRIVATE_KEY en Render"
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

    signature_b64 = base64.b64encode(signature).decode("utf-8")

    return [
        "KALSHI-ACCESS-KEY: " + KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP: " + timestamp,
        "KALSHI-ACCESS-SIGNATURE: " + signature_b64
    ]


# ============================================================
# REGISTRO DE PRECIOS BRTI
# ============================================================

def add_brti_tick(price, timestamp_ms):
    price = safe_float(price)

    if price is None or price <= 0:
        return

    timestamp_ms = parse_timestamp(timestamp_ms)

    with lock:
        if ticks and timestamp_ms <= ticks[-1]["time"]:
            return

        ticks.append({
            "time": timestamp_ms,
            "price": price
        })

        state["last_price"] = price
        state["last_tick_time"] = timestamp_ms
        state["last_message_time"] = utc_now_ms()
        state["error"] = None

        update_candles_locked()


def update_candles_locked():
    """
    Construye velas de un minuto usando exclusivamente
    los ticks recibidos del índice BRTI.
    """

    if not ticks:
        return

    for tick in list(ticks)[-2:]:
        minute = (tick["time"] // 60000) * 60000

        if candles and candles[-1]["time"] == minute:
            candle = candles[-1]
            candle["high"] = max(candle["high"], tick["price"])
            candle["low"] = min(candle["low"], tick["price"])
            candle["close"] = tick["price"]
        elif not candles or minute > candles[-1]["time"]:
            candles.append({
                "time": minute,
                "open": tick["price"],
                "high": tick["price"],
                "low": tick["price"],
                "close": tick["price"]
            })


# ============================================================
# RECEPCION DEL FEED CF BENCHMARKS
# ============================================================

def process_brti_message(packet):
    message = packet.get("msg", {})

    if packet.get("type") == "subscribed":
        update_state(subscribed=True)
        logging.info("Suscripcion WebSocket confirmada")
        return

    if packet.get("type") == "error":
        error_message = message.get("message") or str(message)
        update_state(error="Kalshi: " + error_message)
        logging.error("Error de Kalshi: %s", error_message)
        return

    if packet.get("type") == "cfbenchmarks_value_indexlist":
        available = INDEX_ID in message.get("index_ids", [])
        update_state(index_available=available)

        if not available:
            update_state(
                warning="BRTI no aparece disponible en este feed"
            )
        return

    if packet.get("type") != "cfbenchmarks_value":
        return

    if message.get("index_id") != INDEX_ID:
        return

    raw_data = message.get("data", {})

    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data)
        except json.JSONDecodeError:
            raw_data = {}

    price = safe_float(raw_data.get("value"))

    timestamp = (
        raw_data.get("time")
        or message.get("received_at")
        or packet.get("sending_ts_ms")
    )

    if price is not None:
        add_brti_tick(price, timestamp)

    average_60 = safe_float(
        (message.get("avg_60s_data") or {}).get("value")
    )

    average_15 = safe_float(
        (message.get("last_60s_windowed_average_15min") or {}).get("value")
    )

    with lock:
        if average_60 is not None:
            state["average_60s"] = average_60

        if average_15 is not None:
            state["average_15m"] = average_15


# ============================================================
# CONEXION WEBSOCKET
# ============================================================

def on_ws_open(ws):
    logging.info("WebSocket conectado. Solicitando BRTI...")

    update_state(
        connected=True,
        authenticated=True,
        subscribed=False,
        error=None
    )

    subscription = {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["cfbenchmarks_value"],
            "index_ids": ["BRTI"]
        }
    }

    ws.send(json.dumps(subscription))


def on_ws_message(ws, raw_message):
    try:
        packet = json.loads(raw_message)

        if not isinstance(packet, dict):
            return

        process_brti_message(packet)

    except Exception as error:
        logging.exception("Error procesando mensaje BRTI")
        update_state(warning=str(error))


def on_ws_error(ws, error):
    logging.error("WebSocket error: %s", error)

    update_state(
        connected=False,
        authenticated=False,
        error="WebSocket: " + str(error)
    )


def on_ws_close(ws, close_status_code, close_message):
    logging.warning(
        "WebSocket cerrado: %s | %s",
        close_status_code,
        close_message
    )

    update_state(
        connected=False,
        authenticated=False,
        subscribed=False
    )


def websocket_worker():
    global ws_app

    while True:
        try:
            headers = create_ws_headers()

            ws_app = websocket.WebSocketApp(
                KALSHI_WS,
                header=headers,
                on_open=on_ws_open,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close
            )

            ws_app.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as error:
            logging.exception("No se pudo iniciar WebSocket")
            update_state(
                connected=False,
                authenticated=False,
                error=str(error)
            )

        time.sleep(5)


def start_brti():
    global ws_thread_started

    if ws_thread_started:
        return

    ws_thread_started = True

    thread = threading.Thread(
        target=websocket_worker,
        daemon=True,
        name="BRTI-WebSocket"
    )

    thread.start()


# ============================================================
# INDICADORES TECNICOS
# ============================================================

def calculate_ema(values, period):
    if not values:
        return None

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period

    for value in values[period:]:
        ema = (value - ema) * multiplier + ema

    return ema


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None

    changes = [
        values[index] - values[index - 1]
        for index in range(1, len(values))
    ]

    recent = changes[-period:]

    gains = [max(change, 0) for change in recent]
    losses = [abs(min(change, 0)) for change in recent]

    average_gain = sum(gains) / period
    average_loss = sum(losses) / period

    if average_loss == 0:
        return 100.0

    relative_strength = average_gain / average_loss

    return 100 - (100 / (1 + relative_strength))


def calculate_signal():
    with lock:
        prices = [item["price"] for item in list(ticks)[-100:]]

    if len(prices) < 30:
        return {
            "signal": "ESPERAR",
            "reason": "Reuniendo datos BRTI",
            "strength": 0
        }

    ema9 = calculate_ema(prices, 9)
    ema21 = calculate_ema(prices, 21)
    rsi = calculate_rsi(prices, 14)

    if ema9 is None or ema21 is None or rsi is None:
        return {
            "signal": "ESPERAR",
            "reason": "Indicadores incompletos",
            "strength": 0
        }

    current = prices[-1]
    previous = prices[-6]

    momentum = current - previous

    score = 0
    reasons = []

    if ema9 > ema21:
        score += 2
        reasons.append("EMA 9 sobre EMA 21")
    elif ema9 < ema21:
        score -= 2
        reasons.append("EMA 9 bajo EMA 21")

    if momentum > 0:
        score += 1
        reasons.append("Impulso positivo")
    elif momentum < 0:
        score -= 1
        reasons.append("Impulso negativo")

    if 50 < rsi < 70:
        score += 1
        reasons.append("RSI favorece subida")
    elif 30 < rsi < 50:
        score -= 1
        reasons.append("RSI favorece bajada")

    if score >= 3:
        signal = "SUBE"
    elif score <= -3:
        signal = "BAJA"
    else:
        signal = "ESPERAR"

    return {
        "signal": signal,
        "reason": " | ".join(reasons),
        "strength": min(abs(score) * 20, 100),
        "ema9": ema9,
        "ema21": ema21,
        "rsi": rsi,
        "momentum": momentum
    }
    # ============================================================
# INTERFAZ BITCOIN RADAR PRO
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">

<title>Bitcoin Radar PRO by Jady</title>

<script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #080b14;
    color: #f4f6ff;
    font-family: Arial, sans-serif;
}

header {
    padding: 20px;
    text-align: center;
    background: #101629;
    border-bottom: 1px solid #28334e;
}

header h1 {
    margin: 0;
    color: #00e5ff;
    font-size: 25px;
    letter-spacing: 1px;
}

header p {
    color: #8995b5;
    font-size: 12px;
}

.container {
    width: 100%;
    max-width: 1100px;
    margin: auto;
    padding: 14px;
}

.grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
}

.card {
    background: #11182a;
    border: 1px solid #263653;
    border-radius: 13px;
    padding: 15px;
    min-width: 0;
}

.label {
    color: #8997b8;
    font-size: 12px;
    margin-bottom: 8px;
}

.value {
    font-size: 23px;
    font-weight: bold;
    overflow-wrap: anywhere;
}

.price {
    color: #00e5ff;
    font-size: 29px;
}

.green {
    color: #00e59b;
}

.red {
    color: #ff5577;
}

.yellow {
    color: #ffc857;
}

.status {
    display: inline-block;
    padding: 7px 11px;
    border-radius: 20px;
    background: #263653;
    font-size: 12px;
}

.signal-box {
    text-align: center;
    margin-top: 13px;
    padding: 20px;
    border-radius: 14px;
    background: #11182a;
    border: 1px solid #263653;
}

.signal {
    font-size: 34px;
    font-weight: bold;
    margin: 8px;
}

#chart {
    height: 350px;
    width: 100%;
    margin-top: 12px;
}

.chart-card,
.market-card {
    margin-top: 13px;
}

.market-row {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 9px 0;
    border-bottom: 1px solid #263653;
    font-size: 13px;
}

footer {
    text-align: center;
    color: #66728f;
    font-size: 11px;
    padding: 22px;
}

@media(max-width: 480px) {
    .value {
        font-size: 19px;
    }

    .price {
        font-size: 24px;
    }

    #chart {
        height: 290px;
    }
}
</style>
</head>

<body>

<header>
    <h1>₿ BITCOIN RADAR PRO</h1>
    <p>BY JADY ♛ | KALSHI BTC 15 MIN</p>
    <span class="status" id="connection">CONECTANDO...</span>
</header>

<div class="container">

    <div class="grid">

        <div class="card">
            <div class="label">PRECIO BRTI</div>
            <div class="value price" id="btcPrice">--</div>
            <div class="label">Índice de referencia</div>
        </div>

        <div class="card">
            <div class="label">PROMEDIO 60 SEGUNDOS</div>
            <div class="value" id="avg60">--</div>
            <div class="label">Datos recibidos</div>
        </div>

        <div class="card">
            <div class="label">PROMEDIO 15 MINUTOS</div>
            <div class="value" id="avg15">--</div>
            <div class="label">Promedio de ticks recibidos</div>
        </div>

        <div class="card">
            <div class="label">FUERZA DE SEÑAL</div>
            <div class="value" id="strength">0%</div>
            <div class="label">Indicador técnico</div>
        </div>

    </div>

    <div class="signal-box">
        <div class="label">SEÑAL DEL RADAR</div>
        <div class="signal yellow" id="signal">ESPERAR</div>
        <div id="reason">Esperando datos suficientes</div>
    </div>

    <div class="card chart-card">
        <div class="label">GRÁFICA BRTI · VELAS DE 1 MINUTO</div>
        <div id="chart"></div>
    </div>

    <div class="card market-card">
        <div class="label">CONTRATO KALSHI BTC 15M</div>

        <div class="market-row">
            <span>Contrato</span>
            <strong id="ticker">--</strong>
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

        <div class="market-row">
            <span>Último precio</span>
            <strong id="lastPrice">--</strong>
        </div>

        <div class="market-row">
            <span>Cierre del contrato</span>
            <strong id="closeTime">--</strong>
        </div>
    </div>

    <div class="card market-card">
        <div class="label">ESTADO DEL SISTEMA</div>
        <div id="systemStatus">Iniciando...</div>
        <div id="systemError" class="red"></div>
    </div>

</div>

<footer>
    BITCOIN RADAR PRO BY JADY · SOLO LECTURA · SIN OPERACIONES AUTOMÁTICAS
</footer>

<script>
const chartElement = document.getElementById("chart");

const chart = LightweightCharts.createChart(chartElement, {
    layout: {
        background: { color: "#11182a" },
        textColor: "#aab6d3"
    },
    grid: {
        vertLines: { color: "#202c42" },
        horzLines: { color: "#202c42" }
    },
    rightPriceScale: {
        borderColor: "#34415c"
    },
    timeScale: {
        borderColor: "#34415c",
        timeVisible: true,
        secondsVisible: false
    }
});

const candleSeries = chart.addSeries(
    LightweightCharts.CandlestickSeries,
    {
        upColor: "#00d99a",
        downColor: "#ff5577",
        borderUpColor: "#00d99a",
        borderDownColor: "#ff5577",
        wickUpColor: "#00d99a",
        wickDownColor: "#ff5577"
    }
);

const ema9Series = chart.addSeries(
    LightweightCharts.LineSeries,
    {
        color: "#00e5ff",
        lineWidth: 2,
        title: "EMA 9"
    }
);

const ema21Series = chart.addSeries(
    LightweightCharts.LineSeries,
    {
        color: "#ffbd59",
        lineWidth: 2,
        title: "EMA 21"
    }
);

function money(value) {
    if (value === null || value === undefined) return "--";

    return Number(value).toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function cents(value) {
    if (value === null || value === undefined) return "--";
    return (Number(value) * 100).toFixed(1) + "¢";
}

function updateSignal(signal) {
    const element = document.getElementById("signal");

    element.textContent = signal;
    element.className = "signal";

    if (signal === "SUBE") {
        element.classList.add("green");
    } else if (signal === "BAJA") {
        element.classList.add("red");
    } else {
        element.classList.add("yellow");
    }
}

function updateChart(rows) {
    if (!Array.isArray(rows)) return;

    const valid = rows.filter(row =>
        Number.isFinite(row.time) &&
        Number.isFinite(row.open) &&
        Number.isFinite(row.high) &&
        Number.isFinite(row.low) &&
        Number.isFinite(row.close)
    );

    if (!valid.length) return;

    candleSeries.setData(valid.map(row => ({
        time: row.time,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close
    })));

    ema9Series.setData(valid
        .filter(row => row.ema9 !== null)
        .map(row => ({
            time: row.time,
            value: row.ema9
        }))
    );

    ema21Series.setData(valid
        .filter(row => row.ema21 !== null)
        .map(row => ({
            time: row.time,
            value: row.ema21
        }))
    );
}

async function updateRadar() {
    try {
        const response = await fetch("/api/data", {
            cache: "no-store"
        });

        if (!response.ok) {
            throw new Error("Error consultando los datos");
        }

        const data = await response.json();

        document.getElementById("btcPrice").textContent =
            money(data.price);

        document.getElementById("avg60").textContent =
            money(data.average_60s);

        document.getElementById("avg15").textContent =
            money(data.average_15m);

        document.getElementById("strength").textContent =
            (data.strength || 0) + "%";

        updateSignal(data.signal || "ESPERAR");

        document.getElementById("reason").textContent =
            data.reason || "Sin información";

        document.getElementById("connection").textContent =
            data.connected ? "BRTI EN VIVO" : "SIN TICKS RECIENTES";

        document.getElementById("systemStatus").textContent =
            data.connected
                ? "Conectado y recibiendo datos"
                : "Esperando datos BRTI";

        document.getElementById("systemError").textContent =
            data.error || data.warning || "";

        updateChart(data.candles);

    } catch (error) {
        document.getElementById("systemStatus").textContent =
            "Error actualizando el radar";

        document.getElementById("systemError").textContent =
            error.message;
    }
}

async function updateKalshi() {
    try {
        const response = await fetch("/api/kalshi", {
            cache: "no-store"
        });

        if (!response.ok) {
            throw new Error("No se pudo consultar Kalshi");
        }

        const market = await response.json();

        if (!market || market.error) {
            document.getElementById("ticker").textContent =
                "Sin contrato disponible";
            return;
        }

        document.getElementById("ticker").textContent =
            market.ticker || "--";

        document.getElementById("yesBid").textContent =
            cents(market.yes_bid);

        document.getElementById("yesAsk").textContent =
            cents(market.yes_ask);

        document.getElementById("noBid").textContent =
            cents(market.no_bid);

        document.getElementById("noAsk").textContent =
            cents(market.no_ask);

        document.getElementById("lastPrice").textContent =
            cents(market.last_price);

        document.getElementById("closeTime").textContent =
            market.close_time || "--";

    } catch (error) {
        document.getElementById("ticker").textContent =
            "Error consultando Kalshi";
    }
}

function resizeChart() {
    chart.applyOptions({
        width: chartElement.clientWidth
    });
}

window.addEventListener("resize", resizeChart);

updateRadar();
updateKalshi();

setInterval(updateRadar, 2000);
setInterval(updateKalshi, 10000);
</script>

</body>
</html>
"""


# ============================================================
# RUTAS DE LA APLICACION
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():
    return jsonify(build_data())


@app.route("/api/kalshi")
def api_kalshi():
    market = get_kalshi_market()

    if market is None:
        return jsonify({
            "error": "No se encontró un contrato abierto"
        })

    return jsonify(market)


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "Bitcoin Radar PRO by Jady",
        "time": now_ms()
    })


# ============================================================
# INICIO
# ============================================================

start_brti()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
