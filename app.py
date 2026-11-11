
import json
import threading
import time
from collections import deque

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ==================================================
# CONFIGURACION
# ==================================================

PRODUCT_ID = "BTC-USD"
REST_URL = "https://api.exchange.coinbase.com"
WS_URL = "wss://advanced-trade-ws.coinbase.com"

MAX_CANDLES = 300
STALE_SECONDS = 20

lock = threading.RLock()

candles = deque(maxlen=MAX_CANDLES)
current_price = None
last_update = 0
last_error = ""
last_source = "NINGUNA"
ws_connected = False
services_started = False

session = requests.Session()
session.headers.update({
    "User-Agent": "BitcoinRadar/3.0",
    "Accept": "application/json"
})


# ==================================================
# ACTUALIZAR PRECIO Y VELAS
# ==================================================

def update_price(price, source="COINBASE"):
    global current_price, last_update, last_source

    price = float(price)
    if price <= 0:
        return

    now = int(time.time())
    minute = now - (now % 60)

    with lock:
        current_price = price
        last_update = now
        last_source = source

        if candles and candles[-1]["time"] == minute:
            candle = candles[-1]
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
        else:
            candles.append({
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0
            })


# ==================================================
# HISTORIAL DE COINBASE
# ==================================================

def load_history():
    global last_error

    try:
        response = session.get(
            f"{REST_URL}/products/{PRODUCT_ID}/candles",
            params={"granularity": 60},
            timeout=15
        )
        response.raise_for_status()

        raw = response.json()
        history = []

        for item in raw:
            if len(item) < 6:
                continue

            history.append({
                "time": int(item[0]),
                "low": float(item[1]),
                "high": float(item[2]),
                "open": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5])
            })

        history.sort(key=lambda x: x["time"])

        with lock:
            existing = {
                c["time"]: dict(c)
                for c in candles
            }

            for candle in history:
                if candle["time"] not in existing:
                    existing[candle["time"]] = candle

            merged = sorted(
                existing.values(),
                key=lambda x: x["time"]
            )

            candles.clear()
            candles.extend(merged[-MAX_CANDLES:])
            last_error = ""

    except Exception as exc:
        with lock:
            last_error = "Historial: " + str(exc)


# ==================================================
# ACTUALIZACION MANUAL REAL
# ==================================================

def force_coinbase_update():
    global last_error

    try:
        response = session.get(
            f"{REST_URL}/products/{PRODUCT_ID}/ticker",
            timeout=10
        )
        response.raise_for_status()

        data = response.json()
        price = float(data["price"])

        update_price(price, "ACTUALIZACION MANUAL")

        with lock:
            last_error = ""

        return True

    except Exception as exc:
        with lock:
            last_error = "Actualización manual: " + str(exc)

        return False


# ==================================================
# WEBSOCKET COINBASE
# ==================================================

def on_open(ws):
    global ws_connected, last_error

    try:
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channel": "ticker"
        }))

        with lock:
            ws_connected = True
            last_error = ""

    except Exception as exc:
        with lock:
            ws_connected = False
            last_error = "Suscripción: " + str(exc)


def on_message(ws, message):
    global last_error

    try:
        data = json.loads(message)

        if data.get("channel") != "ticker":
            return

        for event in data.get("events", []):
            for ticker in event.get("tickers", []):

                if ticker.get("product_id") != PRODUCT_ID:
                    continue

                price = ticker.get("price")

                if price is not None:
                    update_price(price, "WEBSOCKET")

                    with lock:
                        last_error = ""

    except Exception as exc:
        with lock:
            last_error = "Datos WebSocket: " + str(exc)


def on_error(ws, error):
    global ws_connected, last_error

    with lock:
        ws_connected = False
        last_error = "WebSocket: " + str(error)


def on_close(ws, close_status_code, close_msg):
    global ws_connected

    with lock:
        ws_connected = False


def websocket_worker():
    global ws_connected, last_error

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            with lock:
                last_error = "Reconexión: " + str(exc)
                ws_connected = False

        with lock:
            ws_connected = False

        time.sleep(5)


# ==================================================
# RESPALDO REST
# ==================================================

def rest_fallback_worker():
    global last_error

    while True:
        try:
            with lock:
                updated = last_update

            if time.time() - updated > 8:
                response = session.get(
                    f"{REST_URL}/products/{PRODUCT_ID}/ticker",
                    timeout=10
                )
                response.raise_for_status()

                data = response.json()
                update_price(float(data["price"]), "REST")

        except Exception as exc:
            with lock:
                last_error = "Respaldo REST: " + str(exc)

        time.sleep(5)


# ==================================================
# SERVICIOS EN SEGUNDO PLANO
# ==================================================

def start_background_services():
    global services_started

    with lock:
        if services_started:
            return

        services_started = True

    threading.Thread(
        target=load_history,
        daemon=True
    ).start()

    threading.Thread(
        target=websocket_worker,
        daemon=True
    ).start()

    threading.Thread(
        target=rest_fallback_worker,
        daemon=True
    ).start()


# ==================================================
# ANALISIS TECNICO
# ==================================================

def calculate_signal(price, history):

    closes = [
        float(c["close"])
        for c in history
        if c.get("close") is not None
    ]

    if len(closes) < 22:
        return {
            "direction": "ESPERANDO DATOS",
            "signal": "ESPERAR",
            "prediction": round(price, 2),
            "confidence": 0
        }

    def ema(values, period):
        alpha = 2 / (period + 1)
        result = values[0]

        for value in values[1:]:
            result = alpha * value + (1 - alpha) * result

        return result

    ema9 = ema(closes[-40:], 9)
    ema21 = ema(closes[-80:], 21)

    momentum = (closes[-1] / closes[-6]) - 1

    projected_change = max(
        -0.003,
        min(0.003, momentum * 2.5)
    )

    prediction = price * (1 + projected_change)

    # Direccion predominante sin señal NEUTRAL
    if ema9 > ema21:
        direction = "ALCISTA"
        signal = "SUBE"
        strength = max(0, momentum * 10000)
    elif ema9 < ema21:
        direction = "BAJISTA"
        signal = "BAJA"
        strength = max(0, -momentum * 10000)
    elif momentum >= 0:
        direction = "ALCISTA"
        signal = "SUBE"
        strength = momentum * 10000
    else:
        direction = "BAJISTA"
        signal = "BAJA"
        strength = -momentum * 10000

    confidence = min(85, 50 + int(strength))

    return {
        "direction": direction,
        "signal": signal,
        "prediction": round(prediction, 2),
        "confidence": confidence
    }


# ==================================================
# API PRINCIPAL
# ==================================================

def get_api_data():

    with lock:
        price = current_price
        history = [dict(c) for c in candles]
        updated = last_update
        error = last_error
        source = last_source
        socket_status = ws_connected

    now = int(time.time())

    live = bool(
        price is not None
        and updated > 0
        and now - updated <= STALE_SECONDS
    )

    countdown = 900 - (now % 900)

    if price is None:
        return {
            "ok": False,
            "connected": False,
            "status": "ESPERANDO COINBASE",
            "price": None,
            "candles": history,
            "countdown": countdown,
            "error": error
        }

    analysis = calculate_signal(price, history)

    return {
        "ok": True,
        "connected": live,
        "websocket_connected": socket_status,
        "status": "EN VIVO" if live else "SIN DATOS RECIENTES",
        "price": round(price, 2),
        "direction": analysis["direction"],
        "signal": analysis["signal"],
        "prediction": analysis["prediction"],
        "confidence": analysis["confidence"],
        "countdown": countdown,
        "last_update": updated,
        "source": source,
        "candles": history[-MAX_CANDLES:],
        "error": error
    }


@app.route("/")
def home():
    start_background_services()
    return render_template_string(HTML)


@app.route("/api")
def api():
    start_background_services()
    return jsonify(get_api_data())


@app.route("/refresh")
def refresh():
    start_background_services()

    success = force_coinbase_update()
    data = get_api_data()
    data["manual_refresh"] = success

    return jsonify(data)


@app.route("/health")
def health():

    with lock:
        price = current_price
        updated = last_update
        error = last_error
        source = last_source
        count = len(candles)
        socket_status = ws_connected

    live = bool(
        price is not None
        and updated > 0
        and time.time() - updated <= STALE_SECONDS
    )

    return jsonify({
        "ok": True,
        "coinbase_live": live,
        "websocket_connected": socket_status,
        "price": price,
        "last_update": updated,
        "source": source,
        "candles": count,
        "error": error
    })


# ==================================================
# PRUEBA DE POLYMARKET
# ==================================================

@app.route("/polymarket-test")
def polymarket_test():

    url = "https://gateway.polymarket.us/v1/events"

    try:
        response = requests.get(
            url,
            params={
                "limit": 100,
                "offset": 0,
                "active": "true",
                "closed": "false"
            },
            headers={"Accept": "application/json"},
            timeout=15
        )

        response.raise_for_status()
        data = response.json()

        if isinstance(data, list):
            events = data
        elif isinstance(data, dict):
            events = data.get("events", [])
        else:
            events = []

        matches = []

        for event in events:
            slug = str(event.get("slug", "")).lower()
            title = str(event.get("title", "")).lower()
            search_text = slug + " " + title

            if (
                ("btc" in search_text or "bitcoin" in search_text)
                and "15m" in search_text
                and "updown" in search_text
                and event.get("active") is not False
                and event.get("closed") is not True
            ):
                matches.append({
                    "slug": event.get("slug"),
                    "title": event.get("title"),
                    "startDate": event.get("startDate"),
                    "endDate": event.get("endDate"),
                    "active": event.get("active"),
                    "closed": event.get("closed")
                })

        return jsonify({
            "ok": True,
            "total_events": len(events),
            "matches": matches
        })

    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc)
        }), 500


# ==================================================
# INTERFAZ VISUAL
# ==================================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar Live</title>

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 16px;
    background: #0b1020;
    color: #f4f6ff;
    font-family: Arial, sans-serif;
}

main {
    max-width: 650px;
    margin: auto;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    margin-bottom: 18px;
}

h1 {
    font-size: 21px;
    margin: 0;
}

.status {
    font-size: 12px;
    color: #ffbd59;
}

.card {
    background: #151d32;
    border: 1px solid #26334e;
    border-radius: 15px;
    padding: 18px;
    margin-bottom: 14px;
}

.label {
    color: #9eacc7;
    font-size: 12px;
    letter-spacing: 1px;
}

.price {
    font-size: 34px;
    font-weight: bold;
    margin-top: 8px;
}

.signal {
    text-align: center;
    font-size: 30px;
    font-weight: bold;
    padding: 15px;
    margin-top: 8px;
}

.up {
    color: #25d695;
}

.down {
    color: #ff6475;
}

.prediction {
    font-size: 27px;
    font-weight: bold;
    margin-top: 8px;
}

.countdown {
    font-size: 32px;
    font-weight: bold;
    margin-top: 8px;
    font-variant-numeric: tabular-nums;
}

canvas {
    width: 100%;
    height: 300px;
    display: block;
    touch-action: none;
}

.small {
    color: #93a1bd;
    font-size: 12px;
    line-height: 1.6;
}

.row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
}

.value {
    font-weight: bold;
    margin-top: 6px;
}

button {
    border: 0;
    border-radius: 9px;
    padding: 10px 14px;
    color: white;
    background: #294a83;
    font-weight: bold;
    cursor: pointer;
}

button:disabled {
    opacity: 0.5;
}

.chart-controls {
    display: flex;
    gap: 8px;
    margin: 12px 0;
}

.chart-controls button {
    min-width: 45px;
}
</style>
</head>

<body>
<main>

<section class="card">

    <div class="label">BITCOIN / USD · COINBASE</div>
    <div id="price" class="price">Cargando...</div>
    <div class="small">Precio real recibido de Coinbase</div>

</section>

<section class="card">

    <div class="label">RADAR DE DIRECCIÓN</div>

    <div id="direction" class="signal">
        ESPERANDO DATOS
    </div>

    <div class="row">
        <div>
            <div class="label">SEÑAL</div>
            <div id="signal" class="value">ESPERAR</div>
        </div>

        <div>
            <div class="label">CONFIANZA TÉCNICA</div>
            <div id="confidence" class="value">--</div>
        </div>
    </div>

</section>

<section class="card">
    <div class="label">PRECIO ESTIMADO AL CIERRE</div>
    <div id="prediction" class="prediction">--</div>
    <div class="small">
        Proyección experimental. No garantiza el precio futuro.
    </div>
</section>

<section class="card">
    <div class="label">TIEMPO RESTANTE · 15 MINUTOS</div>
    <div id="countdown" class="countdown">15:00</div>
</section>

<section class="card">

    <div class="row">
        <div class="label">VELAS DE 1 MINUTO</div>

        <button id="refreshButton" onclick="manualRefresh()">
            ACTUALIZAR
        </button>
    </div>

    <div class="chart-controls">
        <button onclick="zoomIn()">+</button>
        <button onclick="zoomOut()">−</button>
        <button onclick="resetZoom()">AJUSTAR</button>
        <button onclick="goLive()">EN VIVO</button>
    </div>

    <canvas id="chart" width="900" height="360"></canvas>

    <div class="small">
        Usa dos dedos para acercar o alejar.
        Desliza para revisar velas anteriores.
    </div>

</section>

<section class="card">

    <div class="label">ESTADO DE DATOS</div>

    <div id="source" class="value">Esperando...</div>
    <div id="socket" class="small">WebSocket: comprobando</div>
    <div id="updated" class="small">Esperando actualización...</div>
    <div id="error" class="small"></div>

</section>

</main>

<script>

const money = value =>
    Number(value).toLocaleString("en-US", {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 2
    });

function setText(id, value) {
    document.getElementById(id).textContent = value;
}

function formatTime(seconds) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;

    return String(m).padStart(2, "0") +
        ":" + String(s).padStart(2, "0");
}


// ==================================================
// CONTROL DEL ZOOM
// ==================================================

let chartData = [];
let zoomLevel = 1;
let chartOffset = 0;

function zoomIn() {
    zoomLevel = Math.min(8, zoomLevel * 1.3);
    drawChart(chartData);
}

function zoomOut() {
    zoomLevel = Math.max(0.5, zoomLevel / 1.3);
    drawChart(chartData);
}

function resetZoom() {
    zoomLevel = 1;
    chartOffset = 0;
    drawChart(chartData);
}

function goLive() {
    chartOffset = 0;
    drawChart(chartData);
}


// ==================================================
// GRAFICA DE VELAS
// ==================================================

function drawChart(data) {

    chartData = data || [];

    const canvas = document.getElementById("chart");
    const ctx = canvas.getContext("2d");

    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    if (!chartData.length) {
        ctx.fillStyle = "#93a1bd";
        ctx.fillText("Esperando velas...", 25, 40);
        return;
    }

    const visibleCount = Math.max(
        10,
        Math.min(
            chartData.length,
            Math.floor(100 / zoomLevel)
        )
    );

    const maxOffset = Math.max(
        0,
        chartData.length - visibleCount
    );

    chartOffset = Math.max(
        0,
        Math.min(chartOffset, maxOffset)
    );

    const end = chartData.length - chartOffset;
    const start = Math.max(0, end - visibleCount);

    const visible = chartData.slice(start, end);

    const values = visible.flatMap(c => [
        Number(c.high),
        Number(c.low)
    ]);

    let min = Math.min(...values);
    let max = Math.max(...values);

    const padding = Math.max((max - min) * 0.12, 1);

    min -= padding;
    max += padding;

    const top = 20;
    const bottom = height - 25;
    const usableHeight = bottom - top;

    const y = price =>
        bottom - ((price - min) / (max - min)) * usableHeight;

    ctx.strokeStyle = "#26334e";
    ctx.lineWidth = 1;
    ctx.font = "13px Arial";

    for (let i = 0; i < 5; i++) {

        const yy = top + (usableHeight / 4) * i;

        ctx.beginPath();
        ctx.moveTo(0, yy);
        ctx.lineTo(width, yy);
        ctx.stroke();

        const priceLabel =
            max - ((max - min) / 4) * i;

        ctx.fillStyle = "#93a1bd";
        ctx.fillText(
            money(priceLabel),
            8,
            yy - 4
        );
    }

    const chartLeft = 100;
    const chartWidth = width - chartLeft - 10;
    const step = chartWidth / visible.length;

    const candleWidth = Math.max(
        3,
        Math.min(24, step * 0.65)
    );

    visible.forEach((c, i) => {

        const x = chartLeft + i * step + step / 2;

        const rising = c.close >= c.open;

        const color = rising
            ? "#25d695"
            : "#ff6475";

        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        ctx.lineWidth = 2;

        ctx.beginPath();
        ctx.moveTo(x, y(c.high));
        ctx.lineTo(x, y(c.low));
        ctx.stroke();

        const openY = y(c.open);
        const closeY = y(c.close);

        ctx.fillRect(
            x - candleWidth / 2,
            Math.min(openY, closeY),
            candleWidth,
            Math.max(2, Math.abs(closeY - openY))
        );

    });

}


// ==================================================
// ZOOM CON DOS DEDOS Y DESPLAZAMIENTO
// ==================================================

let pointers = new Map();
let initialDistance = 0;
let initialZoom = 1;
let previousX = 0;

const canvas = document.getElementById("chart");

function distance(a, b) {
    return Math.hypot(
        a.x - b.x,
        a.y - b.y
    );
}

canvas.addEventListener("pointerdown", event => {

    canvas.setPointerCapture(event.pointerId);

    pointers.set(event.pointerId, {
        x: event.clientX,
        y: event.clientY
    });

    if (pointers.size === 1) {
        previousX = event.clientX;
    }

    if (pointers.size === 2) {
        const points = Array.from(pointers.values());

        initialDistance = distance(
            points[0],
            points[1]
        );

        initialZoom = zoomLevel;
    }

});

canvas.addEventListener("pointermove", event => {

    if (!pointers.has(event.pointerId)) return;

    pointers.set(event.pointerId, {
        x: event.clientX,
        y: event.clientY
    });

    if (pointers.size === 2) {

        const points = Array.from(pointers.values());

        const currentDistance = distance(
            points[0],
            points[1]
        );

        if (initialDistance > 0) {

            zoomLevel = Math.max(
                0.5,
                Math.min(
                    8,
                    initialZoom *
                    currentDistance / initialDistance
                )
            );

            drawChart(chartData);
        }

    } else if (pointers.size === 1) {

        const delta = event.clientX - previousX;
        previousX = event.clientX;

        const visibleCount = Math.max(
            10,
            Math.floor(100 / zoomLevel)
        );

        const pixelsPerCandle =
            canvas.clientWidth / visibleCount;

        chartOffset += Math.round(
            -delta / Math.max(1, pixelsPerCandle)
        );

        drawChart(chartData);
    }

});

function releasePointer(event) {
    pointers.delete(event.pointerId);

    if (pointers.size < 2) {
        initialDistance = 0;
    }
}

canvas.addEventListener("pointerup", releasePointer);
canvas.addEventListener("pointercancel", releasePointer);


// ==================================================
// RECIBIR DATOS AUTOMATICAMENTE
// ==================================================

let requestInProgress = false;

async function refreshData(manual = false) {

    if (requestInProgress) return;

    requestInProgress = true;

    const button = document.getElementById("refreshButton");

    if (manual) {
        button.disabled = true;
        button.textContent = "ACTUALIZANDO...";
    }

    try {

        const endpoint = manual ? "/refresh" : "/api";

        const response = await fetch(
            endpoint + "?_=" + Date.now(),
            { cache: "no-store" }
        );

        if (!response.ok) {
            throw new Error("HTTP " + response.status);
        }

        const data = await response.json();

        if (data.price !== undefined && data.price !== null) {

            setText("price", money(data.price));
            setText("prediction", money(data.prediction));
            setText("direction", data.direction);
            setText("signal", data.signal);

            setText(
                "confidence",
                data.confidence + "%"
            );

            const direction =
                document.getElementById("direction");

            direction.className =
                "signal " +
                (data.direction === "ALCISTA"
                    ? "up"
                    : data.direction === "BAJISTA"
                    ? "down"
                    : "");

            drawChart(data.candles);
        }

        setText(
            "countdown",
            formatTime(data.countdown || 0)
        );

        const status = document.getElementById("status");

        status.textContent = data.connected
            ? "● EN VIVO"
            : "● SIN DATOS";

        status.style.color = data.connected
            ? "#25d695"
            : "#ff6475";

        setText(
            "source",
            "Fuente: " + (data.source || "NINGUNA")
        );

        setText(
            "socket",
            "WebSocket: " +
            (data.websocket_connected
                ? "CONECTADO"
                : "DESCONECTADO / RESPALDO REST")
        );

        if (data.last_update) {

            setText(
                "updated",
                "Último dato: " +
                new Date(
                    data.last_update * 1000
                ).toLocaleTimeString()
            );
        }

        if (manual) {
            setText(
                "error",
                data.manual_refresh
                    ? "Actualización manual completada."
                    : (data.error || "No se pudo actualizar Coinbase.")
            );
        } else {
            setText("error", data.error || "");
        }

    } catch (error) {

        setText("status", "ERROR DE CONEXIÓN");
        setText("updated", String(error));

    } finally {

        requestInProgress = false;

        if (manual) {
            button.disabled = false;
            button.textContent = "ACTUALIZAR";
        }
    }
}

function manualRefresh() {
    refreshData(true);
}


// ==================================================
// INICIO
// ==================================================

refreshData();

setInterval(() => {
    refreshData(false);
}, 1000);

</script>

</body>
</html>
"""


# ==================================================
# INICIAR SERVIDOR
# ==================================================

if __name__ == "__main__":

    start_background_services()

    app.run(
        host="0.0.0.0",
        port=10000,
        threaded=True
    )
    
