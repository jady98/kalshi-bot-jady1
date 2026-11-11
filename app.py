import json
import threading
import time

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ==========================================
# BITCOIN RADAR LIVE - COINBASE
# Solo lectura. No envia ordenes.
# ==========================================

PRODUCT_ID = "BTC-USD"
REST_URL = "https://api.exchange.coinbase.com"
WS_URL = "wss://advanced-trade-ws.coinbase.com"

lock = threading.RLock()

candles = []
current_price = None
last_update = 0
connected = False
last_error = ""
ws_thread_started = False


def load_history():
    global candles, current_price, last_error

    try:
        response = requests.get(
            f"{REST_URL}/products/{PRODUCT_ID}/candles",
            params={"granularity": 60},
            headers={"User-Agent": "BitcoinRadar/1.0"},
            timeout=15,
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
                "volume": float(item[5]),
            })

        history.sort(key=lambda x: x["time"])

        with lock:
            candles = history[-300:]
            if candles:
                current_price = candles[-1]["close"]

    except Exception as exc:
        with lock:
            last_error = f"Error cargando historial: {exc}"


def update_price(price):
    global current_price, last_update

    now = int(time.time())
    minute = now - (now % 60)

    with lock:
        current_price = price
        last_update = now

        if not candles or candles[-1]["time"] != minute:
            candles.append({
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
            })
        else:
            candle = candles[-1]
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price

        if len(candles) > 300:
            del candles[:-300]


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

                price = float(ticker["price"])

                if price > 0:
                    update_price(price)
                    with lock:
                        last_error = ""

    except Exception as exc:
        with lock:
            last_error = f"Error procesando datos: {exc}"


def on_open(ws):
    global last_error

    try:
        subscription = {
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channel": "ticker",
        }

        ws.send(json.dumps(subscription))

        with lock:
            last_error = ""

    except Exception as exc:
        with lock:
            last_error = f"Error de suscripcion: {exc}"


def on_error(ws, error):
    global last_error, connected

    with lock:
        connected = False
        last_error = str(error)


def on_close(ws, close_status_code, close_msg):
    global connected

    with lock:
        connected = False


def websocket_worker():
    global connected, last_error

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:
            with lock:
                connected = False
                last_error = f"Conexion interrumpida: {exc}"

        with lock:
            connected = False

        time.sleep(5)


def start_background_services():
    global ws_thread_started

    if ws_thread_started:
        return

    ws_thread_started = True

    threading.Thread(
        target=load_history,
        daemon=True,
    ).start()

    threading.Thread(
        target=websocket_worker,
        daemon=True,
    ).start()


def calculate_signal(price, history):
    closes = [
        float(c["close"])
        for c in history
        if c.get("close")
    ]

    if len(closes) < 22:
        return {
            "direction": "ESPERAR",
            "signal": "ESPERAR",
            "prediction": price,
            "confidence": 0,
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
        -0.005,
        min(0.005, momentum * 3 * 0.45)
    )

    prediction = price * (1 + projected_change)

    if ema9 > ema21 and momentum > 0:
        direction = "ALCISTA"
        signal = "SUBE"
        confidence = min(
            95,
            50 + int(abs(momentum) * 10000)
        )

    elif ema9 < ema21 and momentum < 0:
        direction = "BAJISTA"
        signal = "BAJA"
        confidence = min(
            95,
            50 + int(abs(momentum) * 10000)
        )

    else:
        direction = "NEUTRAL"
        signal = "ESPERAR"
        confidence = 0

    return {
        "direction": direction,
        "signal": signal,
        "prediction": round(prediction, 2),
        "confidence": confidence,
    }


@app.route("/")
def home():
    start_background_services()
    return render_template_string(HTML)


@app.route("/api")
def api():
    global connected

    with lock:
        price = current_price
        history = [dict(c) for c in candles]
        updated = last_update
        error = last_error

    now = int(time.time())

    is_connected = bool(
        price is not None
        and updated > 0
        and now - updated <= 30
    )

    with lock:
        connected = is_connected

    seconds_left = 900 - (now % 900)

    if price is None:
        return jsonify({
            "ok": False,
            "connected": False,
            "status": "Esperando datos de Coinbase",
            "error": error,
            "candles": history,
            "countdown": seconds_left,
        })

    analysis = calculate_signal(price, history)

    return jsonify({
        "ok": True,
        "connected": is_connected,
        "status": "EN VIVO" if is_connected else "SIN DATOS RECIENTES",
        "price": round(price, 2),
        "direction": analysis["direction"],
        "signal": analysis["signal"],
        "prediction": analysis["prediction"],
        "confidence": analysis["confidence"],
        "countdown": seconds_left,
        "last_update": updated,
        "candles": history[-100:],
        "error": error,
    })


@app.route("/health")
def health():
    with lock:
        updated = last_update
        price = current_price
        error = last_error

    live = bool(
        price is not None
        and updated > 0
        and time.time() - updated <= 30
    )

    return jsonify({
        "ok": True,
        "coinbase_live": live,
        "price": price,
        "last_update": updated,
        "candles": len(candles),
        "error": error,
    })

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
    padding: 18px;
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
    color: #ffb84d;
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
    padding: 12px;
    margin-top: 8px;
}

.up {
    color: #25d695;
}

.down {
    color: #ff6475;
}

.neutral {
    color: #ffbd59;
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
    height: 260px;
    display: block;
}

.small {
    color: #93a1bd;
    font-size: 12px;
    line-height: 1.5;
}

.row {
    display: flex;
    justify-content: space-between;
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
}
</style>
</head>

<body>
<main>

    <div class="header">
        <h1>BITCOIN RADAR</h1>
        <div id="status" class="status">CONECTANDO...</div>
    </div>

    <section class="card">
        <div class="label">BITCOIN / USD · COINBASE</div>
        <div id="price" class="price">Cargando...</div>
        <div class="small">Precio recibido de Coinbase</div>
    </section>

    <section class="card">
        <div class="label">RADAR DE DIRECCIÓN</div>

        <div id="direction" class="signal neutral">
            ESPERAR
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
        <div class="label">
            PRECIO ESTIMADO AL CIERRE DEL PERIODO
        </div>

        <div id="prediction" class="prediction">--</div>

        <div class="small">
            Estimación técnica, no garantiza el precio futuro.
        </div>
    </section>

    <section class="card">
        <div class="label">
            TIEMPO RESTANTE · PERIODO DE 15 MINUTOS UTC
        </div>

        <div id="countdown" class="countdown">15:00</div>
    </section>

    <section class="card">
        <div class="row">
            <div class="label">VELAS DE 1 MINUTO</div>

            <button onclick="refreshData()">
                ACTUALIZAR
            </button>
        </div>

        <canvas id="chart" width="900" height="360"></canvas>

        <div class="small">
            Las velas cambian cuando llegan nuevos datos de mercado.
        </div>
    </section>

    <div id="updated" class="small">
        Esperando primera actualización...
    </div>

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

    return String(m).padStart(2, "0") + ":" +
           String(s).padStart(2, "0");
}

function drawChart(data) {
    const canvas = document.getElementById("chart");
    const ctx = canvas.getContext("2d");

    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    if (!data || data.length < 2) {
        ctx.fillStyle = "#93a1bd";
        ctx.fillText("Esperando velas...", 25, 40);
        return;
    }

    const values = data.flatMap(c => [
        Number(c.high),
        Number(c.low)
    ]);

    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = Math.max(max - min, 1);

    const top = 20;
    const bottom = height - 25;
    const usableHeight = bottom - top;

    const y = price =>
        bottom - ((price - min) / range) * usableHeight;

    ctx.strokeStyle = "#26334e";
    ctx.lineWidth = 1;

    for (let i = 0; i < 5; i++) {
        const yy = top + (usableHeight / 4) * i;

        ctx.beginPath();
        ctx.moveTo(0, yy);
        ctx.lineTo(width, yy);
        ctx.stroke();
    }

    const step = width / data.length;
    const candleWidth = Math.max(3, step * 0.55);

    data.forEach((c, i) => {
        const x = i * step + step / 2;

        const rising = c.close >= c.open;
        const color = rising ? "#25d695" : "#ff6475";

        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        ctx.lineWidth = 2;

        ctx.beginPath();
        ctx.moveTo(x, y(c.high));
        ctx.lineTo(x, y(c.low));
        ctx.stroke();

        const openY = y(c.open);
        const closeY = y(c.close);

        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(
            2,
            Math.abs(closeY - openY)
        );

        ctx.fillRect(
            x - candleWidth / 2,
            bodyTop,
            candleWidth,
            bodyHeight
        );
    });
}

async function refreshData() {
    try {
        const response = await fetch(
            "/api",
            { cache: "no-store" }
        );

        const data = await response.json();

        if (data.price !== undefined && data.price !== null) {
            setText("price", money(data.price));
            setText("prediction", money(data.prediction));
            setText("direction", data.direction);
            setText("signal", data.signal);
            setText("confidence", data.confidence + "%");

            const direction =
                document.getElementById("direction");

            direction.className = "signal " +
                (
                    data.direction === "ALCISTA"
                    ? "up"
                    : data.direction === "BAJISTA"
                    ? "down"
                    : "neutral"
                );

            drawChart(data.candles);
        }

        setText(
            "countdown",
            formatTime(data.countdown || 0)
        );

        const status = document.getElementById("status");

        status.textContent = data.connected
            ? "● EN VIVO"
            : "● SIN CONEXIÓN";

        status.style.color = data.connected
            ? "#25d695"
            : "#ff6475";

        if (data.last_update) {
            setText(
                "updated",
                "Último dato recibido: " +
                new Date(
                    data.last_update * 1000
                ).toLocaleTimeString()
            );
        } else {
            setText(
                "updated",
                data.error || "Esperando datos de Coinbase..."
            );
        }

    } catch (error) {
        setText("status", "ERROR DE CONEXIÓN");
        setText("updated", String(error));
    }
}

refreshData();
setInterval(refreshData, 1000);
</script>

</body>
</html>
"""


if __name__ == "__main__":
    start_background_services()

    app.run(
        host="0.0.0.0",
        port=10000
    )
