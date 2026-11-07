import time
import requests
import pandas as pd
import numpy as np

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ==========================================================
# BITCOIN RADAR PRO
# MOTOR DE DATOS E INDICADORES
# ==========================================================

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 200

MIAMI = ZoneInfo("America/New_York")

COINBASE_URL = (
    "https://api.exchange.coinbase.com"
    "/products/BTC-USD/candles"
)

BINANCE_URL = (
    "https://api.binance.com/api/v3/klines"
)

KALSHI_URL = (
    "https://api.elections.kalshi.com/trade-api/v2"
)

session = requests.Session()
session.headers.update({
    "User-Agent": "BitcoinRadarPro/1.0"
})

# ==========================================================
# OBTENER DATOS DE BITCOIN
# ==========================================================

def get_btc_data():

    try:
        response = session.get(
            COINBASE_URL,
            params={"granularity": 60},
            timeout=8
        )

        response.raise_for_status()
        raw = response.json()

        df = pd.DataFrame(
            raw,
            columns=[
                "time", "low", "high",
                "open", "close", "volume"
            ]
        )

        df = df.sort_values("time")
        df = df.tail(LIMIT).reset_index(drop=True)

        if len(df) < 30:
            raise ValueError("Datos insuficientes")

        source = "COINBASE"

    except Exception:

        response = session.get(
            BINANCE_URL,
            params={
                "symbol": SYMBOL,
                "interval": INTERVAL,
                "limit": LIMIT
            },
            timeout=8
        )

        response.raise_for_status()

        raw = response.json()

        df = pd.DataFrame(
            raw,
            columns=[
                "time", "open", "high",
                "low", "close", "volume",
                "close_time", "quote_volume",
                "trades", "buy_volume",
                "buy_quote_volume", "ignore"
            ]
        )

        df = df[
            ["time", "open", "high",
             "low", "close", "volume"]
        ]

        df["time"] = (
            pd.to_numeric(df["time"]) // 1000
        )

        df = df.tail(LIMIT).reset_index(drop=True)

        source = "BINANCE"

    numeric = [
        "open", "high", "low", "close", "volume"
    ]

    for col in numeric:
        df[col] = pd.to_numeric(
            df[col], errors="coerce"
        )

    df["time"] = pd.to_numeric(
        df["time"], errors="coerce"
    ).astype(int)

    df = df.dropna().reset_index(drop=True)

    return df, source


# ==========================================================
# INDICADORES TÉCNICOS
# ==========================================================

def calculate_indicators(df):

    df = df.copy()

    close = df["close"]
    volume = df["volume"]

    # EMA 9 Y EMA 21

    df["ema9"] = close.ewm(
        span=9, adjust=False
    ).mean()

    df["ema21"] = close.ewm(
        span=21, adjust=False
    ).mean()

    # VWAP

    typical = (
        df["high"] +
        df["low"] +
        df["close"]
    ) / 3

    cumulative_volume = volume.cumsum()

    df["vwap"] = (
        (typical * volume).cumsum()
        / cumulative_volume.replace(0, np.nan)
    )

    # RSI 14

    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1/14,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1/14,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)

    df["rsi"] = 100 - (
        100 / (1 + rs)
    )

    # MACD

    ema12 = close.ewm(
        span=12, adjust=False
    ).mean()

    ema26 = close.ewm(
        span=26, adjust=False
    ).mean()

    df["macd"] = ema12 - ema26

    df["macd_signal"] = df["macd"].ewm(
        span=9, adjust=False
    ).mean()

    df["macd_hist"] = (
        df["macd"] - df["macd_signal"]
    )

    df = df.replace(
        [np.inf, -np.inf], np.nan
    )

    return df


# ==========================================================
# MOTOR DE DIRECCIÓN DEL MERCADO
# ==========================================================

def calculate_signal(df):

    last = df.iloc[-1]

    score = 0

    # EMA

    if last["ema9"] > last["ema21"]:
        score += 2

    elif last["ema9"] < last["ema21"]:
        score -= 2

    # VWAP

    if last["close"] > last["vwap"]:
        score += 1

    elif last["close"] < last["vwap"]:
        score -= 1

    # RSI

    if last["rsi"] > 55:
        score += 1

    elif last["rsi"] < 45:
        score -= 1

    # MACD

    if last["macd"] > last["macd_signal"]:
        score += 1

    elif last["macd"] < last["macd_signal"]:
        score -= 1

    # Clasificación

    if score >= 4:
        signal = "SUBE"
        trend = "ALCISTA"

    elif score <= -4:
        signal = "BAJA"
        trend = "BAJISTA"

    else:
        signal = "ESPERAR"
        trend = "LATERAL / INDECISO"

    # Porcentajes estimados, no probabilidades reales.

    up = 50 + (score * 8)
    up = max(5, min(95, up))

    down = 100 - up

    return {
        "signal": signal,
        "trend": trend,
        "score": score,
        "up_percent": round(up, 1),
        "down_percent": round(down, 1)
    }


# ==========================================================
# PREPARAR INFORMACIÓN PARA LA GRÁFICA
# ==========================================================

def prepare_chart_data(df):

    candles = []
    ema9 = []
    ema21 = []
    vwap = []
    volume = []
    rsi = []
    macd = []
    macd_signal = []
    macd_hist = []

    for _, row in df.iterrows():

        t = int(row["time"])

        candles.append({
            "time": t,
            "open": round(float(row["open"]), 2),
            "high": round(float(row["high"]), 2),
            "low": round(float(row["low"]), 2),
            "close": round(float(row["close"]), 2)
        })

        ema9.append({
            "time": t,
            "value": round(float(row["ema9"]), 2)
        })

        ema21.append({
            "time": t,
            "value": round(float(row["ema21"]), 2)
        })

        vwap.append({
            "time": t,
            "value": round(float(row["vwap"]), 2)
        })

        volume.append({
            "time": t,
            "value": round(float(row["volume"]), 4),
            "color": (
                "#00d69a"
                if row["close"] >= row["open"]
                else "#ff5268"
            )
        })

        if pd.notna(row["rsi"]):
            rsi.append({
                "time": t,
                "value": round(float(row["rsi"]), 2)
            })

        macd.append({
            "time": t,
            "value": round(float(row["macd"]), 4)
        })

        macd_signal.append({
            "time": t,
            "value": round(float(row["macd_signal"]), 4)
        })

        macd_hist.append({
            "time": t,
            "value": round(float(row["macd_hist"]), 4),
            "color": (
                "#00d69a"
                if row["macd_hist"] >= 0
                else "#ff5268"
            )
        })

    return {
        "candles": candles,
        "ema9": ema9,
        "ema21": ema21,
        "vwap": vwap,
        "volume": volume,
        "rsi_series": rsi,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist
    }


# ==========================================================
# RELOJ DE MIAMI
# ==========================================================

def get_miami_time():

    return datetime.now(
        MIAMI
    ).strftime("%Y-%m-%d %H:%M:%S")


# ==========================================================
# AQUÍ COMIENZA LA INTERFAZ
# LA PARTE 2 CONTINÚA DESDE AQUÍ
# ==========================================================

PAGE = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BITCOIN RADAR PRO</title>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #080d17;
    color: #e8eef8;
    font-family: Arial, sans-serif;
}

header {
    padding: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: #101827;
    border-bottom: 1px solid #263247;
}

.logo {
    font-size: 21px;
    font-weight: bold;
    letter-spacing: 2px;
}

.logo span {
    color: #00d6a0;
}

.status {
    color: #00d6a0;
    font-size: 12px;
}

.container {
    max-width: 1500px;
    margin: auto;
    padding: 16px;
}

.top-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
}

.card {
    background: #111b2b;
    border: 1px solid #25334a;
    border-radius: 13px;
    padding: 17px;
}

.label {
    color: #8999b0;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.price {
    font-size: 30px;
    font-weight: bold;
    margin-top: 12px;
}

.green {
    color: #00d69a;
}

.red {
    color: #ff5268;
}

.blue {
    color: #50a9ff;
}

.signal-card {
    margin-top: 14px;
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 12px;
}

.percent {
    font-size: 34px;
    font-weight: bold;
    margin-top: 12px;
}

.progress {
    height: 7px;
    background: #263247;
    border-radius: 10px;
    margin-top: 14px;
    overflow: hidden;
}

.progress div {
    height: 100%;
    border-radius: 10px;
    transition: width .5s;
}

.green-bar {
    background: #00d69a;
}

.red-bar {
    background: #ff5268;
}

.center-signal {
    text-align: center;
}

.trend {
    font-size: 23px;
    font-weight: bold;
    margin-top: 12px;
}

.subtext {
    color: #8999b0;
    font-size: 12px;
    margin-top: 8px;
}

.section {
    margin-top: 15px;
}

.section-title {
    font-size: 14px;
    font-weight: bold;
    margin-bottom: 12px;
}

.chart-card {
    background: #111b2b;
    border: 1px solid #25334a;
    border-radius: 13px;
    padding: 15px;
}

.chart {
    width: 100%;
    height: 390px;
}

.chart-small {
    width: 100%;
    height: 150px;
}

.indicator-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    margin-top: 14px;
}

.indicator-value {
    font-size: 20px;
    margin-top: 10px;
    font-weight: bold;
}

.alert {
    padding: 13px;
    background: #18263a;
    border-left: 4px solid #50a9ff;
    border-radius: 7px;
    margin-bottom: 9px;
    font-size: 13px;
}

.alert.buy {
    border-color: #00d69a;
}

.alert.sell {
    border-color: #ff5268;
}

.alert.wait {
    border-color: #f0b94b;
}

.alert-list {
    max-height: 250px;
    overflow-y: auto;
}

.clock {
    font-size: 20px;
    font-weight: bold;
    margin-top: 10px;
}

.footer {
    text-align: center;
    padding: 25px;
    color: #65748a;
    font-size: 11px;
}

@media(max-width: 700px) {
    .top-grid {
        grid-template-columns: 1fr 1fr;
    }

    .signal-card {
        grid-template-columns: 1fr 1fr;
    }

    .center-signal {
        grid-column: span 2;
        grid-row: 1;
    }

    .price {
        font-size: 23px;
    }

    .percent {
        font-size: 27px;
    }

    .chart {
        height: 320px;
    }

    header {
        padding: 15px;
    }

    .logo {
        font-size: 16px;
    }
}
</style>
</head>

<body>

<header>
    <div class="logo">₿ BITCOIN <span>RADAR PRO</span></div>
    <div class="status" id="connection">● CONECTANDO</div>
</header>

<div class="container">

    <div class="top-grid">

        <div class="card">
            <div class="label">BTC / USD</div>
            <div class="price" id="price">$0.00</div>
            <div class="subtext" id="source">Fuente: Coinbase</div>
        </div>

        <div class="card">
            <div class="label">Cambio reciente</div>
            <div class="price" id="change">0.00%</div>
            <div class="subtext">Variación de las últimas velas</div>
        </div>

        <div class="card">
            <div class="label">Hora Miami</div>
            <div class="clock" id="clock">--:--:--</div>
            <div class="subtext" id="date">Actualizando</div>
        </div>

    </div>

    <div class="signal-card">

        <div class="card">
            <div class="label green">Probabilidad estimada SUBE</div>
            <div class="percent green" id="up">50%</div>
            <div class="progress">
                <div class="green-bar" id="upbar" style="width:50%"></div>
            </div>
        </div>

        <div class="card center-signal">
            <div class="label">ESTADO DEL MERCADO</div>
            <div class="trend blue" id="trend">ANALIZANDO</div>
            <div class="subtext" id="signal">ESPERANDO DATOS</div>
            <div class="subtext" id="score">Fuerza: 0</div>
        </div>

        <div class="card">
            <div class="label red">Probabilidad estimada BAJA</div>
            <div class="percent red" id="down">50%</div>
            <div class="progress">
                <div class="red-bar" id="downbar" style="width:50%"></div>
            </div>
        </div>

    </div>

    <div class="section chart-card">
        <div class="section-title">
            GRÁFICA PRINCIPAL BTC/USD · 1 MINUTO
        </div>
        <div id="mainChart" class="chart"></div>
        <div class="subtext">
            Coinbase · Velas · EMA 9 · EMA 21 · VWAP
        </div>
    </div>

    <div class="indicator-grid">

        <div class="chart-card">
            <div class="section-title">RSI 14</div>
            <div id="rsiChart" class="chart-small"></div>
            <div class="indicator-value" id="rsiValue">--</div>
        </div>

        <div class="chart-card">
            <div class="section-title">MACD</div>
            <div id="macdChart" class="chart-small"></div>
            <div class="indicator-value" id="macdValue">--</div>
        </div>

    </div>

    <div class="indicator-grid">

        <div class="card">
            <div class="label">EMA 9</div>
            <div class="indicator-value" id="ema9">--</div>
        </div>

        <div class="card">
            <div class="label">EMA 21</div>
            <div class="indicator-value" id="ema21">--</div>
        </div>

        <div class="card">
            <div class="label">VWAP</div>
            <div class="indicator-value" id="vwap">--</div>
        </div>

        <div class="card">
            <div class="label">Volumen</div>
            <div class="indicator-value" id="volume">--</div>
        </div>

    </div>

    <div class="section card">
        <div class="section-title">ALERTAS INTELIGENTES</div>
        <div id="alerts" class="alert-list">
            <div class="alert">Iniciando sistema de alertas...</div>
        </div>
    </div>

    <div class="section card">
        <div class="section-title">RADAR DE OPERACIÓN</div>
        <div id="recommendation" class="alert wait">
            Analizando las condiciones del mercado...
        </div>
        <div class="subtext">
            Las señales son orientativas y no garantizan resultados.
            Confirma siempre el precio y el contrato antes de operar.
        </div>
    </div>

</div>

<div class="footer">
    BITCOIN RADAR PRO · DATOS DE MERCADO EN VIVO · HORARIO MIAMI
</div>

<script>

const chartOptions = {
    layout: {
        background: { color: "#111b2b" },
        textColor: "#9aabc1"
    },
    grid: {
        vertLines: { color: "#1d2a3d" },
        horzLines: { color: "#1d2a3d" }
    },
    rightPriceScale: {
        borderColor: "#263247"
    },
    timeScale: {
        borderColor: "#263247",
        timeVisible: true,
        secondsVisible: false
    },
    crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal
    }
};

const mainChart = LightweightCharts.createChart(
    document.getElementById("mainChart"),
    chartOptions
);

const candles = mainChart.addCandlestickSeries({
    upColor: "#00d69a",
    downColor: "#ff5268",
    borderUpColor: "#00d69a",
    borderDownColor: "#ff5268",
    wickUpColor: "#00d69a",
    wickDownColor: "#ff5268"
});

const e9 = mainChart.addLineSeries({
    color: "#42a5ff",
    lineWidth: 2,
    title: "EMA 9"
});

const e21 = mainChart.addLineSeries({
    color: "#ffb84d",
    lineWidth: 2,
    title: "EMA 21"
});

const vwapLine = mainChart.addLineSeries({
    color: "#c58aff",
    lineWidth: 2,
    title: "VWAP"
});

const rsiChart = LightweightCharts.createChart(
    document.getElementById("rsiChart"),
    chartOptions
);

const rsiLine = rsiChart.addLineSeries({
    color: "#c58aff",
    lineWidth: 2
});

rsiLine.createPriceLine({
    price: 70,
    color: "#ff5268",
    lineStyle: 2
});

rsiLine.createPriceLine({
    price: 30,
    color: "#00d69a",
    lineStyle: 2
});

const macdChart = LightweightCharts.createChart(
    document.getElementById("macdChart"),
    chartOptions
);

const macdLine = macdChart.addLineSeries({
    color: "#42a5ff",
    lineWidth: 2
});

const macdSignalLine = macdChart.addLineSeries({
    color: "#ffb84d",
    lineWidth: 2
});

const macdHistogram = macdChart.addHistogramSeries({
    priceFormat: { type: "price", precision: 4 }
});

function resizeCharts() {
    const width = document.getElementById("mainChart").clientWidth;

    mainChart.applyOptions({ width });
    rsiChart.applyOptions({
        width: document.getElementById("rsiChart").clientWidth
    });
    macdChart.applyOptions({
        width: document.getElementById("macdChart").clientWidth
    });
}

window.addEventListener("resize", resizeCharts);

function money(value) {
    return "$" + Number(value).toLocaleString(
        "en-US",
        {minimumFractionDigits: 2, maximumFractionDigits: 2}
    );
}

let previousSignal = null;
let lastAlertTime = 0;
let firstLoad = true;

function addAlert(message, type) {

    const box = document.getElementById("alerts");

    const item = document.createElement("div");

    item.className = "alert " + type;

    const now = new Date().toLocaleTimeString(
        "es-US",
        {timeZone: "America/New_York"}
    );

    item.textContent = now + " · " + message;

    box.prepend(item);

    while (box.children.length > 30) {
        box.removeChild(box.lastChild);
    }
}

function updateAlerts(data) {

    const signal = data.signal.signal;
    const now = Date.now();

    if (firstLoad) {
        previousSignal = signal;
        firstLoad = false;
        return;
    }

    if (signal !== previousSignal && now - lastAlertTime > 20000) {

        if (signal === "SUBE") {
            addAlert("SEÑAL ALCISTA: aumenta la presión compradora.", "buy");
        }

        else if (signal === "BAJA") {
            addAlert("SEÑAL BAJISTA: aumenta la presión vendedora.", "sell");
        }

        else {
            addAlert("INDECISIÓN: no hay confirmación suficiente. ESPERAR.", "wait");
        }

        previousSignal = signal;
        lastAlertTime = now;
    }

    if (signal === "ESPERAR") {
        document.getElementById("recommendation").textContent =
            "ESPERAR: mercado lateral o sin confirmación clara. Evita entradas impulsivas.";
        document.getElementById("recommendation").className = "alert wait";
    }

    else if (signal === "SUBE") {
        document.getElementById("recommendation").textContent =
            "TENDENCIA ALCISTA: vigilar confirmación y posibles retrocesos antes de entrar.";
        document.getElementById("recommendation").className = "alert buy";
    }

    else {
        document.getElementById("recommendation").textContent =
            "TENDENCIA BAJISTA: vigilar confirmación y posibles rebotes antes de entrar.";
        document.getElementById("recommendation").className = "alert sell";
    }
}

function updateInterface(data) {

    const signal = data.signal;

    document.getElementById("price").textContent =
        money(data.price);

    document.getElementById("up").textContent =
        signal.up_percent + "%";

    document.getElementById("down").textContent =
        signal.down_percent + "%";

    document.getElementById("upbar").style.width =
        signal.up_percent + "%";

    document.getElementById("downbar").style.width =
        signal.down_percent + "%";

    document.getElementById("trend").textContent =
        signal.trend;

    document.getElementById("signal").textContent =
        "SEÑAL: " + signal.signal;

    document.getElementById("score").textContent =
        "Fuerza del movimiento: " + signal.score;

    document.getElementById("source").textContent =
        "Fuente: " + data.source;

    document.getElementById("ema9").textContent =
        money(data.last.ema9);

    document.getElementById("ema21").textContent =
        money(data.last.ema21);

    document.getElementById("vwap").textContent =
        money(data.last.vwap);

    document.getElementById("rsiValue").textContent =
        Number(data.last.rsi).toFixed(2);

    document.getElementById("macdValue").textContent =
        Number(data.last.macd).toFixed(4);

    document.getElementById("volume").textContent =
        Number(data.last.volume).toFixed(4);

    document.getElementById("connection").textContent =
        "● EN VIVO";

    document.getElementById("connection").style.color =
        "#00d69a";

    const times = data.candles;

    const previous = times[times.length - 2];
    const current = times[times.length - 1];

    const change = previous
        ? ((current.close - previous.close) / previous.close) * 100
        : 0;

    const changeElement = document.getElementById("change");

    changeElement.textContent =
        (change >= 0 ? "+" : "") + change.toFixed(3) + "%";

    changeElement.className =
        "price " + (change >= 0 ? "green" : "red");

    const now = new Date();

    document.getElementById("clock").textContent =
        now.toLocaleTimeString("es-US", {
            timeZone: "America/New_York"
        });

    document.getElementById("date").textContent =
        now.toLocaleDateString("es-US", {
            timeZone: "America/New_York",
            weekday: "long",
            year: "numeric",
            month: "long",
            day: "numeric"
        });

    const trend = signal.trend;

    document.getElementById("trend").className =
        "trend " +
        (trend === "ALCISTA" ? "green" :
        trend === "BAJISTA" ? "red" : "blue");

    updateAlerts(data);
}

async function updateRadar() {

    try {

        const response = await fetch(
            "/api/data?t=" + Date.now(),
            {cache: "no-store"}
        );

        if (!response.ok) {
            throw new Error("Error del servidor");
        }

        const data = await response.json();

        if (data.error) {
            throw new Error(data.error);
        }

        candles.setData(data.chart.candles);
        e9.setData(data.chart.ema9);
        e21.setData(data.chart.ema21);
        vwapLine.setData(data.chart.vwap);

        rsiLine.setData(data.chart.rsi_series);

        macdLine.setData(data.chart.macd);
        macdSignalLine.setData(data.chart.macd_signal);
        macdHistogram.setData(data.chart.macd_hist);

        updateInterface(data);

        if (firstLoad) {
            mainChart.timeScale().fitContent();
            rsiChart.timeScale().fitContent();
            macdChart.timeScale().fitContent();
        }

        resizeCharts();

    } catch(error) {

        document.getElementById("connection").textContent =
            "● SIN CONEXIÓN";

        document.getElementById("connection").style.color =
            "#ff5268";

        console.error(error);
    }
}

updateRadar();

setInterval(updateRadar, 3000);

</script>
</body>
</html>
"""

# ==========================================================
# API PRINCIPAL DEL RADAR
# ==========================================================

@app.route("/")
def home():
    return render_template_string(PAGE)


@app.route("/api/data")
def api_data():

    try:

        df, source = get_btc_data()

        # Actualizar el último precio con el ticker de Coinbase.

        if source == "COINBASE":

            try:

                ticker = session.get(
                    "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
                    timeout=5
                )

                ticker.raise_for_status()

                spot = float(ticker.json()["price"])

                last_index = df.index[-1]

                df.loc[last_index, "close"] = spot

                df.loc[last_index, "high"] = max(
                    float(df.loc[last_index, "high"]),
                    spot
                )

                df.loc[last_index, "low"] = min(
                    float(df.loc[last_index, "low"]),
                    spot
                )

            except Exception:
                pass

        df = calculate_indicators(df)

        last = df.iloc[-1]

        signal = calculate_signal(df)

        chart = prepare_chart_data(df)

        return jsonify({

            "price": round(float(last["close"]), 2),

            "source": source,

            "signal": signal,

            "last": {

                "ema9": round(float(last["ema9"]), 2),

                "ema21": round(float(last["ema21"]), 2),

                "vwap": round(float(last["vwap"]), 2),

                "rsi": (
                    round(float(last["rsi"]), 2)
                    if pd.notna(last["rsi"])
                    else 50
                ),

                "macd": round(float(last["macd"]), 4),

                "volume": round(float(last["volume"]), 4)

            },

            "candles": chart["candles"],

            "chart": chart,

            "time": get_miami_time()

        })

    except Exception as error:

        return jsonify({
            "error": str(error)
        }), 503


# ==========================================================
# INICIAR APLICACIÓN
# ==========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )
