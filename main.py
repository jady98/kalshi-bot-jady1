
import os
import requests
import pandas as pd
import numpy as np

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# =====================================
# CONFIGURACION
# =====================================

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 200

EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/1.0"})

# =====================================
# DATOS DE MERCADO
# =====================================

def get_market_data():

    errors = []

    # BINANCE.US
    try:
        response = session.get(
            "https://api.binance.us/api/v3/klines",
            params={
                "symbol": SYMBOL,
                "interval": INTERVAL,
                "limit": CANDLE_LIMIT
            },
            timeout=12
        )

        response.raise_for_status()

        data = response.json()

        df = pd.DataFrame(data, columns=[
            "time", "open", "high", "low", "close",
            "volume", "close_time", "quote_volume",
            "trades", "taker_buy", "taker_quote", "ignore"
        ])

        df["time"] = pd.to_datetime(
            df["time"], unit="ms", utc=True
        )

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        return df, "Binance.US"

    except Exception as e:
        errors.append("Binance.US: " + str(e))

    # COINBASE
    try:
        response = session.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            params={"granularity": 60},
            timeout=12
        )

        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list) or not data:
            raise ValueError("Sin velas disponibles")

        df = pd.DataFrame(data, columns=[
            "time", "low", "high", "open", "close", "volume"
        ])

        df["time"] = pd.to_datetime(
            df["time"], unit="s", utc=True
        )

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        df = df.sort_values("time").tail(CANDLE_LIMIT)

        return df.reset_index(drop=True), "Coinbase"

    except Exception as e:
        errors.append("Coinbase: " + str(e))

    raise Exception(" | ".join(errors))


# =====================================
# INDICADORES
# =====================================

def calculate_indicators(df):

    df = df.copy()

    close = df["close"]

    # EMA
    df["EMA9"] = close.ewm(
        span=EMA_FAST, adjust=False
    ).mean()

    df["EMA21"] = close.ewm(
        span=EMA_SLOW, adjust=False
    ).mean()

    # VWAP
    typical = (df["high"] + df["low"] + close) / 3

    volume_sum = df["volume"].cumsum()

    df["VWAP"] = (
        (typical * df["volume"]).cumsum()
        / volume_sum.replace(0, np.nan)
    )

    # RSI WILDER
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / RSI_PERIOD,
        adjust=False,
        min_periods=RSI_PERIOD
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / RSI_PERIOD,
        adjust=False,
        min_periods=RSI_PERIOD
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)

    df["RSI"] = 100 - (100 / (1 + rs))

    df.loc[(avg_loss == 0) & (avg_gain > 0), "RSI"] = 100
    df.loc[(avg_gain == 0) & (avg_loss > 0), "RSI"] = 0
    df.loc[(avg_gain == 0) & (avg_loss == 0), "RSI"] = 50

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()

    df["MACD"] = ema12 - ema26

    df["MACD_SIGNAL"] = df["MACD"].ewm(
        span=9, adjust=False
    ).mean()

    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    # SOPORTE Y RESISTENCIA
    df["SUPPORT"] = df["low"].rolling(50, min_periods=1).min()
    df["RESISTANCE"] = df["high"].rolling(50, min_periods=1).max()

    return df


# =====================================
# ANALISIS DE MERCADO
# =====================================

def generate_signal(df):

    last = df.iloc[-1]

    if pd.isna(last["RSI"]):
        return "ESPERAR"

    if (
        last["EMA9"] > last["EMA21"]
        and last["RSI"] > 50
        and last["MACD"] > last["MACD_SIGNAL"]
        and last["close"] > last["VWAP"]
    ):
        return "SUBE"

    if (
        last["EMA9"] < last["EMA21"]
        and last["RSI"] < 50
        and last["MACD"] < last["MACD_SIGNAL"]
        and last["close"] < last["VWAP"]
    ):
        return "BAJA"

    return "ESPERAR"


# =====================================
# INTERFAZ PRO
# =====================================

HTML = """
<!DOCTYPE html>
<html lang="es">

<head>

<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">

<title>Bitcoin Signal Radar PRO</title>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 12px;
    background: #080d18;
    color: #e5edf8;
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
    font-size: 19px;
    letter-spacing: 1.5px;
    margin: 5px 0;
}

.status {
    font-size: 11px;
    color: #22d69b;
}

.panel {
    background: #111a2a;
    border: 1px solid #24344b;
    border-radius: 12px;
    padding: 13px;
    margin-bottom: 12px;
}

.metrics {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 9px;
}

.metric {
    background: #182438;
    padding: 13px;
    border-radius: 9px;
    min-width: 0;
}

.metric small {
    color: #91a4bd;
    font-size: 10px;
}

.metric strong {
    display: block;
    font-size: 19px;
    margin-top: 7px;
    overflow-wrap: anywhere;
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

.blue {
    color: #36a3ff;
}

.section-title {
    font-size: 12px;
    color: #9eb0c8;
    letter-spacing: 1px;
    margin: 3px 0 12px;
}

.chart {
    width: 100%;
    height: 360px;
}

.chart.small-chart {
    height: 160px;
}

.chart.macd-chart {
    height: 170px;
}

.legend {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    font-size: 10px;
    color: #aabbd0;
    margin-bottom: 10px;
}

.legend span {
    display: flex;
    align-items: center;
    gap: 5px;
}

.dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
}

button {
    border: 1px solid #344963;
    background: #1b2b42;
    color: white;
    border-radius: 7px;
    padding: 10px 13px;
    font-weight: bold;
}

footer {
    font-size: 10px;
    color: #8293a9;
    text-align: center;
    padding: 12px;
}

@media (min-width: 700px) {
    body {
        max-width: 1150px;
        margin: auto;
        padding: 22px;
    }

    .metrics {
        grid-template-columns: repeat(4, 1fr);
    }

    .chart {
        height: 500px;
    }
}

</style>

</head>

<body>

<header>
    <div>
        <h1>BITCOIN RADAR PRO</h1>
        <div class="status" id="connection">CONECTANDO...</div>
    </div>

    <button onclick="updateDashboard()">ACTUALIZAR</button>
</header>

<div class="metrics">

    <div class="metric">
        <small>BTC / USD</small>
        <strong id="price">$--</strong>
    </div>

    <div class="metric">
        <small>SEÑAL</small>
        <strong id="signal" class="yellow">--</strong>
    </div>

    <div class="metric">
        <small>RSI 14</small>
        <strong id="rsiValue">--</strong>
    </div>

    <div class="metric">
        <small>FUENTE</small>
        <strong id="source" style="font-size:14px">--</strong>
    </div>

</div>

<div class="panel">

    <div class="section-title">
        PRECIO · VELAS JAPONESAS · TENDENCIA
    </div>

    <div class="legend">
        <span><i class="dot" style="background:#ffc857"></i>EMA 9</span>
        <span><i class="dot" style="background:#36a3ff"></i>EMA 21</span>
        <span><i class="dot" style="background:#c58cff"></i>VWAP</span>
        <span><i class="dot" style="background:#00d69a"></i>SUBIDA</span>
        <span><i class="dot" style="background:#ff5268"></i>BAJADA</span>
    </div>

    <div id="priceChart" class="chart"></div>

</div>

<div class="panel">

    <div class="section-title">
        VOLUMEN DE MERCADO
    </div>

    <div id="volumeChart" class="chart small-chart"></div>

</div>

<div class="panel">

    <div class="section-title">
        RSI · FUERZA RELATIVA
    </div>

    <div id="rsiChart" class="chart small-chart"></div>

</div>

<div class="panel">

    <div class="section-title">
        MACD · MOMENTUM
    </div>

    <div id="macdChart" class="chart macd-chart"></div>

</div>

<div class="metrics">

    <div class="metric">
        <small>SOPORTE 50 VELAS</small>
        <strong id="support" class="green">--</strong>
    </div>

    <div class="metric">
        <small>RESISTENCIA 50 VELAS</small>
        <strong id="resistance" class="red">--</strong>
    </div>

</div>

<div class="panel">

    <div class="section-title">LECTURA DEL MERCADO</div>

    <p id="explanation" style="font-size:13px;color:#b5c5da">
        Esperando datos...
    </p>

    <div id="updated" style="font-size:11px;color:#8293a9">
        --
    </div>

</div>

<footer>
    Indicadores técnicos informativos. No son garantías de movimientos futuros.
    Datos de mercado BTC/USD. No representan necesariamente el BRTI de Kalshi.
    <br><br>
    Charts powered by TradingView Lightweight Charts
</footer>

<script>

const $ = id => document.getElementById(id);

const formatPrice = value =>
    "$" + Number(value).toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });

const chartOptions = (element, height) => ({
    width: element.clientWidth,
    height: height,
    layout: {
        background: {color: "#111a2a"},
        textColor: "#aebdd0",
        fontSize: 10
    },
    grid: {
        vertLines: {color: "#1d2b40"},
        horzLines: {color: "#1d2b40"}
    },
    rightPriceScale: {
        borderColor: "#34445a"
    },
    timeScale: {
        borderColor: "#34445a",
        timeVisible: true,
        secondsVisible: false
    },
    crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal
    },
    handleScroll: true,
    handleScale: true
});

// CREAR GRAFICOS

const priceChart = LightweightCharts.createChart(
    $("priceChart"),
    chartOptions($("priceChart"), 360)
);

const candles = priceChart.addCandlestickSeries({
    upColor: "#00d69a",
    downColor: "#ff5268",
    borderUpColor: "#00d69a",
    borderDownColor: "#ff5268",
    wickUpColor: "#00d69a",
    wickDownColor: "#ff5268"
});

const ema9 = priceChart.addLineSeries({
    color: "#ffc857",
    lineWidth: 2,
    title: "EMA 9",
    lastValueVisible: true
});

const ema21 = priceChart.addLineSeries({
    color: "#36a3ff",
    lineWidth: 2,
    title: "EMA 21",
    lastValueVisible: true
});

const vwap = priceChart.addLineSeries({
    color: "#c58cff",
    lineWidth: 2,
    title: "VWAP",
    lastValueVisible: true
});

// VOLUMEN

const volumeChart = LightweightCharts.createChart(
    $("volumeChart"),
    chartOptions($("volumeChart"), 160)
);

const volumeSeries = volumeChart.addHistogramSeries({
    priceFormat: {type: "volume"},
    priceScaleId: "",
    lastValueVisible: false
});

volumeChart.priceScale("").applyOptions({
    scaleMargins: {
        top: 0.1,
        bottom: 0
    }
});

// RSI

const rsiChart = LightweightCharts.createChart(
    $("rsiChart"),
    chartOptions($("rsiChart"), 160)
);

const rsiSeries = rsiChart.addLineSeries({
    color: "#bd8aff",
    lineWidth: 2,
    title: "RSI 14"
});

rsiSeries.createPriceLine({
    price: 70,
    color: "#ff5268",
    lineStyle: LightweightCharts.LineStyle.Dashed,
    title: "70"
});

rsiSeries.createPriceLine({
    price: 50,
    color: "#71839c",
    lineStyle: LightweightCharts.LineStyle.Dotted,
    title: "50"
});

rsiSeries.createPriceLine({
    price: 30,
    color: "#00d69a",
    lineStyle: LightweightCharts.LineStyle.Dashed,
    title: "30"
});

rsiChart.priceScale("right").applyOptions({
    autoScale: false,
    scaleMargins: {top: 0.1, bottom: 0.1}
});

// MACD

const macdChart = LightweightCharts.createChart(
    $("macdChart"),
    chartOptions($("macdChart"), 170)
);

const macdLine = macdChart.addLineSeries({
    color: "#36a3ff",
    lineWidth: 2,
    title: "MACD"
});

const macdSignal = macdChart.addLineSeries({
    color: "#ffc857",
    lineWidth: 2,
    title: "Signal"
});

const macdHistogram = macdChart.addHistogramSeries({
    priceFormat: {type: "price", precision: 2},
    lastValueVisible: false
});

macdLine.createPriceLine({
    price: 0,
    color: "#71839c",
    lineStyle: LightweightCharts.LineStyle.Dashed
});

// SINCRONIZAR EJES DE TIEMPO

let syncing = false;

function syncCharts(source, targets) {
    source.timeScale().subscribeVisibleLogicalRangeChange(range => {
        if (!range || syncing) return;

        syncing = true;

        targets.forEach(chart => {
            chart.timeScale().setVisibleLogicalRange(range);
        });

        syncing = false;
    });
}

syncCharts(priceChart, [volumeChart, rsiChart, macdChart]);
syncCharts(volumeChart, [priceChart, rsiChart, macdChart]);
syncCharts(rsiChart, [priceChart, volumeChart, macdChart]);
syncCharts(macdChart, [priceChart, volumeChart, rsiChart]);

// ACTUALIZACION

let initialized = false;
let busy = false;

async function updateDashboard() {

    if (busy) return;
    busy = true;

    try {

        $("connection").innerText = "ACTUALIZANDO DATOS...";

        const response = await fetch("/api/data", {
            cache: "no-store"
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "Error de datos");
        }

        $("price").innerText = formatPrice(data.price);
        $("source").innerText = data.source;

        $("signal").innerText = data.signal;

        $("signal").className =
            data.signal === "SUBE" ? "green" :
            data.signal === "BAJA" ? "red" : "yellow";

        $("rsiValue").innerText =
            data.rsi === null ? "--" : data.rsi.toFixed(2);

        $("support").innerText = formatPrice(data.support);
        $("resistance").innerText = formatPrice(data.resistance);

        $("explanation").innerText = data.explanation;

        const previousRange = initialized
            ? priceChart.timeScale().getVisibleLogicalRange()
            : null;

        candles.setData(data.candles.map(x => ({
            time: x.time,
            open: x.open,
            high: x.high,
            low: x.low,
            close: x.close
        })));

        ema9.setData(data.candles.map(x => ({
            time: x.time,
            value: x.ema9
        })));

        ema21.setData(data.candles.map(x => ({
            time: x.time,
            value: x.ema21
        })));

        vwap.setData(data.candles.map(x => ({
            time: x.time,
            value: x.vwap
        })));

        volumeSeries.setData(data.candles.map(x => ({
            time: x.time,
            value: x.volume,
            color: x.close >= x.open
                ? "rgba(0,214,154,0.55)"
                : "rgba(255,82,104,0.55)"
        })));

        rsiSeries.setData(data.candles
            .filter(x => x.rsi !== null)
            .map(x => ({
                time: x.time,
                value: x.rsi
            }))
        );

        macdLine.setData(data.candles.map(x => ({
            time: x.time,
            value: x.macd
        })));

        macdSignal.setData(data.candles.map(x => ({
            time: x.time,
            value: x.macd_signal
        })));

        macdHistogram.setData(data.candles.map(x => ({
            time: x.time,
            value: x.macd_hist,
            color: x.macd_hist >= 0
                ? "rgba(0,214,154,0.65)"
                : "rgba(255,82,104,0.65)"
        })));

        if (!initialized) {
            priceChart.timeScale().fitContent();
            initialized = true;
        } else if (previousRange) {
            priceChart.timeScale().setVisibleLogicalRange(previousRange);
        }

        $("connection").innerText =
            "● CONECTADO · " + data.source;

        $("updated").innerText =
            "Ultima actualizacion: " +
            new Date().toLocaleTimeString();

    } catch (error) {

        $("connection").innerText = "● ERROR DE CONEXION";

        $("explanation").innerText = error.message;

    } finally {
        busy = false;
    }
}

// AJUSTE RESPONSIVE

function resizeCharts() {

    const pairs = [
        [priceChart, "priceChart", 360],
        [volumeChart, "volumeChart", 160],
        [rsiChart, "rsiChart", 160],
        [macdChart, "macdChart", 170]
    ];

    pairs.forEach(([chart, id, height]) => {
        const el = $(id);
        chart.applyOptions({
            width: el.clientWidth,
            height: height
        });
    });
}

window.addEventListener("resize", resizeCharts);

updateDashboard();

setInterval(updateDashboard, 15000);

</script>

</body>
</html>
"""


# =====================================
# PAGINA PRINCIPAL
# =====================================

@app.route("/")
def home():
    return render_template_string(HTML)


# =====================================
# API DEL MERCADO
# =====================================

@app.route("/api/data")
def market_data():

    try:

        df, source = get_market_data()

        if len(df) < 30:
            raise ValueError("Datos insuficientes")

        df = calculate_indicators(df)

        last = df.iloc[-1]

        signal = generate_signal(df)

        if signal == "SUBE":
            explanation = (
                "EMA 9 > EMA 21, RSI > 50, MACD positivo "
                "y precio sobre VWAP. Confirmacion tecnica alcista."
            )

        elif signal == "BAJA":
            explanation = (
                "EMA 9 < EMA 21, RSI < 50, MACD negativo "
                "y precio bajo VWAP. Confirmacion tecnica bajista."
            )

        else:
            explanation = (
                "Los indicadores no coinciden. "
                "Esperar una confirmacion antes de operar."
            )

        candles_data = []

        for _, row in df.iterrows():

            candles_data.append({

                "time": int(row["time"].timestamp()),

                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),

                "ema9": float(row["EMA9"]),
                "ema21": float(row["EMA21"]),
                "vwap": (
                    float(row["VWAP"])
                    if pd.notna(row["VWAP"]) else None
                ),

                "rsi": (
                    float(row["RSI"])
                    if pd.notna(row["RSI"]) else None
                ),

                "macd": float(row["MACD"]),
                "macd_signal": float(row["MACD_SIGNAL"]),
                "macd_hist": float(row["MACD_HIST"])

            })

        return jsonify({

            "price": float(last["close"]),

            "signal": signal,

            "rsi": (
                float(last["RSI"])
                if pd.notna(last["RSI"]) else None
            ),

            "support": float(last["SUPPORT"]),
            "resistance": float(last["RESISTANCE"]),

            "source": source,

            "explanation": explanation,

            "candles": candles_data

        })

    except Exception as error:

        print("MARKET ERROR:", error)

        return jsonify({
            "error": str(error)
        }), 500


# ===================
