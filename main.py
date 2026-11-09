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
