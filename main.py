
import os
import requests
import pandas as pd

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# CONFIGURACION
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 100

EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14

# SESIONES HTTP
session = requests.Session()
session.headers.update({
    "User-Agent": "BitcoinSignalRadar/1.0"
})


def get_market_data():

    # FUENTE 1: BINANCE.US
    try:
        url = "https://api.binance.us/api/v3/klines"

        response = session.get(
            url,
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

        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float)

        df["time"] = pd.to_datetime(
            df["time"], unit="ms", utc=True
        )

        return df.sort_values("time").reset_index(drop=True)

    except Exception as binance_error:

        print("Binance.US no disponible:", binance_error)

    # FUENTE 2: COINBASE
    try:
        url = "https://api.exchange.coinbase.com/products/BTC-USD/candles"

        response = session.get(
            url,
            params={
                "granularity": 60
            },
            timeout=12
        )

        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list) or not data:
            raise ValueError("Coinbase no devolvio velas")

        df = pd.DataFrame(
            data,
            columns=[
                "time", "low", "high",
                "open", "close", "volume"
            ]
        )

        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float)

        df["time"] = pd.to_datetime(
            df["time"], unit="s", utc=True
        )

        df = df.sort_values("time").tail(CANDLE_LIMIT)

        return df.reset_index(drop=True)

    except Exception as coinbase_error:

        raise Exception(
            "No se pudieron obtener datos. "
            f"Binance: {binance_error}. "
            f"Coinbase: {coinbase_error}"
        )


def calculate_indicators(df):

    df = df.copy()

    # EMA 9 Y EMA 21
    df["EMA9"] = df["close"].ewm(
        span=EMA_FAST,
        adjust=False
    ).mean()

    df["EMA21"] = df["close"].ewm(
        span=EMA_SLOW,
        adjust=False
    ).mean()

    # RSI 14
    delta = df["close"].diff()

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

    rs = avg_gain / avg_loss.replace(0, float("nan"))

    df["RSI"] = 100 - (100 / (1 + rs))

    # CORREGIR CASOS DE RSI EXTREMO
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

    return df


def find_support_resistance(df):

    recent = df.tail(50)

    support = float(recent["low"].min())
    resistance = float(recent["high"].max())

    return support, resistance


def generate_signal(df):

    last = df.iloc[-1]

    ema9 = last["EMA9"]
    ema21 = last["EMA21"]
    rsi = last["RSI"]

    if pd.isna(rsi):
        return "ESPERAR"

    if ema9 > ema21 and rsi > 50:
        return "SUBE"

    if ema9 < ema21 and rsi < 50:
        return "BAJA"

    return "ESPERAR"


HTML = """
<!DOCTYPE html>
<html lang="es">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1">

<title>Bitcoin Signal Radar</title>

<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>

<style>

* {
    box-sizing: border-box;
}

body {
    background: #080e19;
    color: #ffffff;
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 12px;
}

h1 {
    text-align: center;
    font-size: 23px;
    letter-spacing: 2px;
    color: #e5edf9;
}

h3 {
    font-size: 15px;
    color: #cbd5e1;
}

.panel {
    background: #121d2d;
    border: 1px solid #26364c;
    padding: 15px;
    border-radius: 13px;
    margin-bottom: 13px;
}

.small {
    color: #91a4bd;
    font-size: 12px;
}

.price {
    font-size: 32px;
    font-weight: bold;
    margin-top: 7px;
}

.signal {
    font-size: 29px;
    font-weight: bold;
    margin-top: 8px;
}

#explanation {
    color: #b6c4d7;
    font-size: 13px;
}

#chart {
    width: 100%;
    height: 470px;
}

#rsi {
    width: 100%;
    height: 230px;
}

.levels {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    flex-wrap: wrap;
    font-size: 12px;
    color: #b6c4d7;
}

.level {
    background: #1d2a3d;
    padding: 9px;
    border-radius: 8px;
    flex: 1;
    text-align: center;
}

#updated {
    color: #91a4bd;
    font-size: 12px;
}

button {
    background: #24354c;
    color: white;
    border: 1px solid #425571;
    padding: 9px 14px;
    border-radius: 7px;
    margin-top: 8px;
}

</style>

</head>

<body>

<h1>BITCOIN SIGNAL RADAR</h1>

<div class="panel">

    <div class="small">PRECIO BTC / USD</div>

    <div class="price" id="price">
        Cargando...
    </div>

</div>

<div class="panel">

    <div class="small">DIRECCION DEL MERCADO</div>

    <div class="signal" id="signal">
        ANALIZANDO...
    </div>

    <p id="explanation">
        Esperando indicadores...
    </p>

</div>

<div class="panel">

    <h3>SOPORTE Y RESISTENCIA</h3>

    <div class="levels">

        <div class="level">
            SOPORTE
            <br>
            <b id="support">--</b>
        </div>

        <div class="level">
            RESISTENCIA
            <br>
            <b id="resistance">--</b>
        </div>

    </div>

</div>

<div class="panel">

    <h3>VELAS JAPONESAS | EMA 9 / EMA 21</h3>

    <div id="chart"></div>

</div>

<div class="panel">

    <h3>RSI RELATIVO | PERIODO 14</h3>

    <div id="rsi"></div>

</div>

<div class="panel">

    <div id="updated">
        Esperando datos...
    </div>

    <p class="small">
        Datos públicos de mercado.
        Indicadores informativos, no garantizan
        movimientos futuros.
    </p>

    <button onclick="updateDashboard()">
        ACTUALIZAR AHORA
    </button>

</div>

<script>

const money = value =>
    "$" + Number(value).toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });

async function updateDashboard() {

    const status = document.getElementById("updated");

    try {

        const response = await fetch(
            "/api/data",
            {cache: "no-store"}
        );

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "Error del servidor");
        }

        document.getElementById("price").innerText =
            money(data.price);

        const signalElement =
            document.getElementById("signal");

        signalElement.innerText = data.signal;

        signalElement.style.color =
            data.signal === "SUBE" ? "#22d68a" :
            data.signal === "BAJA" ? "#ff5268" :
            "#f5c451";

        document.getElementById("explanation").innerText =
            data.explanation;

        document.getElementById("support").innerText =
            money(data.support);

        document.getElementById("resistance").innerText =
            money(data.resistance);

        const candles = data.candles;

        const times = candles.map(x => x.time);

        const commonLayout = {
            paper_bgcolor: "#121d2d",
            plot_bgcolor: "#121d2d",
            font: {
                color: "#dce6f4",
                size: 11
            },
            margin: {
                t: 20,
                b: 45,
                l: 65,
                r: 15
            },
            hovermode: "x unified",
            dragmode: "pan",
            uirevision: "bitcoin-chart",
            xaxis: {
                type: "date",
                gridcolor: "#253449",
                showgrid: true,
                rangeslider: {
                    visible: false
                },
                showspikes: true,
                spikemode: "across",
                spikesnap: "cursor"
            },
            yaxis: {
                side: "right",
                gridcolor: "#253449",
                showgrid: true,
                tickformat: ",.2f",
                fixedrange: false
            }
        };

        // VELAS JAPONESAS

        const candleTrace = {

            x: times,

            open: candles.map(x => x.open),
            high: candles.map(x => x.high),
            low: candles.map(x => x.low),
            close: candles.map(x => x.close),

            type: "candlestick",

            name: "BTC",

            increasing: {
                line: {color: "#00e59b"},
                fillcolor: "#00c987"
            },

            decreasing: {
                line: {color: "#ff496c"},
                fillcolor: "#ff496c"
            },

            whiskerwidth: 0.6
        };

        // EMA 9

        const ema9Trace = {

            x: times,

            y: candles.map(x => x.ema9),

            type: "scatter",

            mode: "lines",

            name: "EMA 9",

            line: {
                color: "#ffc857",
                width: 2
            }
        };

        // EMA 21

        const ema21Trace = {

            x: times,

            y: candles.map(x => x.ema21),

            type: "scatter",

            mode: "lines",

            name: "EMA 21",

            line: {
                color: "#36a3ff",
                width: 2
            }
        };

        const layout = {

            ...commonLayout,

            shapes: [

                {
                    type: "line",
                    xref: "paper",
                    x0: 0,
                    x1: 1,
                    y0: data.support,
                    y1: data.support,
                    line: {
                        color: "#00d69a",
                        width: 1.5,
                        dash: "dot"
                    }
                },

                {
                    type: "line",
                    xref: "paper",
                    x0: 0,
                    x1: 1,
                    y0: data.resistance,
                    y1: data.resistance,
                    line: {
                        color: "#ff5268",
                        width: 1.5,
                        dash: "dot"
                    }
                }

            ],

            legend: {
                orientation: "h",
                y: 1.08
            },

            showlegend: true
        };

        Plotly.react(
            "chart",
            [
                candleTrace,
                ema9Trace,
                ema21Trace
            ],
            layout,
            {
                responsive: true,
                scrollZoom: true,
                displaylogo: false
            }
        );

        // RSI

        const rsiTrace = {

            x: times,

            y: candles.map(x => x.rsi),

            type: "scatter",

            mode: "lines",

            name: "RSI 14",

            line: {
                color: "#bd8aff",
                width: 2
            }
        };

        Plotly.react(
            "rsi",
            [rsiTrace],
            {
                paper_bgcolor: "#121d2d",
                plot_bgcolor: "#121d2d",

                font: {
                    color: "#dce6f4"
                },

                margin: {
                    t: 15,
                    b: 35,
                    l: 45,
                    r: 15
                },

                xaxis: {
                    type: "date",
                    gridcolor: "#253449"
                },

                yaxis: {
                    range: [0, 100],
                    side: "right",
                    gridcolor: "#253449"
                },

                shapes: [30, 50, 70].map(level => ({
                    type: "line",
                    xref: "paper",
                    x0: 0,
                    x1: 1,
                    y0: level,
                    y1: level,
                    line: {
                        color: level === 50 ? "#64748b" : "#ffbd59",
                        dash: "dot",
                        width: 1
                    }
                }))
            },
            {
                responsive: true,
                displaylogo: false
            }
        );

        status.innerText =
            "Actualizado: " +
            new Date().toLocaleTimeString() +
            " | Fuente: " + data.source;

    } catch (error) {

        status.innerText =
            "ERROR: " + error.message;

    }

}

updateDashboard();

setInterval(updateDashboard, 15000);

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

        df = get_market_data()

        if len(df) < 25:
            raise ValueError("No hay suficientes velas")

        df = calculate_indicators(df)

        signal = generate_signal(df)

        support, resistance = find_support_resistance(df)

        if signal == "SUBE":

            explanation = (
                "EMA 9 esta por encima de EMA 21 "
                "y RSI superior a 50."
            )

        elif signal == "BAJA":

            explanation = (
                "EMA 9 esta por debajo de EMA 21 "
                "y RSI inferior a 50."
            )

        else:

            explanation = (
                "No hay confirmacion suficiente. Esperar."
            )

        candles = []

        for _, row in df.iterrows():

            candles.append({

                "time": row["time"].isoformat(),

                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),

                "ema9": float(row["EMA9"]),
                "ema21": float(row["EMA21"]),

                "rsi": (
                    None
                    if pd.isna(row["RSI"])
                    else float(row["RSI"])
                )

            })

        return jsonify({

            "price": float(df.iloc[-1]["close"]),

            "signal": signal,

            "explanation": explanation,

            "support": support,

            "resistance": resistance,

            "source": (
                "Binance.US"
                if SYMBOL == "BTCUSDT"
                else "Coinbase"
            ) if len(candles) else "Desconocida",

            "candles": candles

        })

    except Exception as error:

        print("ERROR MARKET DATA:", error)

        return jsonify({
            "error": str(error)
        }), 500


if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
    
