import os
import requests
import pandas as pd

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# CONFIGURACIÓN
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 100

# INDICADORES
EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14

def get_market_data():
    url = "https://api.binance.us/api/v3/klines"

    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": CANDLE_LIMIT
    }

    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()

    data = response.json()

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close",
        "volume", "close_time", "quote_volume",
        "trades", "taker_buy", "taker_quote", "ignore"
    ])

    for column in ["open", "high", "low", "close"]:
        df[column] = df[column].astype(float)

    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)

    return df


def calculate_indicators(df):
    df["EMA9"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["EMA21"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD).mean()

    rs = avg_gain / avg_loss.replace(0, float("nan"))
    df["RSI"] = 100 - (100 / (1 + rs))

    return df
    
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
<meta name="viewport" content="width=device-width, initial-scale=1">

<title>Bitcoin Signal Radar</title>

<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>

<style>
body {
    background: #101827;
    color: white;
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 18px;
}

h1 {
    text-align: center;
}

.panel {
    background: #1d293b;
    padding: 18px;
    border-radius: 12px;
    margin-bottom: 15px;
}

.price {
    font-size: 30px;
    font-weight: bold;
}

.signal {
    font-size: 28px;
    font-weight: bold;
}

#chart {
    width: 100%;
    height: 430px;
}

#rsi {
    width: 100%;
    height: 220px;
}

.small {
    color: #aab7ca;
}
</style>
</head>

<body>

<h1>BITCOIN SIGNAL RADAR</h1>

<div class="panel">
    <div class="small">PRECIO BTC / USD</div>
    <div class="price" id="price">Cargando...</div>
</div>

<div class="panel">
    <div class="small">SEÑAL DEL MERCADO</div>
    <div class="signal" id="signal">ANALIZANDO...</div>
    <p id="explanation"></p>
</div>

<div class="panel">
    <h3>Gráfico Bitcoin - EMA 9 / EMA 21</h3>
    <div id="chart"></div>
</div>

<div class="panel">
    <h3>RSI - 14</h3>
    <div id="rsi"></div>
</div>

<div class="panel">
    <div id="updated">Esperando datos...</div>
    <div class="small">
        Datos públicos de Binance. Señales informativas,
        no garantías de dirección futura.
    </div>
</div>

<script>

async function updateDashboard() {

    try {

        const response = await fetch("/api/data");

        if (!response.ok) {
            throw new Error("Error obteniendo datos");
        }

        const data = await response.json();

        document.getElementById("price").innerText =
            "$" + data.price.toLocaleString("en-US", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });

        document.getElementById("signal").innerText =
            data.signal;

        document.getElementById("explanation").innerText =
            data.explanation;

        document.getElementById("updated").innerText =
            "Actualizado: " + new Date().toLocaleTimeString();

        const times = data.candles.map(x => x.time);

        Plotly.react("chart", [

            {
                x: times,
                open: data.candles.map(x => x.open),
                high: data.candles.map(x => x.high),
                low: data.candles.map(x => x.low),
                close: data.candles.map(x => x.close),
                type: "candlestick",
                name: "Bitcoin"
            },

            {
                x: times,
                y: data.candles.map(x => x.ema9),
                type: "scatter",
                mode: "lines",
                name: "EMA 9"
            },

            {
                x: times,
                y: data.candles.map(x => x.ema21),
                type: "scatter",
                mode: "lines",
                name: "EMA 21"
            }

        ], {
            paper_bgcolor: "#1d293b",
            plot_bgcolor: "#1d293b",
            font: {color: "white"},
            xaxis: {rangeslider: {visible: false}},
            margin: {t: 20, b: 40, l: 45, r: 15}
        }, {
            responsive: true
        });

        Plotly.react("rsi", [{
            x: times,
            y: data.candles.map(x => x.rsi),
            type: "scatter",
            mode: "lines",
            name: "RSI 14"
        }], {
            paper_bgcolor: "#1d293b",
            plot_bgcolor: "#1d293b",
            font: {color: "white"},
            yaxis: {range: [0, 100]},
            shapes: [
                {
                    type: "line",
                    x0: 0, x1: 1,
                    xref: "paper",
                    y0: 70, y1: 70
                },
                {
                    type: "line",
                    x0: 0, x1: 1,
                    xref: "paper",
                    y0: 30, y1: 30
                }
            ],
            margin: {t: 15, b: 35, l: 40, r: 15}
        }, {
            responsive: true
        });

    } catch (error) {

        document.getElementById("updated").innerText =
            "Error: " + error.message;

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
        df = calculate_indicators(df)

        signal = generate_signal(df)

        if signal == "SUBE":
            explanation = "EMA 9 por encima de EMA 21 y RSI superior a 50."

        elif signal == "BAJA":
            explanation = "EMA 9 por debajo de EMA 21 y RSI inferior a 50."

        else:
            explanation = "No hay confirmación suficiente. Esperar."

        candles = []

        for _, row in df.iterrows():

            candles.append({
                "time": row["time"].strftime("%Y-%m-%d %H:%M:%S"),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "ema9": row["EMA9"],
                "ema21": row["EMA21"],
                "rsi": None if pd.isna(row["RSI"]) else row["RSI"]
            })

        return jsonify({
            "price": float(df.iloc[-1]["close"]),
            "signal": signal,
            "explanation": explanation,
            "candles": candles
        })

    except Exception as error:

        return jsonify({
            "error": str(error)
        }), 500


if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
