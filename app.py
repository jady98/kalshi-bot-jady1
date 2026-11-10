import os
import time
import threading
import logging
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BTC-RADAR")

PRODUCT = "BTC-USD"
BASE_URL = "https://api.exchange.coinbase.com"
lock = threading.RLock()

candles = []
current_price = 0.0
last_update = 0.0
last_error = ""
connected = False
worker_started = False

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Radar-PRO/3.0",
    "Accept": "application/json"
})


def get_json(url, params=None, timeout=10):
    response = session.get(
        url,
        params=params,
        timeout=timeout
    )
    response.raise_for_status()
    return response.json()


def load_history():
    global candles, current_price, last_update
    global connected, last_error

    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=300)

    params = {
        "granularity": 60,
        "start": start.isoformat(),
        "end": end.isoformat()
    }

    try:
        rows = get_json(
            f"{BASE_URL}/products/{PRODUCT}/candles",
            params=params,
            timeout=15
        )

        parsed = []

        for row in rows:
            if len(row) < 6:
                continue

            parsed.append({
                "time": int(row[0]),
                "low": float(row[1]),
                "high": float(row[2]),
                "open": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })

        parsed.sort(key=lambda item: item["time"])

        with lock:
            candles = parsed[-300:]

            if candles:
                current_price = candles[-1]["close"]
                last_update = time.time()

            connected = bool(candles)
            last_error = ""

        log.info("Historial cargado: %s velas", len(parsed))
        return True

    except Exception as exc:
        last_error = str(exc)
        log.exception("Error cargando historial")
        return False


def update_candle(price, volume=0.0):
    global current_price, last_update

    if price <= 0:
        return

    now = int(time.time() // 60) * 60

    with lock:
        current_price = price
        last_update = time.time()

        if not candles:
            candles.append({
                "time": now,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            })
            return

        last = candles[-1]

        if now == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
            last["volume"] += volume

        elif now > last["time"]:
            previous = last["close"]
            next_time = last["time"] + 60

            while next_time < now:
                candles.append({
                    "time": next_time,
                    "open": previous,
                    "high": previous,
                    "low": previous,
                    "close": previous,
                    "volume": 0.0
                })
                next_time += 60

            candles.append({
                "time": now,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            })

        if len(candles) > 300:
            del candles[:-300]


def get_live_price():
    global connected, last_error

    ticker_url = f"{BASE_URL}/products/{PRODUCT}/ticker"
    backup_url = f"https://api.coinbase.com/v2/prices/{PRODUCT}/spot"

    try:
        data = get_json(ticker_url)
        price = float(data["price"])

        if price <= 0:
            raise ValueError("Precio inválido")

        update_candle(price)

        with lock:
            connected = True
            last_error = ""

        return True

    except Exception as first_error:
        log.warning("Ticker principal falló: %s", first_error)

    try:
        data = get_json(backup_url)
        price = float(data["data"]["amount"])

        if price <= 0:
            raise ValueError("Precio de respaldo inválido")

        update_candle(price)

        with lock:
            connected = True
            last_error = ""

        return True

    except Exception as second_error:
        with lock:
            connected = (
                last_update > 0
                and time.time() - last_update < 20
            )
            last_error = str(second_error)

        log.error("Coinbase sin respuesta: %s", second_error)
        return False


def background_worker():
    while True:
        try:
            get_live_price()
        except Exception:
            log.exception("Error en actualización")

        time.sleep(3)


def start_worker():
    global worker_started

    with lock:
        if worker_started:
            return

        worker_started = True

    load_history()

    thread = threading.Thread(
        target=background_worker,
        daemon=True
    )
    thread.start()


def ema(values, period):
    if not values:
        return []

    alpha = 2 / (period + 1)
    result = [values[0]]

    for value in values[1:]:
        result.append(
            alpha * value + (1 - alpha) * result[-1]
        )

    return result


def calculate_rsi(values, period=14):
    if len(values) <= period:
        return 50.0

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0) for x in changes]
    losses = [max(-x, 0) for x in changes]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(changes)):
        avg_gain = (
            avg_gain * (period - 1) + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(values):
    if len(values) < 35:
        return 0.0, 0.0, 0.0

    fast = ema(values, 12)
    slow = ema(values, 26)

    line = [
        a - b for a, b in zip(fast, slow)
    ]

    signal = ema(line, 9)

    return line[-1], signal[-1], line[-1] - signal[-1]


def calculate_vwap(data):
    total_volume = 0.0
    total_value = 0.0
    result = []

    for candle in data:
        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3

        volume = candle["volume"]

        total_volume += volume
        total_value += typical * volume

        result.append(
            total_value / total_volume
            if total_volume > 0
            else candle["close"]
        )

    return result


def analyze_market(data):
    closes = [c["close"] for c in data]

    if len(closes) < 35:
        return {
            "direction": "REUNIENDO DATOS",
            "score": 0,
            "rsi": 50,
            "ema9": None,
            "ema21": None,
            "macd": 0,
            "signal": 0,
            "histogram": 0,
            "vwap": None,
            "bull": 50,
            "bear": 50
        }

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    rv = calculate_rsi(closes)
    macd, signal, histogram = calculate_macd(closes)
    vwap = calculate_vwap(data)

    score = 0

    score += 2 if e9[-1] > e21[-1] else -2
    score += 1.5 if closes[-1] > vwap[-1] else -1.5
    score += 1.5 if histogram > 0 else -1.5

    if rv > 55:
        score += 1
    elif rv < 45:
        score -= 1

    if closes[-6] > 0:
        momentum = (
            closes[-1] - closes[-6]
        ) / closes[-6] * 100

        score += 1 if momentum > 0 else -1

    score = max(-7, min(7, score))

    bull = 50 + (score / 7) * 45
    bear = 100 - bull

    direction = (
        "SUBE" if score >= 1.5
        else "BAJA" if score <= -1.5
        else "NEUTRAL"
    )

    return {
        "direction": direction,
        "score": round(score, 2),
        "rsi": round(rv, 2),
        "ema9": round(e9[-1], 2),
        "ema21": round(e21[-1], 2),
        "macd": round(macd, 4),
        "signal": round(signal, 4),
        "histogram": round(histogram, 4),
        "vwap": round(vwap[-1], 2),
        "bull": round(bull, 1),
        "bear": round(bear, 1)
    }


def forecast_15m(data):
    analysis = analyze_market(data)

    if len(data) < 35:
        return {
            "direction": "REUNIENDO DATOS",
            "target": None,
            "change": 0,
            "confidence": 50
        }

    closes = [c["close"] for c in data]

    returns = []

    for i in range(max(1, len(closes) - 15), len(closes)):
        previous = closes[i - 1]

        if previous > 0:
            returns.append(
                (closes[i] - previous) / previous
            )

    drift = (
        sum(returns) / len(returns)
        if returns else 0
    )

    change = (
        drift * 15 * 100
        + analysis["score"] / 7 * 0.08
    )

    change = max(-0.8, min(0.8, change))

    price = closes[-1]
    target = price * (1 + change / 100)

    direction = (
        "SUBE" if change > 0.025
        else "BAJA" if change < -0.025
        else "LATERAL"
    )

    confidence = min(
        85,
        50 + abs(analysis["score"]) * 4
    )

    return {
        "direction": direction,
        "target": round(target, 2),
        "change": round(change, 4),
        "confidence": round(confidence, 1)
    }
    
@app.route("/")
def home():
    return render_template_string(PAGE)


@app.route("/api/data")
def api_data():
    with lock:
        data = [c.copy() for c in candles]
        price = current_price
        updated = last_update
        is_connected = connected
        error = last_error

    analysis = analyze_market(data)
    forecast = forecast_15m(data)

    age = round(time.time() - updated, 1) if updated else None

    return jsonify({
        "connected": is_connected and age is not None and age < 20,
        "price": round(price, 2) if price else None,
        "updated": age,
        "error": error,
        "candles": data,
        "analysis": analysis,
        "forecast": forecast
    })


PAGE = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC RADAR PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>
*{box-sizing:border-box}
body{
    margin:0;
    background:#080d18;
    color:#edf3ff;
    font-family:Arial,sans-serif;
    padding:15px;
}
.container{max-width:1100px;margin:auto}
h1{font-size:24px;margin:8px 0;color:#fff}
.subtitle{color:#91a2bd;font-size:12px;margin-bottom:15px}
.card{
    background:#111a2b;
    border:1px solid #26344d;
    border-radius:13px;
    padding:15px;
    margin-bottom:13px;
}
.row{
    display:flex;
    justify-content:space-between;
    align-items:center;
    gap:12px;
    flex-wrap:wrap;
}
.price{font-size:30px;font-weight:bold}
.muted{color:#91a2bd;font-size:12px}
.status{font-size:13px;font-weight:bold}
.green{color:#29d391}
.red{color:#ff647c}
.yellow{color:#ffca62}
.grid{
    display:grid;
    grid-template-columns:repeat(2,minmax(0,1fr));
    gap:10px;
}
.metric{
    background:#0b1423;
    border-radius:10px;
    padding:12px;
}
.metric .value{
    font-size:19px;
    font-weight:bold;
    margin-top:7px;
}
.direction{
    font-size:27px;
    font-weight:bold;
    margin:8px 0;
}
#chart{
    width:100%;
    height:360px;
}
.bar{
    height:10px;
    border-radius:10px;
    background:#28354b;
    overflow:hidden;
    display:flex;
    margin-top:10px;
}
.bullbar{background:#20c98b}
.bearbar{background:#ff5872}
button{
    border:0;
    border-radius:8px;
    padding:9px 14px;
    background:#263954;
    color:white;
}
.footer{
    color:#73839d;
    font-size:11px;
    text-align:center;
    padding:15px;
}
@media(max-width:500px){
    body{padding:10px}
    #chart{height:300px}
    .price{font-size:25px}
}
</style>
</head>

<body>
<div class="container">

<h1>₿ BTC RADAR PRO</h1>
<div class="subtitle">Análisis en tiempo real · Coinbase REST · Pronóstico 15 minutos</div>

<div class="card">
    <div class="row">
        <div>
            <div class="muted">BITCOIN / USD</div>
            <div class="price" id="price">Conectando...</div>
        </div>
        <div>
            <div class="status yellow" id="status">● CONECTANDO</div>
            <div class="muted" id="updated">Esperando datos</div>
        </div>
    </div>
</div>

<div class="card">
    <div class="row">
        <div>
            <div class="muted">PREDICCIÓN PRÓXIMOS 15 MIN</div>
            <div class="direction" id="forecastDirection">CALCULANDO</div>
            <div class="muted" id="forecastTarget">Precio objetivo: --</div>
        </div>
        <div class="metric">
            <div class="muted">CONFIANZA</div>
            <div class="value" id="confidence">--</div>
            <div class="muted" id="forecastChange">Cambio estimado: --</div>
        </div>
    </div>
</div>

<div class="card">
    <div class="row">
        <strong>GRÁFICA BTC/USD</strong>
        <span class="muted">Velas de 1 minuto</span>
    </div>
    <div id="chart"></div>
</div>

<div class="card">
    <strong>RADAR DEL MERCADO</strong>
    <div class="direction" id="direction">REUNIENDO DATOS</div>

    <div class="bar">
        <div class="bullbar" id="bullbar" style="width:50%"></div>
        <div class="bearbar" id="bearbar" style="width:50%"></div>
    </div>

    <div class="row" style="margin-top:9px">
        <span class="green" id="bull">SUBE 50%</span>
        <span class="red" id="bear">BAJA 50%</span>
    </div>
</div>

<div class="grid">
    <div class="metric">
        <div class="muted">RSI (14)</div>
        <div class="value" id="rsi">--</div>
    </div>
    <div class="metric">
        <div class="muted">EMA 9</div>
        <div class="value" id="ema9">--</div>
    </div>
    <div class="metric">
        <div class="muted">EMA 21</div>
        <div class="value" id="ema21">--</div>
    </div>
    <div class="metric">
        <div class="muted">VWAP</div>
        <div class="value" id="vwap">--</div>
    </div>
    <div class="metric">
        <div class="muted">MACD</div>
        <div class="value" id="macd">--</div>
    </div>
    <div class="metric">
        <div class="muted">SEÑAL MACD</div>
        <div class="value" id="signal">--</div>
    </div>
    <div class="metric">
        <div class="muted">HISTOGRAMA</div>
        <div class="value" id="histogram">--</div>
    </div>
    <div class="metric">
        <div class="muted">PUNTUACIÓN</div>
        <div class="value" id="score">--</div>
    </div>
</div>

<div class="footer">
    Datos de Coinbase. La predicción es una estimación matemática,
    no una garantía de movimiento ni una señal segura de operación.
</div>

</div>

<script>
const chartElement = document.getElementById("chart");

const chart = LightweightCharts.createChart(chartElement,{
    width:chartElement.clientWidth,
    height:360,
    layout:{
        background:{color:"#111a2b"},
        textColor:"#aebbd0"
    },
    grid:{
        vertLines:{color:"#1d2a40"},
        horzLines:{color:"#1d2a40"}
    },
    rightPriceScale:{
        borderColor:"#26344d"
    },
    timeScale:{
        borderColor:"#26344d",
        timeVisible:true,
        secondsVisible:false
    },
    crosshair:{
        mode:1
    }
});

const candleSeries = chart.addCandlestickSeries({
    upColor:"#20c98b",
    downColor:"#ff5872",
    borderUpColor:"#20c98b",
    borderDownColor:"#ff5872",
    wickUpColor:"#20c98b",
    wickDownColor:"#ff5872"
});

const ema9Series = chart.addLineSeries({
    color:"#35a7ff",
    lineWidth:2,
    priceLineVisible:false
});

const ema21Series = chart.addLineSeries({
    color:"#ffbd4a",
    lineWidth:2,
    priceLineVisible:false
});

const vwapSeries = chart.addLineSeries({
    color:"#c084fc",
    lineWidth:2,
    priceLineVisible:false
});

let chartReady = false;
let lastCandleTime = 0;

function money(value){
    if(value === null || value === undefined || !Number.isFinite(Number(value))){
        return "--";
    }
    return "$" + Number(value).toLocaleString("en-US",{
        minimumFractionDigits:2,
        maximumFractionDigits:2
    });
}

function setText(id,value){
    document.getElementById(id).textContent = value;
}

function colorDirection(element,value){
    element.className = "direction";

    if(value === "SUBE"){
        element.classList.add("green");
    }else if(value === "BAJA"){
        element.classList.add("red");
    }else{
        element.classList.add("yellow");
    }
}

async function refresh(){
    try{
        const response = await fetch("/api/data",{
            cache:"no-store"
        });

        if(!response.ok){
            throw new Error("Error HTTP " + response.status);
        }

        const data = await response.json();

        const status = document.getElementById("status");

        if(data.connected){
            status.textContent = "● CONECTADO";
            status.className = "status green";
        }else{
            status.textContent = "● SIN CONEXIÓN";
            status.className = "status red";
        }

        setText("updated",
            data.updated === null
            ? "Sin actualización"
            : "Actualizado hace " + data.updated + " s"
        );

        if(data.price){
            setText("price",money(data.price));
        }

        const f = data.forecast || {};

        setText("forecastDirection",f.direction || "--");
        colorDirection(
            document.getElementById("forecastDirection"),
            f.direction
        );

        setText(
            "forecastTarget",
            "Precio objetivo: " + money(f.target)
        );

        setText(
            "confidence",
            f.confidence === undefined ? "--" : f.confidence + "%"
        );

        setText(
            "forecastChange",
            "Cambio estimado: " +
            (f.change === undefined ? "--" : f.change + "%")
        );

        const a = data.analysis || {};

        setText("direction",a.direction || "--");
        colorDirection(
            document.getElementById("direction"),
            a.direction
        );

        const bull = Number(a.bull || 50);
        const bear = Number(a.bear || 50);

        document.getElementById("bullbar").style.width = bull + "%";
        document.getElementById("bearbar").style.width = bear + "%";

        setText("bull","SUBE " + bull + "%");
        setText("bear","BAJA " + bear + "%");

        setText("rsi",a.rsi ?? "--");
        setText("ema9",money(a.ema9));
        setText("ema21",money(a.ema21));
        setText("vwap",money(a.vwap));
        setText("macd",a.macd ?? "--");
        setText("signal",a.signal ?? "--");
        setText("histogram",a.histogram ?? "--");
        setText("score",a.score ?? "--");

        if(Array.isArray(data.candles) && data.candles.length){
            const valid = data.candles.filter(c =>
                Number.isFinite(c.time) &&
                Number.isFinite(c.open) &&
                Number.isFinite(c.high) &&
                Number.isFinite(c.low) &&
                Number.isFinite(c.close)
            );

            if(valid.length){
                candleSeries.setData(valid.map(c=>({
                    time:c.time,
                    open:c.open,
                    high:c.high,
                    low:c.low,
                    close:c.close
                })));

                const closes = valid.map(c=>c.close);

                function calculateEMA(values,period){
                    const alpha = 2/(period+1);
                    let result = [values[0]];

                    for(let i=1;i<values.length;i++){
                        result.push(
                            alpha*values[i]+
                            (1-alpha)*result[i-1]
                        );
                    }

                    return result;
                }

                function lineData(values){
                    return values.map((value,index)=>({
                        time:valid[index].time,
                        value:value
                    }));
                }

                ema9Series.setData(
                    lineData(calculateEMA(closes,9))
                );

                ema21Series.setData(
                    lineData(calculateEMA(closes,21))
                );

                let volumeSum = 0;
                let valueSum = 0;

                const vwap = valid.map(c=>{
                    const typical = (c.high+c.low+c.close)/3;
                    const volume = Number(c.volume || 0);

                    volumeSum += volume;
                    valueSum += typical*volume;

                    return volumeSum > 0
                        ? valueSum/volumeSum
                        : c.close;
                });

                vwapSeries.setData(lineData(vwap));

                if(!chartReady){
                    chart.timeScale().fitContent();
                    chartReady = true;
                }
            }
        }

    }catch(error){
        const status = document.getElementById("status");
        status.textContent = "● ERROR DE CONEXIÓN";
        status.className = "status red";
        console.error(error);
    }
}

window.addEventListener("resize",()=>{
    chart.applyOptions({
        width:chartElement.clientWidth
    });
});

refresh();
setInterval(refresh,3000);
</script>
</body>
</html>
"""


# Iniciar el actualizador automático
start_worker()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        debug=False
    )
    
