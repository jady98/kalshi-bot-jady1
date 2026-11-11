import os
import time
import threading
import logging
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BTC_RADAR")

COINBASE = "https://api.exchange.coinbase.com"
KRAKEN = "https://api.kraken.com/0"
PRODUCT = "BTC-USD"
POLL_SECONDS = 1
MAX_CANDLES = 300

lock = threading.RLock()
candles = []
current_price = None
last_update = 0
connected = False
provider = "Desconectado"
last_error = ""
worker_started = False

session = requests.Session()
retry = Retry(
    total=2,
    connect=2,
    read=2,
    status=2,
    backoff_factor=0.25,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
    respect_retry_after_header=False
)
adapter = HTTPAdapter(
    max_retries=retry,
    pool_connections=4,
    pool_maxsize=4
)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({
    "User-Agent": "BTC-Radar-PRO/4.0",
    "Accept": "application/json"
})


def get_json(url, params=None, timeout=8):
    response = session.get(url, params=params, timeout=(3, timeout))
    response.raise_for_status()
    return response.json()


def load_coinbase():
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=MAX_CANDLES)

    rows = get_json(
        f"{COINBASE}/products/{PRODUCT}/candles",
        {
            "granularity": 60,
            "start": start.isoformat(timespec="seconds"),
            "end": end.isoformat(timespec="seconds")
        },
        12
    )

    result = []
    for r in rows:
        result.append({
            "time": int(r[0]),
            "low": float(r[1]),
            "high": float(r[2]),
            "open": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5])
        })

    return sorted(result, key=lambda x: x["time"])[-MAX_CANDLES:]


def load_kraken():
    data = get_json(
        f"{KRAKEN}/OHLC",
        {"pair": "XBTUSD", "interval": 1},
        10
    )

    if data.get("error"):
        raise RuntimeError(str(data["error"]))

    result = data.get("result", {})
    key = next((k for k in result if k != "last"), None)

    if not key:
        raise RuntimeError("Kraken no devolvió velas")

    rows = []
    for r in result[key]:
        rows.append({
            "time": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[6])
        })

    return rows[-MAX_CANDLES:]


def load_history():
    global candles, current_price, connected
    global provider, last_update, last_error

    for name, function in [
        ("Coinbase", load_coinbase),
        ("Kraken", load_kraken)
    ]:
        try:
            data = function()

            if not data:
                raise RuntimeError("Historial vacío")

            with lock:
                candles = data
                current_price = data[-1]["close"]
                provider = name
                connected = True
                last_update = time.time()
                last_error = ""

            log.info("%s: %s velas", name, len(data))
            return True

        except Exception as e:
            log.warning("%s historial: %s", name, e)
            last_error = str(e)

    return False


def update_candle(price):
    global current_price, last_update

    now = int(time.time() // 60) * 60

    with lock:
        current_price = float(price)
        last_update = time.time()

        if not candles:
            candles.append({
                "time": now,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0
            })
            return

        last = candles[-1]

        if now == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price

        elif now > last["time"]:
            previous = last["close"]
            t = last["time"] + 60

            while t < now:
                candles.append({
                    "time": t,
                    "open": previous,
                    "high": previous,
                    "low": previous,
                    "close": previous,
                    "volume": 0
                })
                t += 60

            candles.append({
                "time": now,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0
            })

        if len(candles) > MAX_CANDLES:
            del candles[:-MAX_CANDLES]


def coinbase_price():
    d = get_json(f"{COINBASE}/products/{PRODUCT}/ticker")
    return float(d["price"])


def coinbase_spot():
    d = get_json(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    )
    return float(d["data"]["amount"])


def kraken_price():
    d = get_json(
        f"{KRAKEN}/Ticker",
        {"pair": "XBTUSD"}
    )

    if d.get("error"):
        raise RuntimeError(str(d["error"]))

    result = d["result"]
    key = next(iter(result))
    return float(result[key]["c"][0])


def update_price():
    global connected, provider, last_error

    sources = [
        ("Coinbase", coinbase_price),
        ("Coinbase Spot", coinbase_spot),
        ("Kraken", kraken_price)
    ]

    errors = []

    for name, function in sources:
        try:
            price = function()

            if price <= 0:
                raise ValueError("Precio inválido")

            update_candle(price)

            with lock:
                connected = True
                provider = name
                last_error = ""

            return True

        except Exception as e:
            errors.append(name + ": " + str(e))
            log.warning("%s: %s", name, e)

    with lock:
        connected = False
        last_error = " | ".join(errors)

    return False


def background_worker():
    """Actualiza el precio en segundo plano con un ciclo estable de 1 segundo."""
    load_history()
    next_tick = time.monotonic()

    while True:
        try:
            if not candles:
                load_history()
            update_price()
        except Exception:
            log.exception("Error actualizando BTC")

        next_tick += POLL_SECONDS
        delay = next_tick - time.monotonic()

        if delay > 0:
            time.sleep(delay)
        else:
            next_tick = time.monotonic()


def start_worker():
    global worker_started

    with lock:
        if worker_started:
            return
        worker_started = True

    def runner():
        global worker_started
        try:
            background_worker()
        except Exception:
            log.exception("El actualizador terminó inesperadamente")
        finally:
            with lock:
                worker_started = False

    threading.Thread(
        target=runner,
        daemon=True,
        name="BTC-RADAR"
    ).start()


def ema(values, period):
    if not values:
        return []

    alpha = 2 / (period + 1)
    result = [float(values[0])]

    for value in values[1:]:
        result.append(
            alpha * float(value) +
            (1 - alpha) * result[-1]
        )

    return result


def calculate_rsi(values, period=14):
    if len(values) <= period:
        return 50.0

    changes = [
        values[i] - values[i-1]
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
        return 0, 0, 0

    fast = ema(values, 12)
    slow = ema(values, 26)

    line = [
        a - b for a, b in zip(fast, slow)
    ]

    signal = ema(line, 9)

    return line[-1], signal[-1], line[-1] - signal[-1]


def calculate_vwap(data):
    total_volume = 0
    total_value = 0
    result = []

    for c in data:
        typical = (
            c["high"] + c["low"] + c["close"]
        ) / 3

        volume = max(0, c["volume"])

        total_volume += volume
        total_value += typical * volume

        result.append(
            total_value / total_volume
            if total_volume else c["close"]
        )

    return result


def analyze(data):
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
    macd, signal, hist = calculate_macd(closes)
    vw = calculate_vwap(data)

    score = 0

    score += 2 if e9[-1] > e21[-1] else -2
    score += 1.5 if closes[-1] > vw[-1] else -1.5
    score += 1.5 if hist > 0 else -1.5

    if rv > 55:
        score += 1
    elif rv < 45:
        score -= 1

    momentum = (
        (closes[-1] - closes[-6]) / closes[-6] * 100
    )

    score += 1 if momentum > 0 else -1
    score = max(-7, min(7, score))

    bull = 50 + score / 7 * 45

    direction = (
        "SUBE" if score >= 1.5
        else "BAJA" if score <= -1.5
        else "NEUTRAL"
    )

    return {
        "direction": direction,
        "score": round(score, 2),
        "rsi": round(rv, 1),
        "ema9": round(e9[-1], 2),
        "ema21": round(e21[-1], 2),
        "macd": round(macd, 2),
        "signal": round(signal, 2),
        "histogram": round(hist, 2),
        "vwap": round(vw[-1], 2),
        "bull": round(bull, 1),
        "bear": round(100 - bull, 1)
    }


def forecast_15m(data):
    a = analyze(data)
    closes = [c["close"] for c in data]

    if len(closes) < 35:
        return {
            "direction": "REUNIENDO DATOS",
            "target": None,
            "change": 0,
            "confidence": 50
        }

    returns = []

    for i in range(max(1, len(closes)-15), len(closes)):
        if closes[i-1] > 0:
            returns.append(
                (closes[i] - closes[i-1]) / closes[i-1]
            )

    drift = sum(returns) / len(returns) if returns else 0

    change = (
        drift * 15 * 100 +
        (a["score"] / 7) * 0.08
    )

    change = max(-0.8, min(0.8, change))
    target = closes[-1] * (1 + change / 100)

    direction = (
        "SUBE" if change > 0.025
        else "BAJA" if change < -0.025
        else "LATERAL"
    )

    return {
        "direction": direction,
        "target": round(target, 2),
        "change": round(change, 3),
        "confidence": round(
            min(85, 50 + abs(a["score"]) * 4), 1
        )
    }
    @app.route("/")
def home():
    return render_template_string(PAGE)


@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "BTC RADAR PRO"
    })


@app.route("/api/coinbase-test")
def coinbase_test():
    try:
        price = coinbase_price()
        response = jsonify({
            "ok": True,
            "provider": "Coinbase Exchange",
            "product": PRODUCT,
            "price": price,
            "checked_at": int(time.time())
        })
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return response
    except Exception as exc:
        log.exception("Error conectando con Coinbase")
        response = jsonify({
            "ok": False,
            "provider": "Coinbase Exchange",
            "error": str(exc)
        })
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return response, 502


@app.route("/api/data")
def api_data():
    start_worker()
    with lock:
        data = [c.copy() for c in candles]
        price = current_price
        updated = last_update
        is_connected = connected
        source = provider
        error = last_error

    age = round(time.time() - updated, 1) if updated else None
    fresh = age is not None and age < 20

    response = jsonify({
        "connected": bool(is_connected and fresh),
        "provider": source if fresh and is_connected else "Desconectado",
        "price": round(price, 2) if price else None,
        "updated": age,
        "error": error or ("Esperando respuesta de Coinbase" if not fresh else ""),
        "candles": data,
        "analysis": analyze(data),
        "forecast": forecast_15m(data),
        "server_time": int(time.time())
    })
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


PAGE = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#050711">
<title>BTC Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>
*{box-sizing:border-box}
body{margin:0;padding:12px;background:#050711;color:#f4f5ff;font-family:Arial,sans-serif}
.wrap{max-width:1050px;margin:auto}
header{display:flex;align-items:center;gap:12px;margin:5px 2px 15px}
.coin{width:58px;height:58px;border-radius:50%;background:#ff9d13;color:white;display:grid;place-items:center;font-size:36px;font-weight:900}
.brand{flex:1}
.brand h1{font-size:clamp(21px,5vw,32px);font-style:italic;margin:0;font-weight:900}
.brand h1 span{color:#c52bff}
.sub{text-align:center;color:#bdc5e4;font-size:12px;margin-top:4px}
.live{color:#00e58b;font-size:13px}
.gear{background:none;border:0;color:white;font-size:25px}
.card{background:linear-gradient(145deg,#0c1224,#080d1a);border:1px solid #293654;border-radius:18px;padding:13px;margin-bottom:12px}
.topgrid{display:grid;grid-template-columns:1fr 1.15fr 1fr;gap:9px}
.signal{text-align:center;padding:13px 8px;border-radius:18px;background:#080d19}
.up{border:1px solid #00dc84}
.down{border:1px solid #ff3155}
.mid{border:1px solid #39496c}
.signal .name{font-size:clamp(13px,3.5vw,19px);font-weight:800}
.signal .pct{font-size:clamp(25px,6vw,39px);font-weight:900;margin:6px 0}
.up .name,.up .pct{color:#00e58b}
.down .name,.down .pct{color:#ff3155}
.target{color:#00e58b;font-size:clamp(17px,4.5vw,27px);font-weight:900;margin:8px 0}
.change{color:#00e58b;font-size:14px;font-weight:800}
.meter{height:10px;background:#26314c;border-radius:20px;overflow:hidden}
.meter span{height:100%;display:block}
.controls{display:flex;align-items:center;gap:7px;flex-wrap:wrap}
.controls button{border:0;background:transparent;color:white;font-size:16px;padding:8px 10px;border-radius:9px}
.controls button.active{background:linear-gradient(130deg,#9d00ff,#5900c9)}
.legend{display:flex;gap:10px;align-items:center;flex-wrap:wrap;color:#d1d8f0;font-size:11px;margin-left:auto}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:4px}
.chartcard{padding:10px 5px}
.charttitle{padding:4px 10px 8px;display:flex;justify-content:space-between;font-size:13px;font-weight:bold}
#chart{height:350px;width:100%}
.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:9px}
.metric{background:#080d1c;border:1px solid #293654;border-radius:13px;padding:12px;min-width:0}
.metric small{color:#cbd2ea;font-size:12px}
.metric strong{display:block;font-size:clamp(18px,4vw,26px);margin-top:6px;overflow-wrap:anywhere}
.cyan{color:#00d8f5}.green{color:#00e58b}.purple{color:#c54aff}
.bottomgrid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.bigmetric{display:flex;gap:10px;align-items:center}
.ico{font-size:28px}
.label{color:#d4d9ef;font-size:14px}
.big{font-size:clamp(23px,5.5vw,36px);font-weight:900;white-space:nowrap}
.progress{height:10px;background:#28324c;border-radius:20px;margin-top:10px;overflow:hidden}
.progress span{display:block;height:100%;background:linear-gradient(90deg,#9d12ff,#d12cff)}
.actions{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:14px 0}
.actions button{border:0;border-radius:22px;padding:20px 8px;color:white;font-size:clamp(23px,6vw,34px);font-weight:900}
.buy{background:linear-gradient(145deg,#00df86,#00b85e)}
.sell{background:linear-gradient(145deg,#ff425d,#e60032)}
.connection{display:flex;justify-content:space-around;gap:8px;align-items:center;background:#101426;border:1px solid #39425f;border-radius:28px;padding:15px 8px;color:#c7cde4;font-size:13px}
.ok{color:#00e58b}
.error{color:#ff8798;font-size:11px;overflow-wrap:anywhere;margin-top:8px}
@media(max-width:560px){
 body{padding:9px}
 .coin{width:48px;height:48px;font-size:29px}
 .topgrid{gap:6px}
 .signal{padding:10px 5px}
 .metrics{gap:6px}
 .metric{padding:9px}
 .bottomgrid{gap:7px}
 .card{padding:11px}
 .connection{font-size:11px}
 #chart{height:330px}
}
</style>
</head>

<body>
<div class="wrap">

<header>
<div class="coin">₿</div>
<div class="brand">
<h1>BTC Radar <span>PRO</span> ∿</h1>
<div class="sub">Análisis en Tiempo Real · Coinbase / Kraken</div>
</div>
<div class="live" id="live">● Conectando</div>
<button class="gear" onclick="refresh()">⚙</button>
</header>

<section class="topgrid">
<div class="signal up">
<div class="name">↗ Alcista</div>
<div class="pct" id="bull">50.0%</div>
<div class="meter"><span id="bullbar" style="width:50%;background:#00e58b"></span></div>
</div>

<div class="signal mid">
<div class="name">Predicción 15 min</div>
<div class="target" id="forecastTarget">--</div>
<div class="change" id="forecastChange">Calculando</div>
</div>

<div class="signal down">
<div class="name">↘ Bajista</div>
<div class="pct" id="bear">50.0%</div>
<div class="meter"><span id="bearbar" style="width:50%;background:#ff3155"></span></div>
</div>
</section>

<section class="card chartcard">
<div class="charttitle">
<span>GRÁFICA BTC/USD</span>
<span id="priceLabel">--</span>
</div>

<div class="controls">
<button class="active" data-tf="1">1m</button>
<button data-tf="3">3m</button>
<button data-tf="5">5m</button>
<button data-tf="15">15m</button>

<div class="legend">
<span><i class="dot" style="background:#19a7ff"></i>EMA 9</span>
<span><i class="dot" style="background:#ff9d18"></i>EMA 21</span>
<span><i class="dot" style="background:#b51cff"></i>VWAP</span>
</div>
</div>

<div id="chart"></div>
</section>

<section class="metrics">
<div class="metric">
<small>RSI (14)</small>
<strong class="cyan" id="rsi">--</strong>
</div>
<div class="metric">
<small>MACD</small>
<strong class="green" id="macd">--</strong>
</div>
<div class="metric">
<small>Volumen</small>
<strong class="purple" id="volume">--</strong>
</div>
</section>

<section class="bottomgrid" style="margin-top:10px">
<div class="card">
<div class="bigmetric">
<span class="ico">◴</span>
<div>
<div class="label">Tiempo Restante</div>
<div class="big" id="timer">--:--</div>
</div>
</div>
<div class="progress">
<span id="timerbar" style="width:0%"></span>
</div>
</div>

<div class="card">
<div class="bigmetric">
<span class="ico">⚑</span>
<div>
<div class="label">Precio Actual</div>
<div class="big green" id="price">--</div>
<div class="green" id="priceMove">Esperando precio</div>
</div>
</div>
</div>
</section>

<section class="metrics" style="margin-bottom:12px">
<div class="metric"><small>EMA 9</small><strong class="cyan" id="ema9">--</strong></div>
<div class="metric"><small>EMA 21</small><strong id="ema21" style="color:#ff9d18">--</strong></div>
<div class="metric"><small>VWAP</small><strong class="purple" id="vwap">--</strong></div>
</section>

<div class="actions">
<button class="buy" onclick="showSignal('SUBE')">↗ SUBE</button>
<button class="sell" onclick="showSignal('BAJA')">↘ BAJA</button>
</div>

<div class="connection">
<span id="connection" class="ok">● Conectando...</span>
<span>│</span>
<span id="updated">Actualización: --</span>
</div>

<div class="error" id="error"></div>
</div>

<script>
const $ = id => document.getElementById(id);

const fmt = n =>
 n == null || !Number.isFinite(Number(n))
 ? "--"
 : "$" + Number(n).toLocaleString("en-US",{
 minimumFractionDigits:2,
 maximumFractionDigits:2
});

let timeframe = 1;
let latestData = null;
let chartReady = false;
let initialPrice = null;

const chart = LightweightCharts.createChart(
 $("chart"),
 {
 width:$("chart").clientWidth,
 height:330,
 layout:{
  background:{color:"#080d1c"},
  textColor:"#aeb9d7"
 },
 grid:{
  vertLines:{color:"#1b2540"},
  horzLines:{color:"#1b2540"}
 },
 rightPriceScale:{borderColor:"#293654"},
 timeScale:{
  borderColor:"#293654",
  timeVisible:true,
  secondsVisible:false
 },
 crosshair:{mode:1}
 }
);

const candlesSeries = chart.addCandlestickSeries({
 upColor:"#00d889",
 downColor:"#ff3155",
 borderUpColor:"#00d889",
 borderDownColor:"#ff3155",
 wickUpColor:"#00d889",
 wickDownColor:"#ff3155"
});

const e9Series = chart.addLineSeries({
 color:"#19a7ff",
 lineWidth:2,
 priceLineVisible:false
});

const e21Series = chart.addLineSeries({
 color:"#ff9d18",
 lineWidth:2,
 priceLineVisible:false
});

const vwapSeries = chart.addLineSeries({
 color:"#b51cff",
 lineWidth:2,
 priceLineVisible:false
});

function aggregate(rows, minutes){
 const groups = new Map();

 for(const c of rows){
  const t = Math.floor(c.time/(minutes*60))*(minutes*60);

  if(!groups.has(t)){
   groups.set(t,{
    time:t,
    open:c.open,
    high:c.high,
    low:c.low,
    close:c.close,
    volume:Number(c.volume||0)
   });
  }else{
   const g=groups.get(t);
   g.high=Math.max(g.high,c.high);
   g.low=Math.min(g.low,c.low);
   g.close=c.close;
   g.volume+=Number(c.volume||0);
  }
 }

 return [...groups.values()].sort((a,b)=>a.time-b.time);
}

function calcEma(values,p){
 if(!values.length)return [];

 const alpha=2/(p+1);
 const out=[values[0]];

 for(let i=1;i<values.length;i++){
  out.push(
   alpha*values[i]+(1-alpha)*out[i-1]
  );
 }

 return out;
}

function lineData(values,rows){
 return values.map((v,i)=>({
  time:rows[i].time,
  value:v
 }));
}

function draw(rows){
 if(!rows.length)return;

 const closes=rows.map(c=>c.close);

 candlesSeries.setData(
  rows.map(c=>({
   time:c.time,
   open:c.open,
   high:c.high,
   low:c.low,
   close:c.close
  }))
 );

 e9Series.setData(lineData(calcEma(closes,9),rows));
 e21Series.setData(lineData(calcEma(closes,21),rows));

 let vol=0,val=0;

 const vw=rows.map(c=>{
  const typical=(c.high+c.low+c.close)/3;
  vol+=Number(c.volume||0);
  val+=typical*Number(c.volume||0);
  return vol?val/vol:c.close;
 });

 vwapSeries.setData(lineData(vw,rows));

 if(!chartReady){
  chart.timeScale().fitContent();
  chartReady=true;
 }
}

function render(data){
 latestData=data;

 const connected=!!data.connected;

 $("live").textContent=connected
  ?"● En Vivo"
  :"● Sin conexión";

 $("live").style.color=connected
  ?"#00e58b"
  :"#ff526c";

 $("connection").textContent=connected
  ?"● Conectado: "+data.provider
  :"● Sin conexión";

 $("connection").className=connected?"ok":"";

 $("updated").textContent=data.updated==null
  ?"Actualización: --"
  :"Actualización: "+data.updated+"s";

 $("error").textContent=connected
  ?""
  :(data.error||"Esperando respuesta de los proveedores");

 if(data.price){
  $("price").textContent=fmt(data.price);
  $("priceLabel").textContent=fmt(data.price);

  if(initialPrice===null)initialPrice=data.price;

  const diff=data.price-initialPrice;
  const pct=initialPrice?diff/initialPrice*100:0;

  $("priceMove").textContent=
   (diff>=0?"+":"")+fmt(Math.abs(diff))+
   " ("+(pct>=0?"+":"")+pct.toFixed(2)+"%)";

  $("priceMove").style.color=
   diff>=0?"#00e58b":"#ff3155";
 }

 const a=data.analysis||{};
 const f=data.forecast||{};

 $("bull").textContent=(a.bull??50).toFixed(1)+"%";
 $("bear").textContent=(a.bear??50).toFixed(1)+"%";

 $("bullbar").style.width=(a.bull??50)+"%";
 $("bearbar").style.width=(a.bear??50)+"%";

 $("forecastTarget").textContent=f.target
  ?((f.direction==="BAJA"?"↓ ":"↑ ")+fmt(f.target))
  :"REUNIENDO DATOS";

 $("forecastChange").textContent=
  f.change==null
  ?"Calculando"
  :((f.change>=0?"+":"")+f.change.toFixed(2)+
    "% · Confianza técnica "+f.confidence+"%");

 $("rsi").textContent=a.rsi??"--";
 $("macd").textContent=a.macd??"--";

 $("ema9").textContent=fmt(a.ema9);
 $("ema21").textContent=fmt(a.ema21);
 $("vwap").textContent=fmt(a.vwap);

 const raw=data.candles||[];
 draw(aggregate(raw,timeframe));

 const total=raw.reduce(
  (s,c)=>s+Number(c.volume||0),0
 );

 $("volume").textContent=total>=1000
  ?(total/1000).toFixed(1)+"K"
  :total.toFixed(2);
}

async function refresh(){
 try{
  const response=await fetch(
   "/api/data?_="+Date.now(),
   {cache:"no-store"}
  );

  if(!response.ok)
   throw new Error("HTTP "+response.status);

  render(await response.json());

 }catch(e){
  $("live").textContent="● Error de conexión";
  $("live").style.color="#ff526c";

  $("connection").textContent=
   "● No se pudo consultar el servidor";

  $("error").textContent=String(e);
 }
}

function updateTimer(){
 const now=Math.floor(Date.now()/1000);
 const remaining=900-(now%900);

 const m=String(Math.floor(remaining/60)).padStart(2,"0");
 const s=String(remaining%60).padStart(2,"0");

 $("timer").textContent=m+":"+s;

 $("timerbar").style.width=
  ((900-remaining)/900*100)+"%";
}

function showSignal(side){
 const f=latestData?.forecast;

 alert(
  "Radar técnico: "+side+
  "\nPredicción: "+(f?.direction||"Sin datos")+
  "\nEsto no ejecuta operaciones ni garantiza resultados."
 );
}

document.querySelectorAll("[data-tf]").forEach(btn=>{
 btn.addEventListener("click",()=>{
  document.querySelectorAll("[data-tf]").forEach(
   b=>b.classList.remove("active")
  );

  btn.classList.add("active");
  timeframe=Number(btn.dataset.tf);
  chartReady=false;

  if(latestData){
   draw(aggregate(latestData.candles||[],timeframe));
  }
 });
});

window.addEventListener("resize",()=>{
 chart.applyOptions({
  width:$("chart").clientWidth
 });
});

refresh();
updateTimer();

setInterval(refresh,1000);
setInterval(updateTimer,250);

</script>
</body>
</html>
"""

start_worker()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT",10000)),
        debug=False
    )
