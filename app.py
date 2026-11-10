
import os
import time
import logging
import threading
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("btc-radar")

PRODUCT = "BTC-USD"
COINBASE = "https://api.exchange.coinbase.com"
BINANCE_US = "https://api.binance.us"
KRAKEN = "https://api.kraken.com"

lock = threading.RLock()
candles = deque(maxlen=1800)

current_price = 0.0
last_update = 0.0
connected = False
source_name = "SIN CONEXIÓN"
last_error = ""
_started = False
start_lock = threading.Lock()


def make_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "BTC-Radar-PRO/5.0",
        "Accept": "application/json"
    })
    return session


session = make_session()


def number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def minute_time(timestamp=None):
    timestamp = time.time() if timestamp is None else timestamp
    return int(timestamp // 60) * 60


def update_candle(price, volume=0, timestamp=None):
    global current_price, last_update, connected

    if price <= 0:
        return

    stamp = minute_time(timestamp)

    with lock:
        current_price = price
        last_update = time.time()
        connected = True

        if not candles:
            candles.append({
                "time": stamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": max(0, volume)
            })
            return

        last = candles[-1]

        if stamp == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
            last["volume"] += max(0, volume)

        elif stamp > last["time"]:
            next_stamp = last["time"] + 60

            while next_stamp < stamp:
                previous = candles[-1]["close"]
                candles.append({
                    "time": next_stamp,
                    "open": previous,
                    "high": previous,
                    "low": previous,
                    "close": previous,
                    "volume": 0
                })
                next_stamp += 60

            candles.append({
                "time": stamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": max(0, volume)
            })


def get_coinbase_price():
    url = f"{COINBASE}/products/{PRODUCT}/ticker"
    response = session.get(url, timeout=7)
    response.raise_for_status()
    price = number(response.json().get("price"))

    if price <= 0:
        raise ValueError("Coinbase devolvió un precio inválido")

    return price


def get_binance_price():
    url = f"{BINANCE_US}/api/v3/ticker/price"
    response = session.get(
        url,
        params={"symbol": "BTCUSDT"},
        timeout=7
    )
    response.raise_for_status()
    price = number(response.json().get("price"))

    if price <= 0:
        raise ValueError("Binance.US devolvió un precio inválido")

    return price


def get_kraken_price():
    url = f"{KRAKEN}/0/public/Ticker"
    response = session.get(
        url,
        params={"pair": "XBTUSD"},
        timeout=7
    )
    response.raise_for_status()

    result = response.json().get("result", {})
    if not result:
        raise ValueError("Kraken no devolvió datos")

    ticker = next(iter(result.values()))
    price = number(ticker.get("c", [0])[0])

    if price <= 0:
        raise ValueError("Kraken devolvió un precio inválido")

    return price


def get_live_price():
    global source_name, last_error

    providers = [
        ("Coinbase", get_coinbase_price),
        ("Binance.US", get_binance_price),
        ("Kraken", get_kraken_price)
    ]

    errors = []

    for name, provider in providers:
        try:
            price = provider()

            with lock:
                source_name = name
                last_error = ""

            return price, name

        except Exception as exc:
            errors.append(f"{name}: {str(exc)[:120]}")
            log.warning("%s no disponible: %s", name, exc)

    with lock:
        last_error = " | ".join(errors)
        source_name = "SIN CONEXIÓN"

    raise ConnectionError(last_error)


def load_history():
    global last_error

    # Primero intenta cargar 500 velas desde Binance.US.
    try:
        response = session.get(
            f"{BINANCE_US}/api/v3/klines",
            params={
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 500
            },
            timeout=12
        )
        response.raise_for_status()

        rows = response.json()
        parsed = []

        for row in rows:
            parsed.append({
                "time": int(row[0] // 1000),
                "open": number(row[1]),
                "high": number(row[2]),
                "low": number(row[3]),
                "close": number(row[4]),
                "volume": number(row[5])
            })

        if parsed:
            with lock:
                candles.clear()
                candles.extend(parsed)
                current = parsed[-1]["close"]

            log.info("Historial Binance.US: %s velas", len(parsed))
            return

    except Exception as exc:
        log.warning("Historial Binance.US falló: %s", exc)

    # Respaldo: Coinbase permite pedir un bloque menor de velas.
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=280)

        response = session.get(
            f"{COINBASE}/products/{PRODUCT}/candles",
            params={
                "granularity": 60,
                "start": start.isoformat(),
                "end": end.isoformat()
            },
            timeout=12
        )
        response.raise_for_status()

        rows = sorted(response.json(), key=lambda x: x[0])
        parsed = []

        for row in rows:
            if len(row) < 6:
                continue

            parsed.append({
                "time": int(row[0]),
                "low": number(row[1]),
                "high": number(row[2]),
                "open": number(row[3]),
                "close": number(row[4]),
                "volume": number(row[5])
            })

        if parsed:
            with lock:
                candles.clear()
                candles.extend(parsed)

            log.info("Historial Coinbase: %s velas", len(parsed))
            return

    except Exception as exc:
        log.warning("Historial Coinbase falló: %s", exc)

    last_error = "No se pudo cargar el historial. Esperando precios en vivo."


def price_worker():
    global connected, last_error

    while True:
        try:
            price, provider = get_live_price()
            update_candle(price)

            with lock:
                connected = True
                source_name = provider

        except Exception as exc:
            log.error("No hay fuente de precio disponible: %s", exc)

            with lock:
                connected = (
                    bool(last_update)
                    and time.time() - last_update < 20
                )

        time.sleep(3)


def ema(values, period):
    if not values:
        return []

    alpha = 2 / (period + 1)
    output = [values[0]]

    for value in values[1:]:
        output.append(
            value * alpha + output[-1] * (1 - alpha)
        )

    return output


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
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
    return 100 - 100 / (1 + rs)


def calculate_vwap(data):
    total_volume = 0.0
    total_value = 0.0
    output = []

    for candle in data:
        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3

        volume = max(0, candle["volume"])
        total_volume += volume
        total_value += typical * volume

        output.append(
            total_value / total_volume
            if total_volume else candle["close"]
        )

    return output


def aggregate_candles(data, minutes):
    if minutes == 1:
        return list(data)

    grouped = {}

    for candle in data:
        stamp = (
            candle["time"] // (minutes * 60)
        ) * (minutes * 60)

        if stamp not in grouped:
            grouped[stamp] = {
                "time": stamp,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"]
            }
        else:
            item = grouped[stamp]
            item["high"] = max(item["high"], candle["high"])
            item["low"] = min(item["low"], candle["low"])
            item["close"] = candle["close"]
            item["volume"] += candle["volume"]

    return sorted(grouped.values(), key=lambda x: x["time"])


def analyze(data):
    closes = [x["close"] for x in data]

    if len(closes) < 30:
        return {
            "bull": 50,
            "bear": 50,
            "neutral": 0,
            "direction": "REUNIENDO DATOS",
            "score": 0,
            "rsi": 50,
            "ema9": None,
            "ema21": None,
            "vwap": None,
            "histogram": 0
        }

    fast = ema(closes, 9)
    slow = ema(closes, 21)
    vw = calculate_vwap(data)
    rv = calculate_rsi(closes)

    macd_line = [
        a - b
        for a, b in zip(ema(closes, 12), ema(closes, 26))
    ]
    signal = ema(macd_line, 9)
    histogram = macd_line[-1] - signal[-1]

    momentum = (
        (closes[-1] - closes[-6]) / closes[-6] * 100
        if closes[-6] else 0
    )

    score = 0
    score += 2 if fast[-1] > slow[-1] else -2
    score += 1.5 if closes[-1] > vw[-1] else -1.5
    score += 1.5 if histogram > 0 else -1.5
    score += max(-2, min(2, momentum * 8))

    if 50 < rv < 70:
        score += 1
    elif 30 < rv < 50:
        score -= 1

    score = max(-8, min(8, score))

    bullish = 50 + score * 5
    bearish = 50 - score * 5
    neutral = max(5, 100 - abs(score) / 8 * 100)

    total = bullish + bearish + neutral
    bullish = bullish / total * 100
    bearish = bearish / total * 100
    neutral = neutral / total * 100

    if bullish > bearish and bullish > neutral:
        direction = "ALCISTA"
    elif bearish > bullish and bearish > neutral:
        direction = "BAJISTA"
    else:
        direction = "INDECISO"

    return {
        "bull": round(bullish, 1),
        "bear": round(bearish, 1),
        "neutral": round(neutral, 1),
        "direction": direction,
        "score": round(score, 2),
        "rsi": round(rv, 2),
        "ema9": round(fast[-1], 2),
        "ema21": round(slow[-1], 2),
        "vwap": round(vw[-1], 2),
        "histogram": round(histogram, 4)
    }


def forecast_15m(data, price, prediction):
    closes = [x["close"] for x in data if x["close"] > 0]

    if len(closes) < 30:
        return {
            "target": None,
            "change_pct": None,
            "direction": "ESPERANDO DATOS",
            "range_pct": None
        }

    recent = closes[-30:]
    returns = [
        recent[i] / recent[i - 1] - 1
        for i in range(1, len(recent))
        if recent[i - 1] > 0
    ]

    returns.sort()

    if len(returns) > 4:
        returns = returns[2:-2]

    average = sum(returns) / max(1, len(returns))

    variance = sum(
        (x - average) ** 2 for x in returns
    ) / max(1, len(returns))

    volatility = variance ** 0.5
    score = number(prediction.get("score"))

    raw_pct = (
        average * 15 * 100 * 0.45
        + score / 8 * volatility * 15 * 100 * 0.55
    )

    cap = max(
        0.04,
        min(1.20, volatility * (15 ** 0.5) * 100 * 1.5)
    )

    projected = max(-cap, min(cap, raw_pct))
    target = price * (1 + projected / 100)

    if projected > 0.015:
        direction = "SUBE"
    elif projected < -0.015:
        direction = "BAJA"
    else:
        direction = "LATERAL"

    return {
        "target": round(target, 2),
        "change_pct": round(projected, 3),
        "direction": direction,
        "range_pct": round(cap, 3)
    }
    
HTML = r'''
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{box-sizing:border-box}
body{margin:0;background:#050610;color:#f5f4ff;font-family:Arial,sans-serif}
header{padding:16px;border-bottom:1px solid #29263f;display:flex;justify-content:space-between;align-items:center}
h1{font-size:20px;margin:0;color:#b98aff}
#live{font-size:12px;color:#ff657e}
main{max-width:1100px;margin:auto;padding:14px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.card,.tile{background:#101225;border:1px solid #292b49;border-radius:12px;padding:13px;min-width:0}
.label,small{display:block;color:#999bb9;font-size:12px;margin-bottom:8px}
.pct{font-size:25px;font-weight:bold}
.green{color:#00e39a}
.red{color:#ff5574}
.purple{color:#bb8cff}
.direction{font-size:19px;font-weight:bold}
.bar{height:6px;background:#292b49;border-radius:9px;overflow:hidden;margin-top:10px}
.bar span{display:block;height:100%;width:0%;background:#00e39a}
.bar.red span{background:#ff5574}
.bar.purple span{background:#bb8cff}
.tabs{display:flex;gap:8px;margin:14px 0}
button{background:#171a30;color:#ddd;border:1px solid #383b5d;border-radius:8px;padding:9px 16px}
button.active{background:#39245c;border-color:#bb8cff;color:white}
.chart{height:340px;width:100%;background:#080a16;border:1px solid #292b49;border-radius:12px;overflow:hidden}
.indicators,.bottom{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:12px}
.bottom{grid-template-columns:repeat(4,1fr)}
.tile strong{font-size:19px;display:block;overflow-wrap:anywhere}
.status{margin-top:14px;padding:12px;border-radius:10px;background:#101225;border:1px solid #292b49;font-size:12px;color:#b9bad0;overflow-wrap:anywhere}
.note{font-size:11px;line-height:1.5;color:#898ba6;margin:14px 2px}
@media(max-width:650px){
 .grid{grid-template-columns:1fr 1fr}
 .bottom{grid-template-columns:1fr 1fr}
 .indicators{grid-template-columns:1fr 1fr}
 .chart{height:300px}
}
</style>
</head>
<body>
<header>
 <h1>₿ BTC RADAR PRO</h1>
 <div id="live">● CONECTANDO</div>
</header>

<main>
 <section class="grid">
  <div class="card">
   <div class="label">Señal técnica alcista</div>
   <div class="pct green" id="bull">--%</div>
   <div class="bar"><span id="bullbar"></span></div>
  </div>
  <div class="card">
   <div class="label">Dirección técnica</div>
   <div class="direction purple" id="direction">ESPERANDO</div>
   <small id="score">Reuniendo datos</small>
  </div>
  <div class="card">
   <div class="label">Señal técnica bajista</div>
   <div class="pct red" id="bear">--%</div>
   <div class="bar red"><span id="bearbar"></span></div>
  </div>
 </section>

 <div class="tabs">
  <button class="tf active" data-tf="1">1m</button>
  <button class="tf" data-tf="3">3m</button>
  <button class="tf" data-tf="5">5m</button>
  <button class="tf" data-tf="15">15m</button>
 </div>

 <div class="chart" id="chart"></div>

 <section class="indicators">
  <div class="tile"><small>RSI (14)</small><strong id="rsi">--</strong></div>
  <div class="tile"><small>MACD</small><strong id="macd">--</strong></div>
  <div class="tile"><small>EMA 9 / EMA 21</small><strong id="emas">--</strong></div>
 </section>

 <section class="bottom">
  <div class="tile">
   <small>Tiempo restante del bloque 15m</small>
   <strong class="purple" id="countdown">15:00</strong>
   <div class="bar purple"><span id="timebar" style="width:100%"></span></div>
  </div>
  <div class="tile">
   <small>Precio BTC/USD</small>
   <strong class="green" id="price">$--</strong>
   <small id="updated">Esperando precio</small>
  </div>
  <div class="tile">
   <small>Estimación técnica a 15m</small>
   <strong class="purple" id="target">$--</strong>
   <small id="targetchange">Esperando datos</small>
  </div>
  <div class="tile">
   <small>Escenario estimado</small>
   <strong id="forecastdir">--</strong>
   <small>No es una garantía</small>
  </div>
 </section>

 <div class="status" id="status">Conectando con fuentes de precio...</div>
 <p class="note">
  Coinbase, Binance.US y Kraken son fuentes externas. El sistema cambia
  automáticamente a otra fuente si la principal falla. Las señales son
  cálculos técnicos, no probabilidades estadísticas verificadas. El precio
  spot puede diferir del índice BRTI utilizado por algunos contratos Kalshi.
  Este radar no ejecuta operaciones.
 </p>
</main>

<script>
const $ = id => document.getElementById(id);
let tf = 1;
let state = null;
let chart, candlesSeries, ema9Series, ema21Series, vwapSeries, volumeSeries;

const fmt = x => Number(x).toLocaleString("en-US", {
 minimumFractionDigits:2,
 maximumFractionDigits:2
});

function initChart(){
 if(!window.LightweightCharts){
  $("status").textContent="No se pudo cargar la librería de gráficas.";
  return;
 }

 chart=LightweightCharts.createChart($("chart"),{
  width:$("chart").clientWidth,
  height:$("chart").clientHeight,
  layout:{
   background:{color:"#080a16"},
   textColor:"#c7c4dc"
  },
  grid:{
   vertLines:{color:"#202238"},
   horzLines:{color:"#202238"}
  },
  timeScale:{
   timeVisible:true,
   secondsVisible:false,
   borderColor:"#34314f"
  },
  rightPriceScale:{borderColor:"#34314f"}
 });

 candlesSeries=chart.addCandlestickSeries({
  upColor:"#00d98a",
  downColor:"#ff4166",
  borderUpColor:"#00d98a",
  borderDownColor:"#ff4166",
  wickUpColor:"#00d98a",
  wickDownColor:"#ff4166"
 });

 ema9Series=chart.addLineSeries({color:"#00aaff",lineWidth:2});
 ema21Series=chart.addLineSeries({color:"#ff9d2e",lineWidth:2});
 vwapSeries=chart.addLineSeries({color:"#b34cff",lineWidth:2});

 volumeSeries=chart.addHistogramSeries({
  priceScaleId:"",
  priceFormat:{type:"volume"},
  scaleMargins:{top:.82,bottom:0}
 });
}

function ema(values,period){
 if(!values.length)return [];
 const k=2/(period+1);
 const output=[values[0]];

 for(let i=1;i<values.length;i++){
  output.push(values[i]*k+output[i-1]*(1-k));
 }
 return output;
}

function drawChart(){
 if(!state || !chart)return;

 const data=state.timeframes[String(tf)]||[];
 if(!data.length)return;

 candlesSeries.setData(data.map(x=>({
  time:x.time,
  open:x.open,
  high:x.high,
  low:x.low,
  close:x.close
 })));

 const closes=data.map(x=>x.close);
 const fast=ema(closes,9);
 const slow=ema(closes,21);

 let volume=0;
 let value=0;
 const vw=[];

 data.forEach(x=>{
  const typical=(x.high+x.low+x.close)/3;
  volume+=x.volume;
  value+=typical*x.volume;
  vw.push(volume?value/volume:x.close);
 });

 ema9Series.setData(fast.map((v,i)=>({time:data[i].time,value:v})));
 ema21Series.setData(slow.map((v,i)=>({time:data[i].time,value:v})));
 vwapSeries.setData(vw.map((v,i)=>({time:data[i].time,value:v})));

 volumeSeries.setData(data.map(x=>({
  time:x.time,
  value:x.volume,
  color:x.close>=x.open?"#00d98a66":"#ff416666"
 })));

 chart.timeScale().scrollToRealTime();
}

function render(s){
 state=s;

 const p=s.prediction;
 const f=s.forecast;

 $("bull").textContent=p.bull+"%";
 $("bear").textContent=p.bear+"%";
 $("bullbar").style.width=p.bull+"%";
 $("bearbar").style.width=p.bear+"%";

 $("direction").textContent=p.direction;
 $("direction").className="direction "+
  (p.direction==="ALCISTA"?"green":
   p.direction==="BAJISTA"?"red":"purple");

 $("score").textContent="Puntuación técnica: "+p.score;
 $("rsi").textContent=p.rsi;
 $("macd").textContent=p.histogram;
 $("emas").textContent=p.ema9!==null
  ?fmt(p.ema9)+" / "+fmt(p.ema21)
  :"Reuniendo datos";

 $("price").textContent="$"+fmt(s.price);
 $("target").textContent=f.target!==null
  ?"$"+fmt(f.target)
  :"Esperando datos";

 $("targetchange").textContent=f.change_pct!==null
  ?(f.change_pct>0?"+":"")+f.change_pct+"% estimado"
  :"Sin estimación";

 $("forecastdir").textContent=f.direction;
 $("forecastdir").className=
  f.direction==="SUBE"?"green":
  f.direction==="BAJA"?"red":"purple";

 $("live").textContent=s.connected?"● EN VIVO":"● SIN CONEXIÓN";
 $("live").style.color=s.connected?"#00e39a":"#ff657e";

 $("status").textContent=
  "Fuente: "+s.source+
  " · Actualizado hace "+s.update_age+" s"+
  " · Estado: "+(s.connected?"conectado":"sin conexión")+
  (s.last_error?" · Aviso: "+s.last_error:"");

 $("updated").textContent="Fuente: "+s.source;
 drawChart();
}

async function refresh(){
 try{
  const response=await fetch("/api/state?t="+Date.now(),{
   cache:"no-store"
  });

  if(!response.ok)throw new Error("HTTP "+response.status);
  render(await response.json());

 }catch(error){
  $("live").textContent="● REINTENTANDO";
  $("live").style.color="#ffb347";
  $("status").textContent=
   "No llegan datos. El radar seguirá intentando reconectar.";
 }
}

function updateClock(){
 const remaining=900-Math.floor(Date.now()/1000)%900;
 const mins=String(Math.floor(remaining/60)).padStart(2,"0");
 const secs=String(remaining%60).padStart(2,"0");

 $("countdown").textContent=mins+":"+secs;
 $("timebar").style.width=(remaining/900*100)+"%";
}

document.querySelectorAll(".tf").forEach(button=>{
 button.addEventListener("click",()=>{
  document.querySelectorAll(".tf").forEach(x=>x.classList.remove("active"));
  button.classList.add("active");
  tf=Number(button.dataset.tf);
  drawChart();
 });
});

initChart();

window.addEventListener("resize",()=>{
 if(chart){
  chart.applyOptions({
   width:$("chart").clientWidth,
   height:$("chart").clientHeight
  });
 }
});

refresh();
updateClock();
setInterval(refresh,3000);
setInterval(updateClock,250);
</script>
</body>
</html>
'''


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/health")
def health():
    with lock:
        age = (
            round(time.time() - last_update, 1)
            if last_update else None
        )

        return jsonify({
            "ok": True,
            "connected": connected,
            "source": source_name,
            "candles": len(candles),
            "last_update_age": age,
            "last_error": last_error
        })


@app.route("/api/state")
def api_state():
    with lock:
        raw = list(candles)
        price = current_price
        updated = last_update
        provider = source_name
        error = last_error

    is_connected = (
        price > 0
        and updated > 0
        and time.time() - updated < 20
    )

    if not raw or price <= 0:
        return jsonify({
            "error": "Esperando datos de las fuentes",
            "connected": False,
            "source": provider,
            "last_error": error
        }), 503

    frames = {
        str(n): aggregate_candles(raw, n)[-240:]
        for n in (1, 3, 5, 15)
    }

    prediction = analyze(raw)
    forecast = forecast_15m(raw, price, prediction)

    base = raw[0]["open"] or price
    change = (price - base) / base * 100 if base else 0

    return jsonify({
        "price": round(price, 2),
        "change": round(change, 3),
        "connected": is_connected,
        "source": provider,
        "update_age": round(time.time()-updated, 1),
        "last_error": error,
        "prediction": prediction,
        "forecast": forecast,
        "timeframes": frames
    })


def start_background():
    global _started

    with start_lock:
        if _started:
            return
        _started = True

    load_history()

    threading.Thread(
        target=price_worker,
        daemon=True,
        name="price-worker"
    ).start()


start_background()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        threaded=True
    )
    
