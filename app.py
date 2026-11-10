
import os
import json
import time
import threading
import logging
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("btc-radar")

PRODUCT = "BTC-USD"
REST_URL = f"https://api.exchange.coinbase.com/products/{PRODUCT}"
WS_URL = "wss://ws-feed.exchange.coinbase.com"

lock = threading.RLock()
candles = deque(maxlen=1500)

current_price = 0.0
last_update = 0.0
connected = False
ws_connected = False
last_error = ""


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def minute_timestamp(timestamp=None):
    timestamp = time.time() if timestamp is None else timestamp
    return int(timestamp // 60) * 60


def update_candle(price, size=0.0, timestamp=None):
    global current_price, last_update, connected

    if price <= 0:
        return

    stamp = minute_timestamp(timestamp)

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
                "volume": max(0, size)
            })
            return

        last = candles[-1]

        if stamp == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
            last["volume"] += max(0, size)

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
                "volume": max(0, size)
            })


def load_history():
    global current_price, last_update

    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=500)

    try:
        response = requests.get(
            REST_URL + "/candles",
            params={
                "granularity": 60,
                "start": start.isoformat(),
                "end": end.isoformat()
            },
            timeout=15
        )

        response.raise_for_status()

        rows = sorted(response.json(), key=lambda x: x[0])

        parsed = []

        for row in rows:
            if len(row) < 6:
                continue

            parsed.append({
                "time": int(row[0]),
                "low": safe_float(row[1]),
                "high": safe_float(row[2]),
                "open": safe_float(row[3]),
                "close": safe_float(row[4]),
                "volume": safe_float(row[5])
            })

        with lock:
            candles.clear()
            candles.extend(parsed)

            if parsed:
                current_price = parsed[-1]["close"]
                last_update = time.time()

        log.info("Velas históricas cargadas: %s", len(parsed))

    except Exception as exc:
        log.exception("Error cargando historial: %s", exc)


def on_message(ws, message):
    global connected, ws_connected, last_error

    try:
        data = json.loads(message)
        message_type = data.get("type")

        if message_type == "error":
            last_error = str(data)
            log.error("Coinbase error: %s", data)
            return

        if message_type == "subscriptions":
            ws_connected = True
            connected = True
            last_error = ""
            return

        if message_type == "heartbeat":
            ws_connected = True
            return

        if message_type != "ticker":
            return

        if data.get("product_id") != PRODUCT:
            return

        price = safe_float(data.get("price"))
        size = safe_float(data.get("last_size"))

        timestamp = None

        if data.get("time"):
            try:
                timestamp = datetime.fromisoformat(
                    data["time"].replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                pass

        if price > 0:
            update_candle(price, size, timestamp)
            connected = True
            ws_connected = True
            last_error = ""

    except Exception as exc:
        log.exception("Error procesando mensaje WS: %s", exc)


def on_open(ws):
    global ws_connected

    ws_connected = False

    subscription = {
        "type": "subscribe",
        "product_ids": [PRODUCT],
        "channels": ["ticker", "heartbeat"]
    }

    ws.send(json.dumps(subscription))
    log.info("Suscripción Coinbase Exchange enviada")


def on_close(ws, code, message):
    global ws_connected

    ws_connected = False
    log.warning("WebSocket cerrado: %s %s", code, message)


def on_error(ws, error):
    global ws_connected, last_error

    ws_connected = False
    last_error = str(error)
    log.warning("WebSocket error: %s", error)


def websocket_worker():
    while True:
        try:
            client = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_close=on_close,
                on_error=on_error
            )

            client.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            log.warning("WebSocket worker: %s", exc)

        time.sleep(3)


def rest_worker():
    global connected, last_error

    session = requests.Session()

    session.headers.update({
        "User-Agent": "BTC-Radar-PRO/3.0",
        "Accept": "application/json"
    })

    while True:
        try:
            response = session.get(
                REST_URL + "/ticker",
                timeout=8
            )

            response.raise_for_status()

            data = response.json()
            price = safe_float(data.get("price"))

            if price <= 0:
                raise ValueError("Precio inválido de Coinbase")

            update_candle(price)

            connected = True
            last_error = ""

        except Exception as primary_error:

            try:
                response = session.get(
                    "https://api.coinbase.com/v2/prices/BTC-USD/spot",
                    timeout=8
                )

                response.raise_for_status()

                data = response.json()
                price = safe_float(
                    data.get("data", {}).get("amount")
                )

                if price <= 0:
                    raise ValueError("Precio spot inválido")

                update_candle(price)

                connected = True
                last_error = ""

            except Exception as backup_error:

                last_error = (
                    f"{primary_error}; respaldo: {backup_error}"
                )

                with lock:
                    connected = (
                        bool(last_update)
                        and time.time() - last_update < 20
                    )

                log.warning("Error de precio: %s", last_error)

        time.sleep(3)


def ema(values, period):
    if not values:
        return []

    alpha = 2 / (period + 1)
    result = [values[0]]

    for value in values[1:]:
        result.append(
            value * alpha + result[-1] * (1 - alpha)
        )

    return result


def rsi(values, period=14):
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
    return 100 - (100 / (1 + rs))


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

        volume = max(0, candle["volume"])

        total_volume += volume
        total_value += typical * volume

        result.append(
            total_value / total_volume
            if total_volume
            else candle["close"]
        )

    return result


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

    return sorted(
        grouped.values(),
        key=lambda x: x["time"]
    )


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

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    vw = calculate_vwap(data)
    rv = rsi(closes)

    macd_line = [
        a - b
        for a, b in zip(
            ema(closes, 12),
            ema(closes, 26)
        )
    ]

    signal = ema(macd_line, 9)
    histogram = macd_line[-1] - signal[-1]

    momentum = (
        (closes[-1] - closes[-6])
        / closes[-6] * 100
        if closes[-6]
        else 0
    )

    score = 0
    score += 2 if ema9[-1] > ema21[-1] else -2
    score += 1.5 if closes[-1] > vw[-1] else -1.5
    score += 1.5 if histogram > 0 else -1.5
    score += max(-2, min(2, momentum * 8))

    if 50 < rv < 70 or rv <= 25:
        score += 1
    elif 30 < rv < 50 or rv >= 75:
        score -= 1

    score = max(-8, min(8, score))

    strength = abs(score) / 8
    bull_raw = 50 + score * 5
    bear_raw = 50 - score * 5
    neutral_raw = max(5, 100 - strength * 100)

    total = bull_raw + bear_raw + neutral_raw

    bull = bull_raw / total * 100
    bear = bear_raw / total * 100
    neutral = neutral_raw / total * 100

    if bull > bear and bull > neutral:
        direction = "ALCISTA"
    elif bear > bull and bear > neutral:
        direction = "BAJISTA"
    else:
        direction = "INDECISO"

    return {
        "bull": round(bull, 1),
        "bear": round(bear, 1),
        "neutral": round(neutral, 1),
        "direction": direction,
        "score": round(score, 2),
        "rsi": round(rv, 2),
        "ema9": round(ema9[-1], 2),
        "ema21": round(ema21[-1], 2),
        "vwap": round(vw[-1], 2),
        "histogram": round(histogram, 4)
    }


def forecast_15m(data, price, prediction):
    closes = [
        x["close"]
        for x in data
        if x["close"] > 0
    ]

    if len(closes) < 30:
        return {
            "target": None,
            "change_pct": None,
            "direction": "ESPERANDO DATOS"
        }

    recent = closes[-30:]

    returns = []

    for i in range(1, len(recent)):
        if recent[i - 1] > 0:
            returns.append(
                recent[i] / recent[i - 1] - 1
            )

    returns.sort()

    if len(returns) > 4:
        returns = returns[2:-2]

    average = sum(returns) / max(1, len(returns))

    volatility = (
        sum((x - average) ** 2 for x in returns)
        / max(1, len(returns))
    ) ** 0.5

    score = safe_float(prediction.get("score"))

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

body{
 margin:0;
 background:#050610;
 color:#f5f4ff;
 font-family:Arial,sans-serif
}

header{
 padding:15px;
 border-bottom:1px solid #29263f;
 display:flex;
 gap:12px;
 align-items:center
}

.coin{
 background:#f7931a;
 border-radius:50%;
 width:46px;
 height:46px;
 display:grid;
 place-items:center;
 font-size:30px
}

.brand{font-size:23px;font-weight:bold}
.brand b{color:#b12cff}
.sub,small,.label{color:#aaa7c5}
.live{margin-left:auto;color:#00e88b;font-size:12px}

.wrap{max-width:1000px;margin:auto;padding:10px}

.cards,.indicators,.bottom{
 display:grid;
 grid-template-columns:repeat(3,1fr);
 gap:8px
}

.card,.tile{
 background:#0b0d1c;
 border:1px solid #34314f;
 border-radius:14px;
 padding:12px;
 text-align:center;
 min-width:0
}

.label{font-size:12px}
.pct{font-size:clamp(24px,6vw,34px);font-weight:bold;margin:9px}

.green{color:#00e58b}
.red{color:#ff4166}
.purple{color:#bd55ff}

.direction{font-size:21px;font-weight:bold;margin:10px}

.bar{
 height:8px;
 background:#28283a;
 border-radius:20px;
 overflow:hidden
}

.bar span{
 display:block;
 height:100%;
 background:currentColor;
 width:0
}

.tabs{
 display:flex;
 gap:8px;
 justify-content:center;
 margin:14px;
 flex-wrap:wrap
}

.tf{
 background:#17172a;
 color:#ddd;
 border:1px solid #393650;
 border-radius:9px;
 padding:9px 16px
}

.tf.active{background:#8500ff;color:white}

.chart{
 height:370px;
 border:1px solid #302d49;
 border-radius:14px;
 overflow:hidden
}

.indicators{margin-top:9px}

.tile{text-align:left}
.tile strong{display:block;font-size:20px;margin-top:8px}

.bottom{
 grid-template-columns:repeat(2,1fr);
 margin-top:9px
}

.bottom .tile strong{
 font-size:clamp(22px,5vw,30px)
}

.status{
 text-align:center;
 padding:13px;
 color:#c9c5df;
 font-size:12px
}

.note{
 text-align:center;
 color:#918da9;
 font-size:11px
}

@media(max-width:420px){
 .chart{height:330px}
 .tile{padding:9px 7px}
}
</style>
</head>

<body>

<header>
<div class="coin">₿</div>

<div>
 <div class="brand">BTC Radar <b>PRO</b></div>
 <div class="sub">Bitcoin / USD · Coinbase · análisis técnico</div>
</div>

<div class="live" id="live">● INICIANDO</div>
</header>

<main class="wrap">

<section class="cards">

<div class="card">
 <div class="label">Probabilidad alcista*</div>
 <div class="pct green" id="bull">--%</div>
 <div class="bar green"><span id="bullbar"></span></div>
</div>

<div class="card">
 <div class="label">Señal técnica</div>
 <div class="direction purple" id="direction">ESPERANDO</div>
 <div class="label" id="score">Calculando...</div>
</div>

<div class="card">
 <div class="label">Probabilidad bajista*</div>
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

<div class="tile">
 <small>RSI (14)</small>
 <strong id="rsi">--</strong>
</div>

<div class="tile">
 <small>MACD histograma</small>
 <strong id="macd">--</strong>
</div>

<div class="tile">
 <small>EMA 9 / EMA 21</small>
 <strong id="emas">--</strong>
</div>

</section>

<section class="bottom">

<div class="tile">
 <small>Tiempo restante del bloque 15m</small>
 <strong class="purple" id="countdown">15:00</strong>
 <div class="bar purple">
  <span id="timebar" style="width:100%"></span>
 </div>
</div>

<div class="tile">
 <small>Precio BTC/USD en vivo</small>
 <strong class="green" id="price">$--</strong>
 <small id="updated">Esperando actualización</small>
</div>

<div class="tile">
 <small>Estimación de precio en 15 minutos</small>
 <strong class="purple" id="target">$--</strong>
 <small id="targetchange">Sin estimación todavía</small>
</div>

<div class="tile">
 <small>Dirección estimada 15m</small>
 <strong id="forecastdir">--</strong>
 <small>Escenario técnico, no garantía</small>
</div>

</section>

<p class="note">
*Puntuación técnica normalizada, no probabilidad estadística calibrada.
Coinbase spot puede diferir del índice BRTI de Kalshi.
No ejecuta operaciones.
</p>

<div class="status" id="status">
Conectando Coinbase...
</div>

</main>

<script>
const $ = id => document.getElementById(id);

let tf = 1;
let state = null;

let chart, cs, e9s, e21s, vws, vs;

const fmt = x => Number(x).toLocaleString(
 'en-US',
 {minimumFractionDigits:2,maximumFractionDigits:2}
);

function init(){

 if(!window.LightweightCharts){
  $('chart').textContent='No se pudo cargar la gráfica';
  return;
 }

 chart=LightweightCharts.createChart(
  $('chart'),
  {
   width:$('chart').clientWidth,
   height:$('chart').clientHeight,

   layout:{
    background:{color:'#080a16'},
    textColor:'#c7c4dc'
   },

   grid:{
    vertLines:{color:'#202238'},
    horzLines:{color:'#202238'}
   },

   timeScale:{
    timeVisible:true,
    borderColor:'#34314f'
   },

   rightPriceScale:{
    borderColor:'#34314f'
   }
  }
 );

 cs=chart.addCandlestickSeries({
  upColor:'#00d98a',
  downColor:'#ff4166',
  borderUpColor:'#00d98a',
  borderDownColor:'#ff4166',
  wickUpColor:'#00d98a',
  wickDownColor:'#ff4166'
 });

 e9s=chart.addLineSeries({
  color:'#00aaff',
  lineWidth:2
 });

 e21s=chart.addLineSeries({
  color:'#ff9d2e',
  lineWidth:2
 });

 vws=chart.addLineSeries({
  color:'#b34cff',
  lineWidth:2
 });

 vs=chart.addHistogramSeries({
  priceScaleId:'',
  priceFormat:{type:'volume'},
  scaleMargins:{top:.82,bottom:0}
 });
}

function ema(values,n){
 if(!values.length)return[];

 let k=2/(n+1);
 let out=[values[0]];

 for(let i=1;i<values.length;i++){
  out.push(values[i]*k+out[i-1]*(1-k));
 }

 return out;
}

function draw(){

 if(!state || !chart)return;

 let data=state.timeframes[String(tf)]||[];

 if(!data.length)return;

 cs.setData(data.map(x=>({
  time:x.time,
  open:x.open,
  high:x.high,
  low:x.low,
  close:x.close
 })));

 let closes=data.map(x=>x.close);

 let a=ema(closes,9);
 let b=ema(closes,21);

 let volume=0;
 let value=0;
 let vw=[];

 data.forEach(x=>{
  let typical=(x.high+x.low+x.close)/3;

  volume+=x.volume;
  value+=typical*x.volume;

  vw.push(volume?value/volume:x.close);
 });

 e9s.setData(a.map((x,i)=>({
  time:data[i].time,
  value:x
 })));

 e21s.setData(b.map((x,i)=>({
  time:data[i].time,
  value:x
 })));

 vws.setData(vw.map((x,i)=>({
  time:data[i].time,
  value:x
 })));

 vs.setData(data.map(x=>({
  time:x.time,
  value:x.volume,
  color:x.close>=x.open?'#00d98a66':'#ff416666'
 })));

 chart.timeScale().scrollToRealTime();
}

function render(s){

 state=s;

 let p=s.prediction;
 let f=s.forecast;

 $('bull').textContent=p.bull+'%';
 $('bear').textContent=p.bear+'%';

 $('bullbar').style.width=p.bull+'%';
 $('bearbar').style.width=p.bear+'%';

 $('direction').textContent=p.direction;

 $('direction').className='direction '+(
  p.direction==='ALCISTA'?'green':
  p.direction==='BAJISTA'?'red':'purple'
 );

 $('score').textContent='Fuerza técnica: '+p.score;

 $('rsi').textContent=p.rsi;
 $('macd').textContent=p.histogram;

 $('emas').textContent=p.ema9
  ?fmt(p.ema9)+' / '+fmt(p.ema21)
  :'--';

 $('price').textContent='$'+fmt(s.price);

 $('target').textContent=f.target
  ?'$'+fmt(f.target)
  :'$--';

 $('targetchange').textContent=f.change_pct!==null
  ?((f.change_pct>0?'+':'')+f.change_pct+'% estimado')
  :'Esperando datos';

 $('forecastdir').textContent=f.direction;

 $('forecastdir').className=
  f.direction==='SUBE'?'green':
  f.direction==='BAJA'?'red':'purple';

 $('live').textContent=s.connected
  ?'● EN VIVO'
  :'● SIN CONEXIÓN';

 $('status').textContent=
  (s.connected?'Coinbase conectado':'Coinbase desconectado')+
  ' · Último dato hace '+s.update_age+' s'+
  ' · Variación desde inicio '+s.change+'%';

 $('updated').textContent='Actualización cada 3 segundos';

 draw();
}

async function refresh(){

 try{

  let response=await fetch(
   '/api/state?t='+Date.now(),
   {cache:'no-store'}
  );

  if(!response.ok)throw Error(response.status);

  render(await response.json());

 }catch(error){

  $('status').textContent=
   'Esperando datos de Coinbase / reconectando';

  $('live').textContent='● SIN DATOS';
 }
}

function clock(){

 let remaining=900-Math.floor(Date.now()/1000)%900;

 $('countdown').textContent=
  String(Math.floor(remaining/60)).padStart(2,'0')+
  ':'+
  String(remaining%60).padStart(2,'0');

 $('timebar').style.width=
  (remaining/900*100)+'%';
}

document.querySelectorAll('.tf').forEach(button=>{

 button.onclick=()=>{

  document.querySelectorAll('.tf').forEach(
   x=>x.classList.remove('active')
  );

  button.classList.add('active');

  tf=Number(button.dataset.tf);

  draw();
 };
});

init();

window.onresize=()=>{

 if(chart){
  chart.applyOptions({
   width:$('chart').clientWidth,
   height:$('chart').clientHeight
  });
 }
};

refresh();
clock();

setInterval(refresh,2000);
setInterval(clock,250);

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
        count = len(candles)
        age = (
            round(time.time() - last_update, 1)
            if last_update else None
        )

    return jsonify({
        "ok": True,
        "candles": count,
        "connected": connected,
        "websocket_connected": ws_connected,
        "last_error": last_error,
        "last_update_age": age
    })


@app.route("/api/state")
def api_state():
    with lock:
        raw = list(candles)
        price = current_price
        updated = last_update

        is_connected = (
            connected
            and bool(updated)
            and time.time() - updated < 20
        )

    if not raw or price <= 0:
        return jsonify({
            "error": "Esperando datos de Coinbase",
            "connected": False,
            "last_error": last_error
        }), 503

    frames = {
        str(n): aggregate_candles(raw, n)[-240:]
        for n in (1, 3, 5, 15)
    }

    prediction = analyze(raw)
    forecast = forecast_15m(raw, price, prediction)

    base = raw[0]["open"] or price

    change = (
        (price - base) / base * 100
        if base else 0
    )

    return jsonify({
        "price": round(price, 2),
        "change": round(change, 3),
        "connected": is_connected,
        "update_age": (
            round(time.time() - updated, 1)
            if updated else 999
        ),
        "prediction": prediction,
        "forecast": forecast,
        "timeframes": frames
    })


_started = False
_start_lock = threading.Lock()


def start_background():

    global _started

    with _start_lock:

        if _started:
            return

        _started = True

    load_history()

    threading.Thread(
        target=websocket_worker,
        daemon=True,
        name="coinbase-websocket"
    ).start()

    threading.Thread(
        target=rest_worker,
        daemon=True,
        name="coinbase-rest"
    ).start()


start_background()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        threaded=True
)
    
