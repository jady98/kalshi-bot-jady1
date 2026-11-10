import os
import time
import threading
import logging
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("btc-radar")

# ÚNICA FUENTE: Coinbase Exchange REST, producto BTC-USD.
PRODUCT = "BTC-USD"
BASE_URL = "https://api.exchange.coinbase.com"
CANDLES_URL = f"{BASE_URL}/products/{PRODUCT}/candles"
TICKER_URL = f"{BASE_URL}/products/{PRODUCT}/ticker"

lock = threading.RLock()
candles = deque(maxlen=1200)
current_price = 0.0
last_update = 0.0
connection_status = False
last_error = ""


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def minute_timestamp(ts=None):
    ts = time.time() if ts is None else ts
    return int(ts // 60) * 60


def load_historical_candles(session):
    """Carga velas históricas usando la misma fuente Coinbase Exchange."""
    global current_price
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=300)
    params = {
        "granularity": 60,
        "start": start.isoformat(),
        "end": end.isoformat()
    }

    try:
        response = session.get(
            CANDLES_URL,
            params=params,
            timeout=12
        )
        response.raise_for_status()
        rows = response.json()
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

        parsed.sort(key=lambda item: item["time"])

        with lock:
            candles.clear()
            candles.extend(parsed)

            if parsed:
                current_price = parsed[-1]["close"]

        log.info(
            "Coinbase Exchange: %s velas históricas cargadas",
            len(parsed)
        )
        return bool(parsed)

    except Exception as exc:
        log.exception(
            "Error cargando velas históricas de Coinbase Exchange: %s",
            exc
        )
        return False


def update_live_candle(price, size=0.0):
    """Actualiza el precio y la vela del minuto con el ticker REST."""
    global current_price, last_update

    if price <= 0:
        return

    ts = minute_timestamp()

    with lock:
        current_price = price
        last_update = time.time()

        if not candles:
            candles.append({
                "time": ts,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": max(0.0, size)
            })
            return

        last = candles[-1]

        if ts == last["time"]:
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
            last["volume"] += max(0.0, size)

        elif ts > last["time"]:
            next_ts = last["time"] + 60

            while next_ts < ts:
                previous = candles[-1]["close"]

                candles.append({
                    "time": next_ts,
                    "open": previous,
                    "high": previous,
                    "low": previous,
                    "close": previous,
                    "volume": 0.0
                })

                next_ts += 60

            candles.append({
                "time": ts,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": max(0.0, size)
            })


def rest_price_worker():
    """Consulta únicamente Coinbase Exchange cada tres segundos."""
    global connection_status, last_error

    session = requests.Session()
    session.headers.update({
        "User-Agent": "BTC-Radar-PRO/3.0",
        "Accept": "application/json"
    })

    while True:
        try:
            response = session.get(
                TICKER_URL,
                timeout=10
            )
            response.raise_for_status()

            payload = response.json()
            price = safe_float(payload.get("price"))

            if price <= 0:
                raise ValueError(
                    "Coinbase Exchange devolvió un precio inválido"
                )

            update_live_candle(price)

            with lock:
                connection_status = True
                last_error = ""

        except Exception as exc:
            with lock:
                connection_status = False
                last_error = str(exc)

            log.warning(
                "Coinbase Exchange REST no disponible: %s",
                exc
            )

        time.sleep(3)


def ema(values, period):
    if not values:
        return []

    alpha = 2.0 / (period + 1.0)
    result = [values[0]]

    for value in values[1:]:
        result.append(
            value * alpha + result[-1] * (1.0 - alpha)
        )

    return result


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0.0) for x in changes]
    losses = [max(-x, 0.0) for x in changes]

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
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def vwap_series(data):
    total_volume = 0.0
    total_value = 0.0
    result = []

    for candle in data:
        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3.0

        volume = max(0.0, candle["volume"])
        total_volume += volume
        total_value += typical * volume

        result.append(
            total_value / total_volume
            if total_volume
            else candle["close"]
        )

    return result


def macd_values(values):
    if len(values) < 26:
        return 0.0, 0.0, 0.0

    fast = ema(values, 12)
    slow = ema(values, 26)

    line = [
        a - b
        for a, b in zip(fast, slow)
    ]

    signal_line = ema(line, 9)

    return (
        line[-1],
        signal_line[-1],
        line[-1] - signal_line[-1]
    )


def aggregate_candles(data, minutes):
    if minutes == 1:
        return list(data)

    grouped = {}
    span = minutes * 60
        for candle in data:
        bucket = (candle["time"] // span) * span

        if bucket not in grouped:
            grouped[bucket] = {
                "time": bucket,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"]
            }
        else:
            item = grouped[bucket]
            item["high"] = max(item["high"], candle["high"])
            item["low"] = min(item["low"], candle["low"])
            item["close"] = candle["close"]
            item["volume"] += candle["volume"]

    return [grouped[key] for key in sorted(grouped)]


def build_prediction(data):
    if len(data) < 30:
        return {
            "direction": "ESPERANDO DATOS",
            "probability": 50,
            "confidence": "BAJA",
            "change": 0
        }

    closes = [c["close"] for c in data]
    fast = ema(closes, 9)
    slow = ema(closes, 21)
    rsi_value = rsi(closes)
    macd_line, signal_line, histogram = macd_values(closes)

    score = 0

    if fast[-1] > slow[-1]:
        score += 1
    else:
        score -= 1

    if closes[-1] > fast[-1]:
        score += 1
    else:
        score -= 1

    if histogram > 0:
        score += 1
    else:
        score -= 1

    if rsi_value > 55:
        score += 1
    elif rsi_value < 45:
        score -= 1

    if score >= 2:
        direction = "SUBE"
    elif score <= -2:
        direction = "BAJA"
    else:
        direction = "NEUTRAL"

    probability = min(75, 50 + abs(score) * 6)

    return {
        "direction": direction,
        "probability": probability,
        "confidence": (
            "ALTA" if abs(score) >= 3
            else "MEDIA" if abs(score) == 2
            else "BAJA"
        ),
        "rsi": round(rsi_value, 2),
        "ema9": round(fast[-1], 2),
        "ema21": round(slow[-1], 2),
        "macd": round(macd_line, 4),
        "signal": round(signal_line, 4),
        "histogram": round(histogram, 4)
    }


def get_state():
    with lock:
        data = list(candles)
        price = current_price
        updated = last_update
        connected = connection_status
        error = last_error

    age = time.time() - updated if updated else None
    connected = connected and age is not None and age <= 15

    closes = [c["close"] for c in data]
    prediction = build_prediction(data)

    return {
        "source": "Coinbase Exchange REST",
        "symbol": PRODUCT,
        "price": price,
        "connected": connected,
        "status": "CONECTADO" if connected else "DESCONECTADO",
        "last_update": updated,
        "seconds_since_update": round(age, 1) if age is not None else None,
        "error": error,
        "candles": data[-300:],
        "prediction": prediction,
        "ema9": ema(closes, 9)[-1] if closes else None,
        "ema21": ema(closes, 21)[-1] if closes else None,
        "rsi": rsi(closes) if closes else 50
    }


@app.route("/")
def index():
    return render_template_string("""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC Radar PRO</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#08111f;color:#e8f0ff;font-family:Arial,sans-serif;padding:16px}
.wrap{max-width:1000px;margin:auto}
h1{margin:8px 0;color:#58c7ff}
.card{background:#111e30;border:1px solid #263c58;border-radius:14px;padding:16px;margin:12px 0}
.price{font-size:34px;font-weight:bold}
.green{color:#36df9a}
.red{color:#ff657a}
.muted{color:#91a6c2}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.value{font-size:22px;font-weight:bold;margin-top:8px}
canvas{width:100%;height:260px;background:#0a1627;border-radius:10px}
button{background:#1e89c9;color:white;border:0;border-radius:8px;padding:10px}
@media(max-width:550px){.grid{grid-template-columns:1fr}.price{font-size:28px}}
</style>
</head>
<body>
<div class="wrap">
<h1>BTC RADAR PRO</h1>
<div class="card">
<div class="muted">FUENTE: COINBASE EXCHANGE REST · BTC-USD</div>
<div id="status">Conectando...</div>
<div class="price" id="price">--</div>
<div class="muted" id="updated">Esperando precio en vivo</div>
</div>
<div class="card">
<h3>Gráfica BTC · Velas de 1 minuto</h3>
<canvas id="chart" width="1000" height="300"></canvas>
</div>
<div class="grid">
<div class="card">
<div class="muted">PREDICCIÓN 15 MIN</div>
<div class="value" id="direction">ESPERANDO DATOS</div>
<div id="probability">Probabilidad: --</div>
<div class="muted" id="confidence">Confianza: --</div>
</div>
<div class="card">
<div class="muted">INDICADORES</div>
<div>RSI: <b id="rsi">--</b></div>
<div>EMA 9: <b id="ema9">--</b></div>
<div>EMA 21: <b id="ema21">--</b></div>
<div>MACD: <b id="macd">--</b></div>
</div>
</div>
<div class="card">
<div class="muted">Diagnóstico</div>
<div id="error">Comprobando conexión...</div>
</div>
</div>
<script>
const $ = id => document.getElementById(id);
function money(n){
  return Number(n).toLocaleString("en-US",{
    style:"currency",currency:"USD",maximumFractionDigits:2
  });
}
function drawChart(data){
  const canvas=$("chart");
  const ctx=canvas.getContext("2d");
  const w=canvas.width,h=canvas.height;
  ctx.clearRect(0,0,w,h);
  if(!data || data.length<2){
    ctx.fillStyle="#91a6c2";
    ctx.fillText("Esperando velas de Coinbase...",20,30);
    return;
  }
  const min=Math.min(...data.map(x=>x.low));
  const max=Math.max(...data.map(x=>x.high));
  const range=max-min || 1;
  const y=p=>h-20-((p-min)/range)*(h-40);
  ctx.strokeStyle="#263c58";
  ctx.lineWidth=1;
  for(let i=0;i<5;i++){
    const yy=20+i*(h-40)/4;
    ctx.beginPath();ctx.moveTo(0,yy);ctx.lineTo(w,yy);ctx.stroke();
  }
  const step=w/data.length;
  data.forEach((c,i)=>{
    const x=i*step+step/2;
    const up=c.close>=c.open;
    ctx.strokeStyle=up?"#36df9a":"#ff657a";
    ctx.fillStyle=ctx.strokeStyle;
    ctx.beginPath();
    ctx.moveTo(x,y(c.high));ctx.lineTo(x,y(c.low));ctx.stroke();
    const top=y(Math.max(c.open,c.close));
    const bottom=y(Math.min(c.open,c.close));
    ctx.fillRect(x-step*.3,top,Math.max(2,step*.6),Math.max(1,bottom-top));
  });
}
async function refresh(){
  try{
    const response=await fetch("/api/state?t="+Date.now(),{cache:"no-store"});
    if(!response.ok)throw new Error("HTTP "+response.status);
    const s=await response.json();
    $("price").textContent=s.price>0?money(s.price):"--";
    $("status").textContent=s.connected?"● CONECTADO":"● DESCONECTADO";
    $("status").className=s.connected?"green":"red";
    $("updated").textContent=s.connected?
      "Actualizado hace "+s.seconds_since_update+" segundos":
      "No se reciben datos recientes";
    const p=s.prediction||{};
    $("direction").textContent=p.direction||"ESPERANDO DATOS";
    $("probability").textContent="Probabilidad estimada: "+(p.probability??"--")+"%";
    $("confidence").textContent="Confianza: "+(p.confidence||"--");
    $("rsi").textContent=s.rsi==null?"--":Number(s.rsi).toFixed(2);
    $("ema9").textContent=s.ema9==null?"--":money(s.ema9);
    $("ema21").textContent=s.ema21==null?"--":money(s.ema21);
    $("macd").textContent=p.macd==null?"--":p.macd;
    $("error").textContent=s.connected?
      "Fuente activa: "+s.source:
      (s.error||"Coinbase no ha enviado datos recientes");
    drawChart(s.candles||[]);
  }catch(e){
    $("status").textContent="● ERROR DE CONEXIÓN";
    $("status").className="red";
    $("error").textContent=e.message;
  }
}
refresh();
setInterval(refresh,3000);
</script>
</body>
</html>
""")


@app.route("/api/state")
def api_state():
    return jsonify(get_state())


@app.route("/health")
def health():
    with lock:
        connected = connection_status
        price = current_price
    return jsonify({
        "ok": True,
        "connected": connected,
        "price": price,
        "source": "Coinbase Exchange REST"
    })


_worker_started = False
_worker_lock = threading.Lock()


def start_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True

    session = requests.Session()
    session.headers.update({
        "User-Agent": "BTC-Radar-PRO/3.0",
        "Accept": "application/json"
    })
    load_historical_candles(session)

    thread = threading.Thread(
        target=rest_price_worker,
        daemon=True,
        name="coinbase-exchange-rest"
    )
    thread.start()


start_worker()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        debug=False
            )
