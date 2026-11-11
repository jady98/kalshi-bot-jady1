
import json
import math
import threading
import time
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# RADAR BTC 15M - SOLO LECTURA
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
COINBASE_CANDLES = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
POLYMARKET = "https://gateway.polymarket.us"
HEADERS = {"User-Agent": "BTC-Radar/1.0", "Accept": "application/json"}

LOCK = threading.RLock()
STATE = {
    "btc": None, "btc_time": None, "coinbase_connected": False,
    "coinbase_status": "Iniciando", "coinbase_error": None,
    "candles": [], "candles_time": None, "candles_error": None,
    "market": None, "polymarket_connected": False,
    "polymarket_status": "Buscando contrato BTC 15m", "polymarket_error": None,
    "bbo_time": None, "last_update": None,
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def as_float(value):
    if isinstance(value, dict):
        value = value.get("value", value.get("amount", value.get("price")))
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def extract_list(payload, keys):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            val = payload.get(key)
            if isinstance(val, list):
                return val
        for val in payload.values():
            if isinstance(val, dict):
                found = extract_list(val, keys)
                if found:
                    return found
    return []


def fetch_candles():
    try:
        response = SESSION.get(
            COINBASE_CANDLES,
            params={"granularity": 60},
            timeout=12
        )
        response.raise_for_status()
        raw = response.json()

        if not isinstance(raw, list):
            raise ValueError("Coinbase no devolvió una lista de velas")

        candles = []
        for row in raw:
            if not isinstance(row, list) or len(row) < 6:
                continue

            # Coinbase: [time, low, high, open, close, volume]
            candles.append({
                "time": int(row[0]),
                "low": float(row[1]),
                "high": float(row[2]),
                "open": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })

        candles.sort(key=lambda c: c["time"])

        if len(candles) < 30:
            raise ValueError(
                f"Coinbase devolvió solo {len(candles)} velas"
            )

        with LOCK:
            STATE["candles"] = candles[-300:]
            STATE["candles_time"] = now_iso()
            STATE["candles_error"] = None
            STATE["last_update"] = now_iso()

    except Exception as exc:
        with LOCK:
            STATE["candles_error"] = (
                f"{type(exc).__name__}: {exc}"
            )


def candles_worker():
    while True:
        fetch_candles()
        time.sleep(60)


def coinbase_worker():

    def on_open(ws):
        sub = {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker", "heartbeat"]
        }
        ws.send(json.dumps(sub))

        with LOCK:
            STATE["coinbase_connected"] = True
            STATE["coinbase_status"] = (
                "WebSocket conectado; suscripción enviada"
            )
            STATE["coinbase_error"] = None

    def on_message(ws, message):
        try:
            data = json.loads(message)
            kind = data.get("type")

            if kind == "ticker" and data.get("product_id") == "BTC-USD":
                price = as_float(data.get("price"))

                if price:
                    with LOCK:
                        STATE["btc"] = price
                        STATE["btc_time"] = now_iso()
                        STATE["coinbase_connected"] = True
                        STATE["coinbase_status"] = (
                            "Recibiendo precio BTC-USD"
                        )
                        STATE["coinbase_error"] = None
                        STATE["last_update"] = now_iso()

            elif kind == "subscriptions":
                with LOCK:
                    STATE["coinbase_status"] = (
                        "Suscripción confirmada por Coinbase"
                    )

            elif kind == "error":
                with LOCK:
                    STATE["coinbase_error"] = str(data)
                    STATE["coinbase_status"] = (
                        "Coinbase reportó un error"
                    )

        except Exception as exc:
            with LOCK:
                STATE["coinbase_error"] = (
                    f"Error leyendo mensaje: {exc}"
                )

    def on_error(ws, error):
        with LOCK:
            STATE["coinbase_connected"] = False
            STATE["coinbase_status"] = "Error de conexión"
            STATE["coinbase_error"] = str(error)

    def on_close(ws, code, msg):
        with LOCK:
            STATE["coinbase_connected"] = False
            STATE["coinbase_status"] = f"Desconectado ({code})"

    while True:
        try:
            ws = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            with LOCK:
                STATE["coinbase_connected"] = False
                STATE["coinbase_error"] = (
                    f"{type(exc).__name__}: {exc}"
                )

        time.sleep(5)


def market_terms(market):
    terms = (
        market.get("assetPriceTerms")
        or market.get("asset_price_terms")
        or {}
    )

    if not isinstance(terms, dict):
        terms = {}

    asset = terms.get("asset") or {}

    if not isinstance(asset, dict):
        asset = {}

    symbol = str(
        asset.get("symbol")
        or terms.get("symbol")
        or ""
    ).lower()

    horizon = str(
        terms.get("horizon")
        or terms.get("duration")
        or ""
    ).lower()

    return symbol, horizon, terms


def is_btc_15m(market):
    symbol, horizon, terms = market_terms(market)
    blob = json.dumps(market, ensure_ascii=False).lower()

    btc = (
        symbol in ("btc", "bitcoin")
        or "bitcoin" in str(market.get("title", "")).lower()
    )

    fifteen = (
        horizon in ("15m", "15min", "15-minute", "15_minutes")
        or "15m" in blob
        or "15 min" in blob
    )

    return btc and fifteen


def find_market():
    response = SESSION.get(
        POLYMARKET + "/v1/markets",
        params={
            "active": "true",
            "closed": "false",
            "limit": 100,
            "offset": 0
        },
        timeout=15
    )

    response.raise_for_status()
    payload = response.json()

    markets = extract_list(
        payload,
        ["markets", "data", "results", "items"]
    )

    matches = [
        m for m in markets
        if isinstance(m, dict) and is_btc_15m(m)
    ]

    if not matches:
        sr = SESSION.get(
            POLYMARKET + "/v1/search",
            params={"query": "bitcoin"},
            timeout=15
        )
        sr.raise_for_status()

        sp = sr.json()
        events = extract_list(
            sp,
            ["events", "data", "results"]
        )

        for event in events:
            if isinstance(event, dict):
                evmarkets = event.get("markets") or []

                if isinstance(evmarkets, list):
                    matches.extend(
                        m for m in evmarkets
                        if isinstance(m, dict) and is_btc_15m(m)
                    )

    if not matches:
        raise ValueError(
            f"No encontré contrato BTC de 15 minutos "
            f"entre {len(markets)} mercados consultados"
        )

    def sort_key(m):
        terms = market_terms(m)[2]
        end = (
            terms.get("windowEnd")
            or terms.get("window_end")
            or ""
        )
        return str(end)

    matches.sort(key=sort_key)
    return matches[0]


def get_market_slug(market):
    return (
        market.get("slug")
        or market.get("marketSlug")
        or market.get("market_slug")
    )


def parse_market_data(payload):
    if isinstance(payload, dict):
        md = (
            payload.get("marketData")
            or payload.get("market_data")
        )

        if isinstance(md, dict):
            return md

        return payload

    return {}


def fetch_bbo(slug):
    response = SESSION.get(
        f"{POLYMARKET}/v1/markets/{slug}/bbo",
        timeout=12
    )

    response.raise_for_status()
    payload = response.json()
    md = parse_market_data(payload)

    bid = as_float(md.get("bestBid"))
    ask = as_float(md.get("bestAsk"))
    current = as_float(md.get("currentPx"))
    last = as_float(md.get("lastTradePx"))

    # NO es el complemento de la cotización YES.
    no_bid = (1 - ask) if ask is not None else None
    no_ask = (1 - bid) if bid is not None else None

    return {
        "yes_bid": bid,
        "yes_ask": ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "current": current,
        "last": last,
        "raw_state": md.get("state")
    }


def polymarket_worker():
    current_slug = None

    while True:
        try:
            if current_slug is None:
                market = find_market()
                current_slug = get_market_slug(market)

                if not current_slug:
                    raise ValueError(
                        "El contrato encontrado no contiene slug"
                    )

                with LOCK:
                    STATE["market"] = {
                        "title": (
                            market.get("title")
                            or market.get("question")
                            or current_slug
                        ),
                        "slug": current_slug,
                        "terms": market_terms(market)[2],
                        "raw": market
                    }

                    STATE["polymarket_status"] = (
                        "Contrato BTC 15m encontrado"
                    )
                    STATE["polymarket_error"] = None

            quote = fetch_bbo(current_slug)

            with LOCK:
                if STATE["market"]:
                    STATE["market"].update(quote)

                STATE["polymarket_connected"] = True
                STATE["polymarket_status"] = (
                    "Cotizaciones BBO recibidas"
                )
                STATE["polymarket_error"] = None
                STATE["bbo_time"] = now_iso()
                STATE["last_update"] = now_iso()

        except requests.HTTPError as exc:
            status = (
                exc.response.status_code
                if exc.response is not None
                else "?"
            )

            with LOCK:
                STATE["polymarket_connected"] = False
                STATE["polymarket_status"] = f"Error HTTP {status}"
                STATE["polymarket_error"] = str(exc)

                if status == 404:
                    current_slug = None

        except Exception as exc:
            with LOCK:
                STATE["polymarket_connected"] = False
                STATE["polymarket_status"] = "Sin cotización"
                STATE["polymarket_error"] = (
                    f"{type(exc).__name__}: {exc}"
                )

                if current_slug is None:
                    STATE["market"] = None

        time.sleep(8)


def ema(values, period):
    if len(values) < period:
        return None

    alpha = 2 / (period + 1)
    result = sum(values[:period]) / period

    for value in values[period:]:
        result = value * alpha + result * (1 - alpha)

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            avg_gain * (period - 1) + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_analysis(snapshot):
    candles = snapshot["candles"]
    btc = snapshot["btc"]
    btc_time = snapshot["btc_time"]

    if not btc or not btc_time or len(candles) < 30:
        return {
            "ready": False,
            "message": "Esperando precio BTC y al menos 30 velas"
        }

    try:
        age = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(btc_time)
        ).total_seconds()
    except Exception:
        age = 9999

    if age > 45:
        return {
            "ready": False,
            "message": (
                f"Precio BTC atrasado ({int(age)} s); señal bloqueada"
            )
        }

    closes = [c["close"] for c in candles]
    closes[-1] = btc

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    rv = rsi(closes, 14)

    total_vol = sum(c["volume"] for c in candles[-60:])

    vwap = (
        sum(
            (
                (c["high"] + c["low"] + c["close"]) / 3
            ) * c["volume"]
            for c in candles[-60:]
        ) / total_vol
    ) if total_vol > 0 else None

    if e9 is None or e21 is None or rv is None or vwap is None:
        return {
            "ready": False,
            "message": "Indicadores todavía incompletos"
        }

    score = 0
    score += 1 if e9 > e21 else -1
    score += 1 if btc > vwap else -1
    score += 1 if rv >= 55 else (-1 if rv <= 45 else 0)
    score += (
        1 if closes[-1] > closes[-2]
        else (-1 if closes[-1] < closes[-2] else 0)
    )

    bull = max(0, min(100, 50 + score * 12.5))
    bear = 100 - bull

    signal = (
        "ALCISTA" if score >= 2
        else ("BAJISTA" if score <= -2 else "NEUTRAL")
    )

    confidence = min(85, 50 + abs(score) * 8)

    return {
        "ready": True,
        "signal": signal,
        "bullish": round(bull, 1),
        "bearish": round(bear, 1),
        "confidence": confidence,
        "rsi": round(rv, 2),
        "ema9": round(e9, 2),
        "ema21": round(e21, 2),
        "vwap": round(vwap, 2),
        "score": score,
        "message": (
            "Indicadores calculados con velas de 1 minuto "
            "y precio en vivo"
        )
    }
    
@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/health")
def health():
    with LOCK:
        return jsonify({
            "ok": True,
            "btc": STATE["btc"],
            "coinbase_connected": STATE["coinbase_connected"],
            "polymarket_connected": STATE["polymarket_connected"],
            "candles": len(STATE["candles"]),
            "coinbase_error": STATE["coinbase_error"],
            "candles_error": STATE["candles_error"],
            "polymarket_error": STATE["polymarket_error"],
            "last_update": STATE["last_update"]
        })


@app.route("/api/data")
def api_data():
    with LOCK:
        snap = {
            k: (v.copy() if isinstance(v, (dict, list)) else v)
            for k, v in STATE.items()
        }

    analysis = calculate_analysis(snap)

    return jsonify({
        "btc": snap["btc"],
        "btc_time": snap["btc_time"],
        "candles": snap["candles"][-120:],
        "analysis": analysis,
        "market": snap["market"],
        "connections": {
            "coinbase": snap["coinbase_connected"],
            "coinbase_status": snap["coinbase_status"],
            "coinbase_error": snap["coinbase_error"],
            "candles_error": snap["candles_error"],
            "polymarket": snap["polymarket_connected"],
            "polymarket_status": snap["polymarket_status"],
            "polymarket_error": snap["polymarket_error"],
            "candles_count": len(snap["candles"]),
            "candles_time": snap["candles_time"],
            "bbo_time": snap["bbo_time"],
            "last_update": snap["last_update"]
        }
    })


HTML = r'''<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC Radar PRO</title>
<style>
*{box-sizing:border-box}
body{
 margin:0;background:#090e18;color:#eaf0fa;
 font-family:Arial,sans-serif;padding:14px
}
.wrap{max-width:1000px;margin:auto}
.top{
 display:flex;justify-content:space-between;
 align-items:center;gap:10px;flex-wrap:wrap
}
h1{font-size:22px;margin:4px 0}
.muted{color:#91a0b8;font-size:12px}
.pill{
 border:1px solid #27344a;border-radius:20px;
 padding:7px 10px;font-size:12px
}
.grid{
 display:grid;grid-template-columns:repeat(2,minmax(0,1fr));
 gap:10px;margin-top:12px
}
.card{
 background:#111a29;border:1px solid #26344b;
 border-radius:13px;padding:14px;min-width:0
}
.label{
 color:#91a0b8;font-size:11px;letter-spacing:.7px
}
.big{font-size:25px;font-weight:700;margin-top:7px}
.green{color:#2bd69a}
.red{color:#ff657a}
.yellow{color:#ffc65c}
.bar{
 height:9px;background:#26344b;border-radius:10px;
 overflow:hidden;margin-top:12px;display:flex
}
.up{background:#22c78b}
.down{background:#f15e76}
.row{
 display:flex;justify-content:space-between;
 gap:10px;margin-top:9px;font-size:13px
}
.chart{width:100%;height:220px;margin-top:8px}
.diag{font-size:12px;line-height:1.6;overflow-wrap:anywhere}
.status{
 display:inline-block;width:8px;height:8px;
 border-radius:50%;background:#69758a;margin-right:6px
}
.on{background:#24d69a}
.off{background:#ff6477}
@media(max-width:600px){
 .grid{grid-template-columns:1fr}
 .big{font-size:23px}
 .chart{height:190px}
}
</style>
</head>

<body>
<div class="wrap">

<div class="top">
 <div>
  <h1>₿ BTC RADAR PRO</h1>
  <div class="muted">
   Coinbase BTC-USD · Polymarket US · Análisis 15 minutos
  </div>
 </div>
 <div class="pill" id="clock">Actualizando…</div>
</div>

<div class="grid">

 <div class="card">
  <div class="label">BITCOIN EN VIVO</div>
  <div class="big" id="btc">Esperando precio…</div>
  <div class="muted" id="btcTime">Coinbase</div>
 </div>

 <div class="card">
  <div class="label">SEÑAL TÉCNICA 15 MIN</div>
  <div class="big" id="signal">SIN DATOS</div>
  <div class="row">
   <span>Confianza técnica</span>
   <b id="confidence">—</b>
  </div>
  <div class="bar">
   <div class="up" id="upbar" style="width:50%"></div>
   <div class="down" id="downbar" style="width:50%"></div>
  </div>
  <div class="row">
   <span class="green" id="bull">Alcista —</span>
   <span class="red" id="bear">Bajista —</span>
  </div>
  <div class="muted" id="message">Esperando datos</div>
 </div>

 <div class="card">
  <div class="label">GRÁFICA BTC/USD · VELAS 1M</div>
  <canvas id="chart" class="chart"></canvas>
 </div>

 <div class="card">
  <div class="label">INDICADORES</div>
  <div class="row"><span>RSI 14</span><b id="rsi">—</b></div>
  <div class="row"><span>EMA 9</span><b id="ema9">—</b></div>
  <div class="row"><span>EMA 21</span><b id="ema21">—</b></div>
  <div class="row"><span>VWAP</span><b id="vwap">—</b></div>
 </div>

 <div class="card">
  <div class="label">POLYMARKET US · BTC 15M</div>
  <div class="muted" id="marketTitle">Buscando contrato…</div>
  <div class="row">
   <span>YES bid / ask</span><b id="yes">—</b>
  </div>
  <div class="row">
   <span>NO bid / ask</span><b id="no">—</b>
  </div>
  <div class="row">
   <span>Último precio</span><b id="last">—</b>
  </div>
  <div class="muted" id="marketState">
   Cotizaciones indicativas
  </div>
 </div>

 <div class="card">
  <div class="label">DIAGNÓSTICO DE CONEXIONES</div>
  <div class="diag" id="diag">Cargando diagnóstico…</div>
 </div>

</div>
</div>

<script>
const $ = id => document.getElementById(id);

const money = x => x == null
 ? '—'
 : '$' + Number(x).toLocaleString('en-US',{
     minimumFractionDigits:2,
     maximumFractionDigits:2
   });

const px = x => x == null
 ? '—'
 : (Number(x)*100).toFixed(1)+'¢';


function draw(candles){
 const cv=$('chart');
 const ctx=cv.getContext('2d');
 const dpr=window.devicePixelRatio||1;
 const w=cv.clientWidth;
 const h=cv.clientHeight;

 cv.width=w*dpr;
 cv.height=h*dpr;
 ctx.scale(dpr,dpr);
 ctx.clearRect(0,0,w,h);

 if(!candles||candles.length<2){
  ctx.fillStyle='#91a0b8';
  ctx.fillText('Esperando velas de Coinbase…',12,25);
  return;
 }

 const arr=candles.slice(-65);
 const vals=arr.map(x=>Number(x.close));
 const lo=Math.min(...vals);
 const hi=Math.max(...vals);
 const pad=(hi-lo)*.12||1;
 const min=lo-pad;
 const max=hi+pad;

 ctx.strokeStyle='#26344b';
 ctx.lineWidth=1;

 for(let i=1;i<4;i++){
  let y=h*i/4;
  ctx.beginPath();
  ctx.moveTo(0,y);
  ctx.lineTo(w,y);
  ctx.stroke();
 }

 const step=w/arr.length;

 arr.forEach((c,i)=>{
  let x=i*step+step/2;
  let yo=h-(c.open-min)/(max-min)*h;
  let yc=h-(c.close-min)/(max-min)*h;

  ctx.strokeStyle=c.close>=c.open?'#28d69a':'#ff6477';
  ctx.fillStyle=ctx.strokeStyle;

  ctx.beginPath();
  ctx.moveTo(x,h-(c.low-min)/(max-min)*h);
  ctx.lineTo(x,h-(c.high-min)/(max-min)*h);
  ctx.stroke();

  ctx.fillRect(
   x-step*.28,
   Math.min(yo,yc),
   Math.max(2,step*.56),
   Math.max(1,Math.abs(yc-yo))
  );
 });
}


async function refresh(){
 try{
  const r=await fetch('/api/data',{cache:'no-store'});
  const d=await r.json();

  $('btc').textContent=money(d.btc);

  $('btcTime').textContent=d.btc_time
   ? 'Actualizado '+new Date(d.btc_time).toLocaleTimeString()
   : 'Sin precio en vivo';

  const a=d.analysis||{};

  $('signal').textContent=a.ready?a.signal:'SIN DATOS';

  $('signal').className='big '+(
   a.signal==='ALCISTA'?'green':
   a.signal==='BAJISTA'?'red':'yellow'
  );

  $('confidence').textContent=a.ready?a.confidence+'%':'—';

  $('bull').textContent='Alcista '+(
   a.ready?a.bullish+'%':'—'
  );

  $('bear').textContent='Bajista '+(
   a.ready?a.bearish+'%':'—'
  );

  $('upbar').style.width=(a.ready?a.bullish:50)+'%';
  $('downbar').style.width=(a.ready?a.bearish:50)+'%';

  $('message').textContent=a.message||'Sin diagnóstico';
  $('rsi').textContent=a.rsi??'—';
  $('ema9').textContent=a.ema9?money(a.ema9):'—';
  $('ema21').textContent=a.ema21?money(a.ema21):'—';
  $('vwap').textContent=a.vwap?money(a.vwap):'—';

  const m=d.market||{};

  $('marketTitle').textContent=m.title||'Contrato no encontrado';
  $('yes').textContent=px(m.yes_bid)+' / '+px(m.yes_ask);
  $('no').textContent=px(m.no_bid)+' / '+px(m.no_ask);
  $('last').textContent=px(m.last??m.current);

  $('marketState').textContent='BBO: '+(
   d.connections.bbo_time
    ? new Date(d.connections.bbo_time).toLocaleTimeString()
    : 'sin actualización'
  );

  const c=d.connections;
  const dot=v=>'<span class="status '+(v?'on':'off')+'"></span>';

  $('diag').innerHTML=
   dot(c.coinbase)+'Coinbase: '+c.coinbase_status+'<br>'+
   dot(c.polymarket)+'Polymarket: '+c.polymarket_status+'<br>'+
   'Velas recibidas: '+c.candles_count+'<br>'+
   'Error Coinbase: '+(c.coinbase_error||'ninguno')+'<br>'+
   'Error velas: '+(c.candles_error||'ninguno')+'<br>'+
   'Error Polymarket: '+(c.polymarket_error||'ninguno');

  $('clock').textContent='Servidor '+new Date().toLocaleTimeString();

  draw(d.candles);

 }catch(e){
  $('diag').textContent='No se pudo consultar /api/data: '+e;
 }
}

refresh();
setInterval(refresh,2500);
</script>
</body>
</html>'''


def start_workers():
    if getattr(app, "_workers_started", False):
        return

    app._workers_started = True

    workers = [
        (coinbase_worker, "coinbase-ws"),
        (candles_worker, "coinbase-candles"),
        (polymarket_worker, "polymarket-bbo")
    ]

    for target, name in workers:
        threading.Thread(
            target=target,
            name=name,
            daemon=True
        ).start()


start_workers()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
    
