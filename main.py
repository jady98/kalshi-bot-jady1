import os
import time
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# BITCOIN RADAR PRO + KALSHI BTC 15M
# Solo lectura de mercado. NO envia ordenes a Kalshi.
# ============================================================

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 200
EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14

KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/2.0"})


def get_market_data():
    errors = []

    # Binance.US
    try:
        r = session.get(
            "https://api.binance.us/api/v3/klines",
            params={"symbol": SYMBOL, "interval": INTERVAL, "limit": CANDLE_LIMIT},
            timeout=12,
        )
        r.raise_for_status()
        rows = r.json()
        df = pd.DataFrame(rows, columns=[
            "time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy",
            "taker_quote", "ignore"
        ])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna().reset_index(drop=True), "Binance.US"
    except Exception as exc:
        errors.append("Binance.US: " + str(exc))

    # Coinbase fallback
    try:
        r = session.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            params={"granularity": 60},
            timeout=12,
        )
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list) or not rows:
            raise ValueError("Coinbase no devolvio velas")
        df = pd.DataFrame(rows, columns=[
            "time", "low", "high", "open", "close", "volume"
        ])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_values("time").tail(CANDLE_LIMIT)
        return df.dropna().reset_index(drop=True), "Coinbase"
    except Exception as exc:
        errors.append("Coinbase: " + str(exc))

    raise RuntimeError("No se pudieron obtener datos BTC. " + " | ".join(errors))


def calculate_indicators(df):
    df = df.copy()
    close = df["close"]

    df["EMA9"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["EMA21"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    typical = (df["high"] + df["low"] + close) / 3
    cumulative_volume = df["volume"].cumsum().replace(0, np.nan)
    df["VWAP"] = (typical * df["volume"]).cumsum() / cumulative_volume

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, adjust=False,
                        min_periods=RSI_PERIOD).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, adjust=False,
                        min_periods=RSI_PERIOD).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))
    df.loc[(avg_loss == 0) & (avg_gain > 0), "RSI"] = 100
    df.loc[(avg_gain == 0) & (avg_loss > 0), "RSI"] = 0
    df.loc[(avg_gain == 0) & (avg_loss == 0), "RSI"] = 50

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    df["SUPPORT"] = df["low"].rolling(50, min_periods=1).min()
    df["RESISTANCE"] = df["high"].rolling(50, min_periods=1).max()
    return df


def generate_signal(df):
    last = df.iloc[-1]
    if pd.isna(last["RSI"]) or pd.isna(last["VWAP"]):
        return "ESPERAR"

    bullish = (
        last["EMA9"] > last["EMA21"]
        and last["RSI"] > 50
        and last["MACD"] > last["MACD_SIGNAL"]
        and last["close"] > last["VWAP"]
    )
    bearish = (
        last["EMA9"] < last["EMA21"]
        and last["RSI"] < 50
        and last["MACD"] < last["MACD_SIGNAL"]
        and last["close"] < last["VWAP"]
    )
    if bullish:
        return "SUBE"
    if bearish:
        return "BAJA"
    return "ESPERAR"


def as_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def kalshi_market_data():
    """Busca el contrato abierto de la serie BTC 15m y devuelve cotizaciones."""
    response = session.get(
        f"{KALSHI_API}/markets",
        params={"series_ticker": KALSHI_SERIES, "status": "open", "limit": 100},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    markets = payload.get("markets", []) or []

    now = datetime.now(timezone.utc)
    candidates = []
    for market in markets:
        close_text = market.get("close_time") or market.get("expected_expiration_time")
        close_dt = None
        if close_text:
            try:
                close_dt = datetime.fromisoformat(close_text.replace("Z", "+00:00"))
                if close_dt.tzinfo is None:
                    close_dt = close_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                close_dt = None

        # Solo mercados que no hayan cerrado, priorizando el que cierra primero.
        if close_dt is None or close_dt > now:
            candidates.append((close_dt or datetime.max.replace(tzinfo=timezone.utc), market))

    if not candidates:
        return {
            "available": False,
            "message": "Kalshi no devolvio un contrato BTC 15M abierto en este momento.",
            "series": KALSHI_SERIES,
        }

    candidates.sort(key=lambda item: item[0])
    market = candidates[0][1]

    yes_bid = as_float(market.get("yes_bid_dollars"))
    yes_ask = as_float(market.get("yes_ask_dollars"))
    no_bid = as_float(market.get("no_bid_dollars"))
    no_ask = as_float(market.get("no_ask_dollars"))

    # Si la API no entrega un ask explicito, se estima desde el bid opuesto.
    # Se identifica como estimado en la respuesta.
    yes_ask_estimated = False
    no_ask_estimated = False
    if yes_ask is None and no_bid is not None:
        yes_ask = 1.0 - no_bid
        yes_ask_estimated = True
    if no_ask is None and yes_bid is not None:
        no_ask = 1.0 - yes_bid
        no_ask_estimated = True

    close_text = market.get("close_time") or market.get("expected_expiration_time")
    close_ts = None
    if close_text:
        try:
            close_dt = datetime.fromisoformat(close_text.replace("Z", "+00:00"))
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=timezone.utc)
            close_ts = int(close_dt.timestamp())
        except ValueError:
            pass

    return {
        "available": True,
        "series": KALSHI_SERIES,
        "ticker": market.get("ticker"),
        "title": market.get("title") or market.get("subtitle") or "Bitcoin 15 minutos",
        "status": market.get("status"),
        "close_ts": close_ts,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "last_price": as_float(market.get("last_price_dollars")),
        "yes_ask_estimated": yes_ask_estimated,
        "no_ask_estimated": no_ask_estimated,
        "floor_strike": as_float(market.get("floor_strike")),
        "cap_strike": as_float(market.get("cap_strike")),
        "rules": market.get("rules_primary") or "",
    }


HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="theme-color" content="#080d18">
<title>Bitcoin Radar PRO | Kalshi BTC 15M</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#080d18;--panel:#111a2a;--panel2:#182438;--line:#263750;--muted:#91a4bd;--white:#edf4ff;--green:#00d69a;--red:#ff5268;--yellow:#ffc857;--blue:#36a3ff;--purple:#c58cff}
*{box-sizing:border-box}
body{margin:0;padding:12px;background:var(--bg);color:var(--white);font-family:Arial,sans-serif}
main{max-width:1180px;margin:auto}
header{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:5px 0 14px;flex-wrap:wrap}
h1{font-size:19px;letter-spacing:1.4px;margin:0 0 5px}
.sub{font-size:10px;color:var(--green)}
button{border:1px solid #3b526e;background:#1b2b42;color:white;border-radius:8px;padding:10px 13px;font-weight:bold}
button:active{transform:scale(.98)}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:13px;padding:13px;margin-bottom:12px}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin-bottom:12px}
.metric{background:var(--panel2);padding:12px;border-radius:9px;min-width:0}
.metric small,.muted{font-size:10px;color:var(--muted)}
.metric strong{display:block;font-size:19px;margin-top:7px;overflow-wrap:anywhere}
.section{font-size:11px;color:#aebed2;letter-spacing:1px;margin:2px 0 12px;font-weight:bold}
.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}
.signal-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.signal{padding:13px 5px;text-align:center;border-radius:9px;background:#182438;font-size:12px;font-weight:bold;border:1px solid #344963}
.signal.active-up{background:#103d34;border-color:var(--green);color:var(--green)}
.signal.active-down{background:#3b202b;border-color:var(--red);color:var(--red)}
.signal.active-wait{background:#40351e;border-color:var(--yellow);color:var(--yellow)}
.chart{width:100%;height:360px}
.small-chart{height:150px}
.legend{display:flex;flex-wrap:wrap;gap:12px;font-size:10px;color:#aabbd0;margin-bottom:10px}
.legend span{display:flex;align-items:center;gap:5px}
.dot{width:8px;height:8px;border-radius:50%}
.two{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}
.quote{background:var(--panel2);border-radius:10px;padding:12px}
.quote b{font-size:10px;color:var(--muted)}
.quote strong{display:block;font-size:25px;margin-top:6px}
.formgrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}
label{display:block;font-size:10px;color:var(--muted);margin-bottom:5px}
input,select{width:100%;background:#0b1422;color:var(--white);border:1px solid #344963;border-radius:8px;padding:11px;font-size:14px}
.actionrow{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.actionrow button{flex:1}
.note{font-size:10px;line-height:1.5;color:#8293a9}
table{width:100%;border-collapse:collapse;font-size:12px}
td{padding:8px 3px;border-bottom:1px solid #24344b}
td:last-child{text-align:right;font-weight:bold}
.alertrow{display:flex;gap:9px;align-items:center;padding:8px 0;border-bottom:1px solid #24344b;font-size:12px}
.pill{font-size:10px;border-radius:20px;padding:4px 7px;background:#263750;color:#b9cbe0}
footer{text-align:center;color:#71839b;font-size:10px;padding:12px}
@media(min-width:700px){body{padding:22px}.metrics{grid-template-columns:repeat(4,1fr)}.chart{height:490px}.small-chart{height:170px}.layout{display:grid;grid-template-columns:1.4fr 1fr;gap:12px}.layout .panel{height:fit-content}}
</style>
</head>
<body>
<main>
<header>
 <div><h1>BITCOIN RADAR PRO</h1><div class="sub" id="connection">● CONECTANDO DATOS...</div></div>
 <button onclick="updateAll()">ACTUALIZAR</button>
</header>

<div class="metrics">
 <div class="metric"><small>BITCOIN / USD</small><strong id="btcPrice">--</strong></div>
 <div class="metric"><small>SEÑAL TÉCNICA</small><strong id="mainSignal" class="yellow">ESPERAR</strong></div>
 <div class="metric"><small>RSI 14</small><strong id="rsiValue">--</strong></div>
 <div class="metric"><small>CONTRATO CIERRA EN</small><strong id="timer">--:--</strong></div>
</div>

<div class="layout">
<div>
 <section class="panel">
  <div class="section">RADAR DE DIRECCIÓN</div>
  <div class="signal-grid">
   <div class="signal" id="upSignal">▲ SUBE</div>
   <div class="signal" id="downSignal">▼ BAJA</div>
   <div class="signal active-wait" id="waitSignal">Ⅱ ESPERAR</div>
  </div>
  <p class="note" id="explanation">Esperando confirmación de indicadores.</p>
 </section>

 <section class="panel">
  <div class="section">BTC · VELAS JAPONESAS · 1 MINUTO</div>
  <div class="legend">
   <span><i class="dot" style="background:#ffc857"></i>EMA 9</span>
   <span><i class="dot" style="background:#36a3ff"></i>EMA 21</span>
   <span><i class="dot" style="background:#c58cff"></i>VWAP</span>
  </div>
  <div id="priceChart" class="chart"></div>
 </section>
 <section class="panel"><div class="section">VOLUMEN</div><div id="volumeChart" class="chart small-chart"></div></section>
 <section class="panel"><div class="section">RSI 14</div><div id="rsiChart" class="chart small-chart"></div></section>
 <section class="panel"><div class="section">MACD</div><div id="macdChart" class="chart small-chart"></div></section>
</div>

<div>
 <section class="panel">
  <div class="section">MERCADO KALSHI · BTC 15 MINUTOS</div>
  <div id="marketTitle" style="font-size:13px;font-weight:bold;margin-bottom:8px">Buscando contrato...</div>
  <div class="muted" id="marketTicker">Ticker: --</div>
  <div class="two" style="margin-top:10px">
   <div class="quote"><b>YES · COMPRA</b><strong class="green" id="yesAsk">--¢</strong><span class="muted" id="yesBid">Bid: --</span></div>
   <div class="quote"><b>NO · COMPRA</b><strong class="red" id="noAsk">--¢</strong><span class="muted" id="noBid">Bid: --</span></div>
  </div>
  <p class="note" id="marketMessage">Los precios se consultan directamente en la API pública de Kalshi.</p>
  <div class="muted" id="strikeInfo"></div>
 </section>

 <section class="panel">
  <div class="section">MI OPERACIÓN · SEGUIMIENTO MANUAL</div>
  <div class="formgrid">
   <div><label>LADO COMPRADO</label><select id="side"><option value="yes">YES</option><option value="no">NO</option></select></div>
   <div><label>CONTRATOS</label><input id="quantity" type="number" min="1" step="1" value="1"></div>
   <div><label>PRECIO DE ENTRADA (¢)</label><input id="entry" type="number" min="1" max="99" step="1" placeholder="Ej. 52"></div>
   <div><label>OBJETIVO DE SALIDA (¢)</label><input id="target" type="number" min="1" max="99" step="1" placeholder="Ej. 70"></div>
   <div><label>LÍMITE DE PÉRDIDA (¢)</label><input id="stop" type="number" min="1" max="99" step="1" placeholder="Ej. 40"></div>
   <div><label>ALERTA SONORA</label><select id="sound"><option value="on">Activada</option><option value="off">Desactivada</option></select></div>
  </div>
  <div class="actionrow">
   <button onclick="saveTrade()">GUARDAR OPERACIÓN</button>
   <button onclick="clearTrade()">CERRAR / BORRAR</button>
  </div>
  <table style="margin-top:10px">
   <tr><td>Precio actual de salida estimado</td><td id="currentExit">--</td></tr>
   <tr><td>Ganancia / pérdida estimada</td><td id="pnl">--</td></tr>
   <tr><td>Estado</td><td id="tradeStatus">Sin operación guardada</td></tr>
  </table>
  <p class="note">El cálculo es una estimación antes de comisiones y deslizamiento. Introduce tu entrada real. El panel no compra ni vende por ti.</p>
 </section>

 <section class="panel">
  <div class="section">ALERTAS INTELIGENTES</div>
  <div class="alertrow"><span class="pill">SEÑAL</span> Aviso cuando cambia SUBE / BAJA / ESPERAR</div>
  <div class="alertrow"><span class="pill">OBJETIVO</span> Aviso al alcanzar el precio objetivo</div>
  <div class="alertrow"><span class="pill">STOP</span> Aviso al alcanzar el límite de pérdida</div>
  <div class="alertrow"><span class="pill">SALIDA</span> Aviso si se debilita la señal técnica</div>
  <div class="actionrow"><button onclick="enableSound()">ACTIVAR SONIDO</button></div>
  <p class="note" id="alertStatus">Las alertas iguales no se repiten en cada actualización. Se avisa al cambiar de condición.</p>
 </section>
</div>
</div>
<footer>Datos informativos. Señales técnicas no garantizan resultados. Los contratos YES/NO dependen de las reglas y el precio objetivo del mercado seleccionado.</footer>
</main>

<script>
const $ = id => document.getElementById(id);
const fmtUSD = n => n == null ? "--" : "$" + Number(n).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2});
const cents = n => n == null ? "--¢" : (Number(n)*100).toFixed(1)+"¢";
const priceOptions = (el,h) => ({
 width:el.clientWidth,height:h,
 layout:{background:{color:"#111a2a"},textColor:"#aebdd0",fontSize:10},
 grid:{vertLines:{color:"#1d2b40"},horzLines:{color:"#1d2b40"}},
 rightPriceScale:{borderColor:"#34445a"},
 timeScale:{borderColor:"#34445a",timeVisible:true,secondsVisible:false,
  rightOffset:3,barSpacing:7,minBarSpacing:2},
 crosshair:{mode:LightweightCharts.CrosshairMode.Normal},
 handleScroll:true,handleScale:true
});
const pc=LightweightCharts.createChart($("priceChart"),priceOptions($("priceChart"),360));
const candle=pc.addCandlestickSeries({upColor:"#00d69a",downColor:"#ff5268",borderUpColor:"#00d69a",borderDownColor:"#ff5268",wickUpColor:"#00d69a",wickDownColor:"#ff5268"});
const e9=pc.addLineSeries({color:"#ffc857",lineWidth:2,title:"EMA 9"});
const e21=pc.addLineSeries({color:"#36a3ff",lineWidth:2,title:"EMA 21"});
const vw=pc.addLineSeries({color:"#c58cff",lineWidth:2,title:"VWAP"});
const vc=LightweightCharts.createChart($("volumeChart"),priceOptions($("volumeChart"),150));
const vol=vc.addHistogramSeries({priceFormat:{type:"volume"},priceScaleId:""});
vc.priceScale("").applyOptions({scaleMargins:{top:.1,bottom:0}});
const rc=LightweightCharts.createChart($("rsiChart"),priceOptions($("rsiChart"),150));
const rsi=rc.addLineSeries({color:"#bd8aff",lineWidth:2,title:"RSI"});
[70,50,30].forEach((p,i)=>rsi.createPriceLine({price:p,color:i===0?"#ff5268":i===1?"#71839c":"#00d69a",lineStyle:LightweightCharts.LineStyle.Dashed,title:String(p)}));
rc.priceScale("right").applyOptions({autoScale:false,scaleMargins:{top:.1,bottom:.1}});
const mc=LightweightCharts.createChart($("macdChart"),priceOptions($("macdChart"),150));
const ml=mc.addLineSeries({color:"#36a3ff",lineWidth:2,title:"MACD"});
const ms=mc.addLineSeries({color:"#ffc857",lineWidth:2,title:"Signal"});
const mh=mc.addHistogramSeries({lastValueVisible:false});
ml.createPriceLine({price:0,color:"#71839c",lineStyle:LightweightCharts.LineStyle.Dashed});

let busy=false, initialized=false, btcData=null, marketData=null, lastAlertKey="", alertedConditions={};
let audioContext=null;
function soundAlert(){
 if($("sound").value==="off")return;
 try{
  const Ctx=window.AudioContext||window.webkitAudioContext;
  if(!Ctx)return;
  audioContext=audioContext||new Ctx();
  if(audioContext.state==="suspended")audioContext.resume();
  const osc=audioContext.createOscillator(), gain=audioContext.createGain();
  osc.connect(gain);gain.connect(audioContext.destination);
  osc.frequency.value=880;gain.gain.value=.08;
  osc.start();osc.stop(audioContext.currentTime+.22);
 }catch(e){}
}
function notifyOnce(key,title,message){
 if(alertedConditions[key])return;
 alertedConditions[key]=true;
 $("alertStatus").textContent=title+": "+message;
 soundAlert();
 if("Notification" in window && Notification.permission==="granted"){
  try{new Notification(title,{body:message});}catch(e){}
 }
}
function enableSound(){
 soundAlert();
 if("Notification" in window && Notification.permission==="default"){
  Notification.requestPermission().then(p=>$("alertStatus").textContent="Permiso de notificaciones: "+p);
 }else{$("alertStatus").textContent="Sonido de prueba activado."}
}
function renderSignal(signal){
 $("mainSignal").textContent=signal;
 $("mainSignal").className=signal==="SUBE"?"green":signal==="B
