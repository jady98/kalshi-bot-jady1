import time
from datetime import datetime, timezone
import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 200
KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/4.0"})

def get_market_data():
    r = session.get("https://api.binance.us/api/v3/klines", params={"symbol": SYMBOL, "interval": INTERVAL, "limit": CANDLE_LIMIT}, timeout=12)
    rows = r.json()
    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "trades", "taker_buy", "taker_quote", "ignore"])
    df["time"] = pd.to_numeric(df["time"]) // 1000
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["close"]).reset_index(drop=True), "Binance.US"

def calculate_indicators(df):
    df = df.copy()
    close = df["close"]
    df["EMA9"] = close.ewm(span=9, adjust=False).mean()
    df["EMA21"] = close.ewm(span=21, adjust=False).mean()
    df["VOL_MA"] = df["volume"].rolling(window=20, min_periods=1).mean()
    
    typical = (df["high"] + df["low"] + close) / 3
    df["VWAP"] = ((typical * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, np.nan))

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))
    df["RSI"] = df["RSI"].fillna(50)

    df["MACD"] = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]
    return df

def generate_signal(df):
    last = df.iloc[-1]
    if last["volume"] < (last["VOL_MA"] * 0.75): return "ESPERAR"
    if last["EMA9"] > last["EMA21"] and last["RSI"] > 50 and last["MACD"] > last["MACD_SIGNAL"] and last["close"] > last["VWAP"]: return "SUBE"
    if last["EMA9"] < last["EMA21"] and last["RSI"] < 50 and last["MACD"] < last["MACD_SIGNAL"] and last["close"] < last["VWAP"]: return "BAJA"
    return "ESPERAR"

def kalshi_market_data():
    try:
        res = session.get(f"{KALSHI_API}/markets", params={"series_ticker": KALSHI_SERIES, "status": "open", "limit": 50}, timeout=10)
        markets = res.json().get("markets", []) or []
        if not markets: return {"available": False, "message": "Sin contratos"}
        m = markets[0]
        return {
            "available": True,
            "ticker": m.get("ticker"),
            "title": m.get("title") or "Bitcoin 15 minutos",
            "yes_ask": float(m.get("yes_ask_dollars") or 0.5),
            "yes_bid": float(m.get("yes_bid_dollars") or 0.5),
            "no_ask": float(m.get("no_ask_dollars") or 0.5),
            "no_bid": float(m.get("no_bid_dollars") or 0.5)
        }
    except:
        return {"available": False, "message": "Error API"}

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#080d18;--panel:#111a2a;--panel2:#182438;--line:#263750;--muted:#91a4bd;--white:#edf4ff;--green:#00d69a;--red:#ff5268;--yellow:#ffc857;--blue:#36a3ff;}
*{box-sizing:border-box;}
body{margin:0;padding:10px;background:var(--bg);color:var(--white);font-family:Arial,sans-serif;}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;padding:0 4px;}
h1{font-size:16px;margin:0;letter-spacing:1px;}
.sub{font-size:10px;color:var(--green);}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px;margin-bottom:10px;}
.metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:10px;}
.metric{background:var(--panel2);padding:10px;border-radius:8px;}
.metric small{font-size:9px;color:var(--muted);display:block;}
.metric strong{font-size:15px;margin-top:4px;display:block;}
.chart{width:100%;height:260px;}
.small-chart{width:100%;height:130px;}
.signal-box{text-align:center;padding:10px;border-radius:8px;font-weight:bold;font-size:14px;background:var(--panel2);border:1px solid var(--line);}
.signal-up{background:#103d34;color:var(--green);border-color:var(--green);}
.signal-down{background:#3b202b;color:var(--red);border-color:var(--red);}
.signal-wait{background:#40351e;color:var(--yellow);border-color:var(--yellow);}
.two{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;}
.quote{background:var(--panel2);border-radius:8px;padding:8px;text-align:center;}
.quote strong{font-size:18px;display:block;margin-top:4px;}
</style>
</head>
<body>
<header>
 <div>
  <h1>BITCOIN RADAR PRO</h1>
  <div class="sub" id="connection">● CONECTANDO...</div>
 </div>
</header>

<div class="metrics">
 <div class="metric"><small>BITCOIN / USD</small><strong id="btcPrice">--</strong></div>
 <div class="metric"><small>RSI 14</small><strong id="rsiValue">--</strong></div>
</div>

<div class="panel">
 <small style="color:var(--muted)">SEÑAL TÉCNICA (CON FILTRO DE VOLUMEN)</small>
 <div id="mainSignal" class="signal-box signal-wait" style="margin-top:6px;">ESPERAR</div>
</div>

<div class="panel">
 <div style="font-size:11px;font-weight:bold;margin-bottom:6px;">MERCADO KALSHI (15M)</div>
 <div id="marketTitle" style="font-size:11px;color:var(--muted);margin-bottom:8px;">Buscando contrato...</div>
 <div class="two">
  <div class="quote"><small style="color:var(--green)">YES COMPRA</small><strong id="yesAsk" style="color:var(--green)">--¢</strong></div>
  <div class="quote"><small style="color:var(--red)">NO COMPRA</small><strong id="noAsk" style="color:var(--red)">--¢</strong></div>
 </div>
</div>

<div class="panel">
 <div style="font-size:11px;font-weight:bold;margin-bottom:6px;">GRÁFICO PRECIO · 1M (EMA 9/21 + VWAP)</div>
 <div id="priceChart" class="chart"></div>
</div>

<div class="panel">
 <div style="font-size:11px;font-weight:bold;margin-bottom:6px;">VOLUMEN</div>
 <div id="volumeChart" class="small-chart"></div>
</div>

<div class="panel">
 <div style="font-size:11px;font-weight:bold;margin-bottom:6px;">RSI 14</div>
 <div id="rsiChart" class="small-chart"></div>
</div>

<div class="panel">
 <div style="font-size:11px;font-weight:bold;margin-bottom:6px;">MACD</div>
 <div id="macdChart" class="small-chart"></div>
</div>

<script>
function createOpt(h) {
 return {
  width: document.getElementById('priceChart').clientWidth,
  height: h,
  layout: {background:{color:"#111a2a"}, textColor:"#aebdd0", fontSize:9},
  grid: {vertLines:{color:"#1d2b40"}, horzLines:{color:"#1d2b40"}},
  timeScale:{timeVisible:true, secondsVisible:false}
 };
}

const pc = LightweightCharts.createChart(document.getElementById('priceChart'), createOpt(260));
const candle = pc.addCandlestickSeries({upColor:"#00d69a", downColor:"#ff5268"});
const e9 = pc.addLineSeries({color:"#ffc857", lineWidth:2});
const e21 = pc.addLineSeries({color:"#36a3ff", lineWidth:2});
const vw = pc.addLineSeries({color:"#c58cff", lineWidth:2});

const vc = LightweightCharts.createChart(document.getElementById('volumeChart'), createOpt(130));
const vol = vc.addHistogramSeries();

const rc = LightweightCharts.createChart(document.getElementById('rsiChart'), createOpt(130));
const rsi = rc.addLineSeries({color:"#bd8aff", lineWidth:2});

const mc = LightweightCharts.createChart(document.getElementById('macdChart'), createOpt(130));
const ml = mc.addLineSeries({color:"#36a3ff", lineWidth:2});
const ms = mc.addLineSeries({color:"#ffc857", lineWidth:2});
const mh = mc.addHistogramSeries();

async function updateAll() {
  try {
    const r = await fetch('/api/data');
    const d = await r.json();
    if(d.success) {
      document.getElementById("connection").innerText = "● CONECTADO";
      const last = d.btc[d.btc.length-1];
      document.getElementById("btcPrice").innerText = "$" + last.close.toLocaleString("en-US", {minimumFractionDigits:2});
      document.getElementById("rsiValue").innerText = last.RSI ? last.RSI.toFixed(1) : "--";
      
      const sigEl = document.getElementById("mainSignal");
      sigEl.innerText = d.signal;
      sigEl.className = "signal-box " + (d.signal === "SUBE" ? "signal-up" : d.signal === "BAJA" ? "signal-down" : "signal-wait");

      candle.setData(d.btc);
      e9.setData(d.btc.map(x => ({time: x.time, value: x.EMA9})));
      e21.setData(d.btc.map(x => ({time: x.time, value: x.EMA21})));
      vw.setData(d.btc.map(x => ({time: x.time, value: x.VWAP})));
      vol.setData(d.btc.map(x => ({time: x.time, value: x.volume, color: x.close >= x.open ? "#00d69a" : "#ff5268"})));
      rsi.setData(d.btc.map(x => ({time: x.time, value: x.RSI})));
      ml.setData(d.btc.map(x => ({time: x.time, value: x.MACD})));
      ms.setData(d.btc.map(x => ({time: x.time, value: x.MACD_SIGNAL})));
      mh.setData(d.btc.map(x => ({time: x.time, value: x.MACD_HIST, color: x.MACD_HIST >= 0 ? "#00d69a" : "#ff5268"})));

      if(d.kalshi.available) {
        document.getElementById("marketTitle").innerText = d.kalshi.title;
        document.getElementById("yesAsk").innerText = (d.kalshi.yes_ask * 100).toFixed(1) + "¢";
        document.getElementById("noAsk").innerText = (d.kalshi.no_ask * 100).toFixed(1) + "¢";
      }
    }
  } catch(e) {
    document.getElementById("connection").innerText = "● ERROR DE RED";
  }
}
setInterval(updateAll, 10000);
updateAll();
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/data")
def api_data():
    df, source = get_market_data()
    df = calculate_indicators(df)
    signal = generate_signal(df)
    return jsonify({"success": True, "signal": signal, "btc": df.to_dict(orient="records"), "kalshi": kalshi_market_data()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
    
