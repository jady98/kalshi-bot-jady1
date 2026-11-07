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

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/4.4"})

def get_market_data():
    r = session.get("https://api.binance.us/api/v3/klines", params={"symbol": SYMBOL, "interval": INTERVAL, "limit": CANDLE_LIMIT}, timeout=12)
    rows = r.json()
    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "trades", "taker_buy", "taker_quote", "ignore"])
    df["time"] = pd.to_numeric(df["time"]) // 1000
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Limpiar duplicados y huecos de tiempo vacíos para fluidez
    df = df.dropna(subset=["close"]).drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df, "Binance.US"

def calculate_indicators(df):
    df = df.copy()
    close = df["close"]
    df["EMA9"] = close.ewm(span=9, adjust=False).mean()
    df["EMA21"] = close.ewm(span=21, adjust=False).mean()
    df["VOL_MA"] = df["volume"].rolling(window=20, min_periods=1).mean()
    
    typical = (df["high"] + df["low"] + close) / 3
    cum_vol = df["volume"].cumsum().replace(0, np.nan)
    df["VWAP"] = ((typical * df["volume"]).cumsum() / cum_vol).fillna(close)

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = (100 - (100 / (1 + rs))).fillna(50)

    return df

def generate_signal(df):
    last = df.iloc[-1]
    if last["volume"] < (last["VOL_MA"] * 0.75): return "ESPERAR"
    if last["EMA9"] > last["EMA21"] and last["RSI"] > 50 and last["close"] > last["VWAP"]: return "SUBE"
    if last["EMA9"] < last["EMA21"] and last["RSI"] < 50 and last["close"] < last["VWAP"]: return "BAJA"
    return "ESPERAR"

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>Bitcoin Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#080d18;--panel:#111a2a;--panel2:#182438;--line:#263750;--muted:#91a4bd;--white:#edf4ff;--green:#00d69a;--red:#ff5268;--yellow:#ffc857;--orange:#ff8800;}
*{box-sizing:border-box;}
body{margin:0;padding:10px;background:var(--bg);color:var(--white);font-family:Arial,sans-serif;}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}
h1{font-size:16px;margin:0;letter-spacing:1px;}
.sub{font-size:10px;color:var(--green);}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px;margin-bottom:10px;}
.chart-title{font-size:11px;font-weight:bold;margin-bottom:8px;color:var(--white);}
.chart{width:100%;height:320px;}
.small-chart{width:100%;height:120px;}
.signal-box{text-align:center;padding:10px;border-radius:8px;font-weight:bold;font-size:14px;background:var(--panel2);border:1px solid var(--line);}
.signal-up{background:#103d34;color:var(--green);border-color:var(--green);}
.signal-down{background:#3b202b;color:var(--red);border-color:var(--red);}
.signal-wait{background:#40351e;color:var(--orange);border-color:var(--orange);}
</style>
</head>
<body>
<header>
 <div>
  <h1>BITCOIN RADAR PRO</h1>
  <div class="sub" id="connection">● CONECTANDO...</div>
 </div>
</header>

<div class="panel">
 <small style="color:var(--muted)">SEÑAL TÉCNICA</small>
 <div id="mainSignal" class="signal-box signal-wait" style="margin-top:6px;">ESPERAR</div>
</div>

<div class="panel">
 <div class="chart-title">GRÁFICO PRECIO · 1M (EMA 9/21 + VWAP)</div>
 <div id="priceChart" class="chart"></div>
</div>

<div class="panel">
 <div class="chart-title">VOLUMEN 1M</div>
 <div id="volumeChart" class="small-chart"></div>
</div>

<script>
function createOpt(h, elId) {
 return {
  width: document.getElementById(elId).clientWidth,
  height: h,
  layout: {background:{color:"#111a2a"}, textColor:"#aebdd0", fontSize:10},
  grid: {vertLines:{color:"#1d2b40"}, horzLines:{color:"#1d2b40"}},
  timeScale:{timeVisible:true, secondsVisible:false, barSpacing: 8, borderColor:"#263750"},
  priceScale:{borderColor:"#263750"}
 };
}

const pc = LightweightCharts.createChart(document.getElementById('priceChart'), createOpt(320, 'priceChart'));
const candle = pc.addCandlestickSeries({upColor:"#00d69a", downColor:"#ff5268", wickUpColor:"#00d69a", wickDownColor:"#ff5268", borderVisible:false});
const e9 = pc.addLineSeries({color:"#ffc857", lineWidth:2});
const e21 = pc.addLineSeries({color:"#36a3ff", lineWidth:2});
const vw = pc.addLineSeries({color:"#bf40bf", lineWidth:1.5});

const vc = LightweightCharts.createChart(document.getElementById('volumeChart'), createOpt(120, 'volumeChart'));
const vol = vc.addHistogramSeries({base:0});

async function updateAll() {
  try {
    const r = await fetch('/api/data');
    const d = await r.json();
    if(d.success) {
      document.getElementById("connection").innerText = "● CONECTADO";
      
      const sigEl = document.getElementById("mainSignal");
      sigEl.innerText = d.signal;
      sigEl.className = "signal-box " + (d.signal === "SUBE" ? "signal-up" : d.signal === "BAJA" ? "signal-down" : "signal-wait");

      candle.setData(d.btc);
      e9.setData(d.btc.map(x => ({time: x.time, value: x.EMA9})));
      e21.setData(d.btc.map(x => ({time: x.time, value: x.EMA21})));
      vw.setData(d.btc.map(x => ({time: x.time, value: x.VWAP})));
      vol.setData(d.btc.map(x => ({time: x.time, value: x.volume, color: x.close >= x.open ? "#00d69a" : "#ff5268"})));
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
    return jsonify({"success": True, "signal": signal, "btc": df.to_dict(orient="records")})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
    
