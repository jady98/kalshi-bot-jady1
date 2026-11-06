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
session.headers.update({"User-Agent": "BitcoinRadarPRO/3.3"})

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
        return {"available": True, "ticker": m.get("ticker"), "title": m.get("title"), "yes_ask": float(m.get("yes_ask_dollars") or 0.5), "no_bid": float(m.get("no_bid_dollars") or 0.5)}
    except:
        return {"available": False, "message": "Error API"}

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>body{margin:0;padding:12px;background:#080d18;color:#edf4ff;font-family:Arial,sans-serif}</style>
</head>
<body>
<h1>BITCOIN RADAR PRO</h1>
<div id="status">Conectando...</div>
<div id="price">--</div>
<div id="chart" style="width:100%;height:350px;"></div>
<script>
const chart = LightweightCharts.createChart(document.getElementById('chart'), {width: window.innerWidth - 24, height: 350});
const candleSeries = chart.addCandlestickSeries();
async function loadData() {
  const r = await fetch('/api/data');
  const d = await r.json();
  if(d.success) {
    document.getElementById('status').innerText = "Señal: " + d.signal;
    document.getElementById('price').innerText = "$" + d.btc[d.btc.length-1].close.toFixed(2);
    candleSeries.setData(d.btc);
  }
}
setInterval(loadData, 10000);
loadData();
</script>
</body>
</html>"""

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
    
