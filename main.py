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
session.headers.update({"User-Agent": "BitcoinRadarPRO/3.2"})

def get_market_data():
    try:
        r = session.get(
            "https://api.binance.us/api/v3/klines",
            params={"symbol": SYMBOL, "interval": INTERVAL, "limit": CANDLE_LIMIT},
            timeout=12
        )
        r.raise_for_status()
        rows = r.json()
        df = pd.DataFrame(rows, columns=[
            "time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy",
            "taker_quote", "ignore"
        ])
        df["time"] = pd.to_numeric(df["time"]) // 1000
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_values("time").drop_duplicates(subset=["time"])
        return df.dropna(subset=["close"]).reset_index(drop=True), "Binance.US"
    except Exception as exc:
        raise RuntimeError("Error obteniendo datos: " + str(exc))

def calculate_indicators(df):
    df = df.copy()
    close = df["close"]
    df["EMA9"] = close.ewm(span=9, adjust=False).mean()
    df["EMA21"] = close.ewm(span=21, adjust=False).mean()
    df["VOL_MA"] = df["volume"].rolling(window=20, min_periods=1).mean()
    
    typical = (df["high"] + df["low"] + close) / 3
    cumulative_volume = df["volume"].cumsum().replace(0, np.nan)
    df["VWAP"] = ((typical * df["volume"]).cumsum() / cumulative_volume)

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
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
    return df
    def generate_signal(df):
    last = df.iloc[-1]
    if pd.isna(last["RSI"]) or pd.isna(last["VWAP"]) or pd.isna(last["VOL_MA"]):
        return "ESPERAR"
    
    if last["volume"] < (last["VOL_MA"] * 0.75):
        return "ESPERAR"

    bullish = (last["EMA9"] > last["EMA21"] and last["RSI"] > 50 and last["MACD"] > last["MACD_SIGNAL"] and last["close"] > last["VWAP"])
    bearish = (last["EMA9"] < last["EMA21"] and last["RSI"] < 50 and last["MACD"] < last["MACD_SIGNAL"] and last["close"] < last["VWAP"])

    if bullish: return "SUBE"
    if bearish: return "BAJA"
    return "ESPERAR"

def as_float(value):
    try:
        return float(value) if value else None
    except:
        return None

def kalshi_market_data():
    try:
        res = session.get(f"{KALSHI_API}/markets", params={"series_ticker": KALSHI_SERIES, "status": "open", "limit": 100}, timeout=12)
        res.raise_for_status()
        markets = res.json().get("markets", []) or []
        now = datetime.now(timezone.utc)
        candidates = []
        for m in markets:
            close_text = m.get("close_time") or m.get("expected_expiration_time")
            close_dt = datetime.fromisoformat(close_text.replace("Z", "+00:00")) if close_text else None
            if close_dt is None or close_dt > now:
                candidates.append((close_dt or datetime.max.replace(tzinfo=timezone.utc), m))
        if not candidates:
            return {"available": False, "message": "Sin contratos abiertos", "series": KALSHI_SERIES}
        candidates.sort(key=lambda x: x[0])
        market = candidates[0][1]
        
        yes_ask = as_float(market.get("yes_ask_dollars"))
        no_bid = as_float(market.get("no_bid_dollars"))
        if yes_ask is None and no_bid is not None: yes_ask = 1.0 - no_bid

        close_ts = int(datetime.fromisoformat(market.get("close_time").replace("Z", "+00:00")).timestamp()) if market.get("close_time") else None

        return {
            "available": True,
            "series": KALSHI_SERIES,
            "ticker": market.get("ticker"),
            "title": market.get("title") or "Bitcoin 15 minutos",
            "close_ts": close_ts,
            "yes_bid": as_float(market.get("yes_bid_dollars")),
            "yes_ask": yes_ask,
            "no_bid": as_float(market.get("no_bid_dollars")),
            "no_ask": as_float(market.get("no_ask_dollars"))
        }
    except Exception as e:
        return {"available": False, "message": str(e), "series": KALSHI_SERIES}

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
body{margin:0;padding:12px;background:#080d18;color:#edf4ff;font-family:Arial,sans-serif}
main{max-width:1180px;margin:auto}
.panel{background:#111a2a;border:1px solid #263750;border-radius:13px;padding:13px;margin-bottom:12px}
.chart{width:100%;height:360px}
</style>
</head>
<body>
<main>
<h1>BITCOIN RADAR PRO</h1>
<div id="connection">● CONECTANDO...</div>
<div id="btcPrice">--</div>
<div class="panel">
 <div id="priceChart" class="chart"></div>
</div>
</main>
<script>
const pc = LightweightCharts.createChart(document.getElementById("priceChart"), { width: document.getElementById("priceChart").clientWidth, height: 360 });
const candle = pc.addCandlestickSeries();

async function updateAll() {
  const res = await fetch("/api/data");
  const data = await res.json();
  if(data.success) {
    document.getElementById("connection").innerText = "● CONECTADO";
    const last = data.btc[data.btc.length-1];
    document.getElementById("btcPrice").innerText = "$" + last.close.toFixed(2);
    candle.setData(data.btc.map(d => ({time: d.time, open: d.open, high: d.high, low: d.low, close: d.close})));
  }
}
setInterval(updateAll, 10000);
window.onload = updateAll;
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
    return jsonify({"success": True, "source": source, "signal": signal, "btc": df.to_dict(orient="records"), "kalshi": kalshi_market_data()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
    
