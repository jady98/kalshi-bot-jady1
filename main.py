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
session.headers.update({"User-Agent": "BitcoinRadarPRO/4.6"})

def get_market_data():
    errors = []
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
        df = df.dropna(subset=["close"]).drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
        return df, "Binance.US"
    except Exception as exc:
        errors.append("Binance.US: " + str(exc))

    try:
        r = session.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            params={"granularity": 60},
            timeout=12
        )
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list) or not rows:
            raise ValueError("Coinbase no devolvio velas")
        df = pd.DataFrame(rows, columns=["time", "low", "high", "open", "close", "volume"])
        df["time"] = pd.to_numeric(df["time"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_values("time").tail(CANDLE_LIMIT).drop_duplicates(subset=["time"]).reset_index(drop=True)
        return df.dropna(subset=["close"]).reset_index(drop=True), "Coinbase"
    except Exception as exc:
        errors.append("Coinbase: " + str(exc))

    raise RuntimeError("No se pudieron obtener datos BTC. " + " | ".join(errors))

def calculate_indicators(df):
    df = df.copy()
    close = df["close"]

    df["EMA9"] = close.ewm(span=9, adjust=False).mean()
    df["EMA21"] = close.ewm(span=21, adjust=False).mean()
    df["VOL_MA"] = df["volume"].rolling(window=20, min_periods=1).mean()

    typical = (df["high"] + df["low"] + close) / 3
    cumulative_volume = df["volume"].cumsum().replace(0, np.nan)
    df["VWAP"] = ((typical * df["volume"]).cumsum() / cumulative_volume).fillna(close)

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = (100 - (100 / (1 + rs))).fillna(50)

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    # Cálculo del porcentaje de cambio respecto a la vela anterior (1M)
    df["pct_change"] = close.pct_change() * 100

    return df

def generate_signal(df):
    last = df.iloc[-1]
    pct = last.get("pct_change", 0)
    if pd.isna(pct):
        pct = 0.0

    if last["volume"] < (last["VOL_MA"] * 0.75):
        return "ESPERAR", pct
    if last["EMA9"] > last["EMA21"] and last["RSI"] > 50 and last["close"] > last["VWAP"]:
        return "SUBE", pct
    if last["EMA9"] < last["EMA21"] and last["RSI"] < 50 and last["close"] < last["VWAP"]:
        return "BAJA", pct
    return "ESPERAR", pct

def as_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None

def kalshi_market_data():
    try:
        response = session.get(
            f"{KALSHI_API}/markets",
            params={"series_ticker": KALSHI_SERIES, "status": "open", "limit": 100},
            timeout=12
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
            if close_dt is None or close_dt > now:
                candidates.append((close_dt or datetime.max.replace(tzinfo=timezone.utc), market))

        if not candidates:
            return {"available": False, "message": "No se encontro contrato BTC 15M.", "series": KALSHI_SERIES}

        candidates.sort(key=lambda item: item[0])
        market = candidates[0][1]

        yes_bid = as_float(market.get("yes_bid_dollars"))
        yes_ask = as_float(market.get("yes_ask_dollars"))
        no_bid = as_float(market.get("no_bid_dollars"))
        no_ask = as_float(market.get("no_ask_dollars"))

        if yes_ask is None and no_bid is not None:
            yes_ask = 1.0 - no_bid
        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid

        close_ts = None
        close_text = market.get("close_time") or market.get("expected_expiration_time")
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
            "close_ts": close_ts,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "last_price": as_float(market.get("last_price_dollars"))
        }
    except Exception as exc:
        return {"available": False, "message": f"Error Kalshi: {str(exc)}", "series": KALSHI_SERIES}

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<meta name="theme-color" content="#05080f">
<title>Bitcoin Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root {
 --bg:#05080f; --panel:#0e1626; --panel2:#162238; --line:#1f2e4d;
 --muted:#8fa3c7; --white:#f3f7ff; --green:#00e69d; --red:#ff4d6d; --orange:#ffb703;
}
*{box-sizing:border-box}
body{margin:0;padding:8px;background:var(--bg);color:var(--white);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}
main{max-width:1180px;margin:auto}
header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;padding:4px 8px;}
h1{font-size:16px;margin:0;letter-spacing:1.2px;font-weight:700;}
.sub{font-size:10px;color:var(--green);font-weight:600;}
button{border:1px solid var(--line);background:var(--panel2);color:var(--white);border-radius:8px;padding:8px 12px;font-weight:bold;cursor:pointer;}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px;margin-bottom:10px;box-shadow:0 4px 12px rgba(0,0,0,0.3);}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-bottom:10px;}
.metric{background:var(--panel2);padding:10px;border-radius:10px;border:1px solid var(--line);}
.metric small{font-size:10px;color:var(--muted);}
.metric strong{display:block;font-size:17px;margin-top:5px;word-break:break-all;}
.section{font-size:11px;color:var(--muted);letter-spacing:0.5px;margin-bottom:8px;font-weight:700;}
.green{color:var(--green)} .red{color:var(--red)} .orange{color:var(--orange)}
.signal-header{display:flex;justify-content:space-between;align-items:center;background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:12px;gap:10px;}
.signal-badge{font-size:18px;font-weight:800;letter-spacing:1px;padding:6px 14px;border-radius:8px;text-align:center;min-width:110px;}
.signal-up{background:linear-gradient(135deg, #0d3b30, #104f40);color:var(--green);border:1px solid var(--green);}
.signal-down{background:linear-gradient(135deg, #421a22, #57202b);color:var(--red);border:1px solid var(--red);}
.signal-wait{background:linear-gradient(135deg, #423214, #574219);color:var(--orange);border:1px solid var(--orange);}
.signal-details{text-align:right;}
.pct-val{font-size:15px;font-weight:700;display:block;}
.alert-pill{font-size:10px;color:var(--muted);background:#080d18;padding:3px 6px;border-radius:6px;border:1px solid var(--line);display:inline-block;margin-top:4px;}
.chart{width:100%;height:320px;}
.small-chart{width:100%;height:120px;}
.legend{display:flex;flex-wrap:wrap;gap:10px;font-size:10px;color:var(--muted);margin-bottom:6px;}
.legend span{display:flex;align-items:center;gap:4px;}
.dot{width:8px;height:8px;border-radius:50%}
footer{text-align:center;color:var(--muted);font-size:10px;padding:10px;}
@media(min-width:700px){body{padding:16px}.chart{height:420px}}
</style>
</head>
<body>
<main>

<header>
 <div>
  <h1>BITCOIN RADAR PRO</h1>
  <div class="sub" id="connection">● CONECTANDO...</div>
 </div>
 <button onclick="updateAll()">ACTUALIZAR</button>
</header>

<div class="metrics">
 <div class="metric">
  <small>BITCOIN / USD</small>
  <strong id="btcPrice">--</strong>
 </div>
 <div class="metric">
  <small>CONTRATO CIERRA EN</small>
  <strong id="timer">--:--</strong>
 </div>
</div>

<section class="panel">
 <div class="section">SEÑAL TÉCNICA Y PORCENTAJE (1M)</div>
 <div class="signal-header">
  <div id="mainSignal" class="signal-badge signal-wait">ESPERAR</div>
  <div class="signal-details">
   <span class="pct-val" id="pctChange">CAMBIO: 0.00%</span>
   <span class="alert-pill">🔔 ALERTAS ACTIVAS</span>
  </div>
 </div>
</section>

<section class="panel">
 <div class="section">GRÁFICO PRECIO · 1M (EMA 9/21 + VWAP)</div>
 <div class="legend">
  <span><i class="dot" style="background:#ffb703"></i>EMA 9</span>
  <span><i class="dot" style="background:#3a86ff"></i>EMA 21</span>
  <span><i class="dot" style="background:#b5179e"></i>VWAP</span>
 </div>
 <div id="priceChart" class="chart"></div>
</section>

<section class="panel">
 <div class="section">VOLUMEN 1M</div>
 <div id="volumeChart" class="small-chart"></div>
</section>

<section class="panel">
 <div class="section">RSI 14</div>
 <div id="rsiChart" class="small-chart"></div>
</section>

<section class="panel">
 <div class="section">MACD</div>
 <div id="macdChart" class="small-chart"></div>
</section>

<footer>
 Bitcoin Radar PRO • Monitoreo técnico en tiempo real optimizado para dispositivos móviles.
</footer>

</main>

<script>
const $ = id => document.getElementById(id);

function chartOptions(el, height) {
 return {
  width: el.clientWidth,
  height: height,
  layout: {background:{color:"#0e1626"}, textColor:"#8fa3c7", fontSize:10},
  grid: {vertLines:{color:"#17243c"}, horzLines:{color:"#17243c"}},
  timeScale:{timeVisible:true, secondsVisible:false, barSpacing:9, borderColor:"#1f2e4d"},
  priceScale:{borderColor:"#1f2e4d"}
 };
}

const pc = LightweightCharts.createChart($("priceChart"), chartOptions($("priceChart"), 320));
const candle = pc.addCandlestickSeries({upColor:"#00e69d", downColor:"#ff4d6d", wickUpColor:"#00e69d", wickDownColor:"#ff4d6d", borderVisible:false});
const e9 = pc.addLineSeries({color:"#ffb703", lineWidth:2});
const e21 = pc.addLineSeries({color:"#3a86ff", lineWidth:2});
const vw = pc.addLineSeries({color:"#b5179e", lineWidth:1.5});

const vc = LightweightCharts.createChart($("volumeChart"), chartOptions($("volumeChart"), 120));
const vol = vc.addHistogramSeries({base:0});

const rc = LightweightCharts.createChart($("rsiChart"), chartOptions($("rsiChart"), 120));
const rsi = rc.addLineSeries({color:"#bd8aff", lineWidth:2});
[70, 30].forEach(p => {
 rsi.createPriceLine({price: p, color: p === 70 ? "#ff4d6d" : "#00e69d", lineStyle: LightweightCharts.LineStyle.Dashed});
});

const mc = LightweightCharts.createChart($("macdChart"), chartOptions($("macdChart"), 120));
const ml = mc.addLineSeries({color:"#3a86ff", lineWidth:2});
const ms = mc.addLineSeries({color:"#ffb703", lineWidth:2});

let expiryTimestamp = null;

function updateTimer() {
 if(!expiryTimestamp) {
  $("timer").innerText = "--:--";
  return;
 }
 const now = Math.floor(Date.now() / 1000);
 const diff = expiryTimestamp - now;
 if(diff <= 0) {
  $("timer").innerText = "00:00 (Cerrado)";
  return;
 }
 const m = Math.floor(diff / 60);
 const s = diff % 60;
 $("timer").innerText = (m < 10 ? "0" + m : m) + ":" + (s < 10 ? "0" + s : s);
}
setInterval(updateTimer, 1000);

async function updateAll() {
  try {
    const r = await fetch('/api/data');
    const d = await r.json();
    if(d.success) {
      document.getElementById("connection").innerText = "● CONECTADO";
      
      const sigEl = document.getElementById("mainSignal");
      sigEl.innerText = d.signal;
      sigEl.className = "signal-badge " + (d.signal === "SUBE" ? "signal-up" : d.signal === "BAJA" ? "signal-down" : "signal-wait");

      const pctEl = document.getElementById("pctChange");
      const pctVal = d.pct_change || 0;
      pctEl.innerText = "CAMBIO: " + (pctVal >= 0 ? "+" : "") + pctVal.toFixed(2) + "% (1M)";
      pctEl.className = "pct-val " + (pctVal >= 0 ? "green" : "red");

      if(d.btc && d.btc.length > 0) {
        const lastPrice = d.btc[d.btc.length - 1].close;
        $("btcPrice").innerText = "$" + Number(lastPrice).toLocaleString("en-US", {minimumFractionDigits:2});
      }

      if(d.kalshi && d.kalshi.available) {
        expiryTimestamp = d.kalshi.close_ts;
      }

      candle.setData(d.btc);
      e9.setData(d.btc.map(x => ({time: x.time, value: x.EMA9})));
      e21.setData(d.btc.map(x => ({time: x.time, value: x.EMA21})));
      vw.setData(d.btc.map(x => ({time: x.time, value: x.VWAP})));
      vol.setData(d.btc.map(x => ({time: x.time, value: x.volume, color: x.close >= x.open ? "#00e69d" : "#ff4d6d"})));
      rsi.setData(d.btc.map(x => ({time: x.time, value: x.RSI})));
      ml.setData(d.btc.map(x => ({time: x.time, value: x.MACD})));
      ms.setData(d.btc.map(x => ({time: x.time, value: x.MACD_SIGNAL})));
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
    signal, pct = generate_signal(df)
    kalshi = kalshi_market_data()
    return jsonify({
        "success": True,
        "signal": signal,
        "pct_change": pct,
        "btc": df.to_dict(orient="records"),
        "kalshi": kalshi
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
        
