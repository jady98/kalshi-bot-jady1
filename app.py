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
# Solo lectura. NO envia ordenes a Kalshi.
# ============================================================

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
CANDLE_LIMIT = 200
KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers.update({"User-Agent": "BitcoinRadarPRO/3.0"})


def get_market_data():
    errors = []

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

        df = pd.DataFrame(rows, columns=[
            "time", "low", "high", "open", "close", "volume"
        ])

        df["time"] = pd.to_datetime(
            df["time"], unit="s", utc=True
        )

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("time").tail(CANDLE_LIMIT)
        return df.dropna().reset_index(drop=True), "Coinbase"

    except Exception as exc:
        errors.append("Coinbase: " + str(exc))

    try:
        r = session.get(
            "https://api.binance.us/api/v3/klines",
            params={
                "symbol": SYMBOL,
                "interval": INTERVAL,
                "limit": CANDLE_LIMIT
            },
            timeout=12
        )
        r.raise_for_status()
        rows = r.json()

        df = pd.DataFrame(rows, columns=[
            "time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy",
            "taker_quote", "ignore"
        ])

        df["time"] = pd.to_datetime(
            df["time"], unit="ms", utc=True
        )

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df.dropna().reset_index(drop=True), "Binance.US"

    except Exception as exc:
        errors.append("Binance.US: " + str(exc))

    raise RuntimeError("No se pudieron obtener datos BTC. " + " | ".join(errors))


def calculate_indicators(df):
    df = df.copy()
    close = df["close"]

    df["EMA9"] = close.ewm(span=9, adjust=False).mean()
    df["EMA21"] = close.ewm(span=21, adjust=False).mean()

    typical = (df["high"] + df["low"] + close) / 3
    cumulative_volume = df["volume"].cumsum().replace(0, np.nan)
    df["VWAP"] = (
        (typical * df["volume"]).cumsum() / cumulative_volume
    )

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / 14, adjust=False, min_periods=14
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / 14, adjust=False, min_periods=14
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))

    df.loc[(avg_loss == 0) & (avg_gain > 0), "RSI"] = 100
    df.loc[(avg_gain == 0) & (avg_loss > 0), "RSI"] = 0
    df.loc[(avg_gain == 0) & (avg_loss == 0), "RSI"] = 50

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()

    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(
        span=9, adjust=False
    ).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    df["SUPPORT"] = df["low"].rolling(
        50, min_periods=1
    ).min()

    df["RESISTANCE"] = df["high"].rolling(
        50, min_periods=1
    ).max()

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
    try:
        response = session.get(
            f"{KALSHI_API}/markets",
            params={
                "series_ticker": KALSHI_SERIES,
                "status": "open",
                "limit": 100
            },
            timeout=12
        )
        response.raise_for_status()

        payload = response.json()
        markets = payload.get("markets", []) or []

        now = datetime.now(timezone.utc)
        candidates = []

        for market in markets:
            close_text = (
                market.get("close_time")
                or market.get("expected_expiration_time")
            )

            close_dt = None

            if close_text:
                try:
                    close_dt = datetime.fromisoformat(
                        close_text.replace("Z", "+00:00")
                    )
                    if close_dt.tzinfo is None:
                        close_dt = close_dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    close_dt = None

            if close_dt is None or close_dt > now:
                candidates.append((
                    close_dt or datetime.max.replace(tzinfo=timezone.utc),
                    market
                ))

        if not candidates:
            return {
                "available": False,
                "message": "No se encontro un contrato BTC 15M abierto.",
                "series": KALSHI_SERIES
            }

        candidates.sort(key=lambda item: item[0])
        market = candidates[0][1]

        yes_bid = as_float(market.get("yes_bid_dollars"))
        yes_ask = as_float(market.get("yes_ask_dollars"))
        no_bid = as_float(market.get("no_bid_dollars"))
        no_ask = as_float(market.get("no_ask_dollars"))

        yes_estimated = False
        no_estimated = False

        if yes_ask is None and no_bid is not None:
            yes_ask = 1.0 - no_bid
            yes_estimated = True

        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid
            no_estimated = True

        close_ts = None
        close_text = (
            market.get("close_time")
            or market.get("expected_expiration_time")
        )

        if close_text:
            try:
                close_dt = datetime.fromisoformat(
                    close_text.replace("Z", "+00:00")
                )
                if close_dt.tzinfo is None:
                    close_dt = close_dt.replace(tzinfo=timezone.utc)
                close_ts = int(close_dt.timestamp())
            except ValueError:
                pass

        return {
            "available": True,
            "series": KALSHI_SERIES,
            "ticker": market.get("ticker"),
            "title": (
                market.get("title")
                or market.get("subtitle")
                or "Bitcoin 15 minutos"
            ),
            "status": market.get("status"),
            "close_ts": close_ts,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "last_price": as_float(
                market.get("last_price_dollars")
            ),
            "yes_ask_estimated": yes_estimated,
            "no_ask_estimated": no_estimated,
            "floor_strike": as_float(market.get("floor_strike")),
            "cap_strike": as_float(market.get("cap_strike")),
            "rules": market.get("rules_primary") or ""
        }
    except Exception as exc:
        return {
            "available": False,
            "message": f"Error conectando a Kalshi: {str(exc)}",
            "series": KALSHI_SERIES
        }


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():
    try:
        df_raw, source = get_market_data()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    df = calculate_indicators(df_raw)
    signal = generate_signal(df)
    last = df.iloc[-1]

    def line(col):
        out = []
        for t, v in zip(df["time"], df[col]):
            if pd.isna(v):
                continue
            out.append({
                "time": int(pd.Timestamp(t).timestamp()),
                "value": round(float(v), 4),
            })
        return out

    candles = []
    volume = []
    for t, o, h, l, c, v in zip(
        df["time"], df["open"], df["high"],
        df["low"], df["close"], df["volume"]
    ):
        ts = int(pd.Timestamp(t).timestamp())
        candles.append({
            "time": ts,
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
        })
        volume.append({
            "time": ts,
            "value": float(v),
            "color": "#00d69a" if c >= o else "#ff5268",
        })

    macd_hist = []
    for t, v in zip(df["time"], df["MACD_HIST"]):
        if pd.isna(v):
            continue
        macd_hist.append({
            "time": int(pd.Timestamp(t).timestamp()),
            "value": round(float(v), 2),
            "color": "#00d69a" if v >= 0 else "#ff5268",
        })

    return jsonify({
        "price": float(last["close"]),
        "signal": signal,
        "rsi": None if pd.isna(last["RSI"]) else float(last["RSI"]),
        "source": source,
        "candles": candles,
        "ema9": line("EMA9"),
        "ema21": line("EMA21"),
        "vwap": line("VWAP"),
        "volume": volume,
        "rsi_series": line("RSI"),
        "macd": line("MACD"),
        "macd_signal": line("MACD_SIGNAL"),
        "macd_hist": macd_hist,
        "support": None if pd.isna(last["SUPPORT"]) else float(last["SUPPORT"]),
        "resistance": None if pd.isna(last["RESISTANCE"]) else float(last["RESISTANCE"]),
        "market": kalshi_market_data(),
    })
    HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#080d18">
<title>Bitcoin Radar PRO</title>

<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>

<style>
:root {
 --bg:#080d18;
 --panel:#111a2a;
 --panel2:#182438;
 --line:#263750;
 --muted:#91a4bd;
 --white:#edf4ff;
 --green:#00d69a;
 --red:#ff5268;
 --yellow:#ffc857;
 --blue:#36a3ff;
 --purple:#c58cff;
}
* {box-sizing:border-box}
body {
 margin:0;
 padding:12px;
 background:var(--bg);
 color:var(--white);
 font-family:Arial,sans-serif;
}
main {max-width:1180px;margin:auto}
header {
 display:flex;
 justify-content:space-between;
 align-items:center;
 gap:10px;
 margin:5px 0 14px;
 flex-wrap:wrap;
}
h1 {font-size:19px;letter-spacing:1.4px;margin:0 0 5px}
.sub {font-size:10px;color:var(--green)}
button {
 border:1px solid #3b526e;
 background:#1b2b42;
 color:white;
 border-radius:8px;
 padding:10px 13px;
 font-weight:bold;
 cursor:pointer;
}
.panel {
 background:var(--panel);
 border:1px solid var(--line);
 border-radius:13px;
 padding:13px;
 margin-bottom:12px;
}
.metrics {
 display:grid;
 grid-template-columns:repeat(2,minmax(0,1fr));
 gap:9px;
 margin-bottom:12px;
}
.metric {
 background:var(--panel2);
 padding:12px;
 border-radius:9px;
 min-width:0;
}
.metric small,.muted {
 font-size:10px;
 color:var(--muted);
}
.metric strong {
 display:block;
 font-size:19px;
 margin-top:7px;
 overflow-wrap:anywhere;
}
.section {
 font-size:11px;
 color:#aebed2;
 letter-spacing:1px;
 margin:2px 0 12px;
 font-weight:bold;
}
.green {color:var(--green)}
.red {color:var(--red)}
.yellow {color:var(--yellow)}
.blue {color:var(--blue)}
.signal-grid {
 display:grid;
 grid-template-columns:repeat(3,1fr);
 gap:8px;
}
.signal {
 padding:13px 5px;
 text-align:center;
 border-radius:9px;
 background:#182438;
 font-size:12px;
 font-weight:bold;
 border:1px solid #344963;
}
.active-up {
 background:#103d34;
 border-color:var(--green);
 color:var(--green);
}
.active-down {
 background:#3b202b;
 border-color:var(--red);
 color:var(--red);
}
.active-wait {
 background:#40351e;
 border-color:var(--yellow);
 color:var(--yellow);
}
.chart {width:100%;height:360px}
.small-chart {height:150px}
.legend {
 display:flex;
 flex-wrap:wrap;
 gap:12px;
 font-size:10px;
 color:#aabbd0;
 margin-bottom:10px;
}
.legend span {display:flex;align-items:center;gap:5px}
.dot {width:8px;height:8px;border-radius:50%}
.two {
 display:grid;
 grid-template-columns:repeat(2,minmax(0,1fr));
 gap:9px;
}
.quote {
 background:var(--panel2);
 border-radius:10px;
 padding:12px;
}
.quote b {font-size:10px;color:var(--muted)}
.quote strong {
 display:block;
 font-size:25px;
 margin-top:6px;
}
.formgrid {
 display:grid;
 grid-template-columns:repeat(2,minmax(0,1fr));
 gap:9px;
}
label {
 display:block;
 font-size:10px;
 color:var(--muted);
 margin-bottom:5px;
}
input,select {
 width:100%;
 background:#0b1422;
 color:var(--white);
 border:1px solid #344963;
 border-radius:8px;
 padding:11px;
 font-size:14px;
}
.actionrow {
 display:flex;
 gap:8px;
 flex-wrap:wrap;
 margin-top:10px;
}
.actionrow button {flex:1}
.note {
 font-size:10px;
 line-height:1.5;
 color:#8293a9;
}
table {
 width:100%;
 border-collapse:collapse;
 font-size:12px;
}
td {
 padding:8px 3px;
 border-bottom:1px solid #24344b;
}
td:last-child {
 text-align:right;
 font-weight:bold;
}
.alertrow {
 display:flex;
 gap:9px;
 align-items:center;
 padding:8px 0;
 border-bottom:1px solid #24344b;
 font-size:12px;
}
.pill {
 font-size:10px;
 border-radius:20px;
 padding:4px 7px;
 background:#263750;
 color:#b9cbe0;
}
footer {
 text-align:center;
 color:#71839b;
 font-size:10px;
 padding:12px;
}
@media(min-width:700px) {
 body {padding:22px}
 .metrics {grid-template-columns:repeat(4,1fr)}
 .chart {height:490px}
 .small-chart {height:170px}
 .layout {
  display:grid;
  grid-template-columns:1.4fr 1fr;
  gap:12px;
 }
 .layout .panel {height:fit-content}
}
</style>
</head>

<body>
<main>

<header>
 <div>
  <h1>BITCOIN RADAR PRO</h1>
  <div class="sub" id="connection">● CONECTANDO DATOS...</div>
 </div>
 <button onclick="updateAll()">ACTUALIZAR</button>
</header>

<div class="metrics">
 <div class="metric">
  <small>BITCOIN / USD</small>
  <strong id="btcPrice">--</strong>
 </div>
 <div class="metric">
  <small>SEÑAL TÉCNICA</small>
  <strong id="mainSignal" class="yellow">ESPERAR</strong>
 </div>
 <div class="metric">
  <small>RSI 14</small>
  <strong id="rsiValue">--</strong>
 </div>
 <div class="metric">
  <small>CONTRATO CIERRA EN</small>
  <strong id="timer">--:--</strong>
 </div>
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
 <p class="note" id="explanation">
  Esperando confirmación de indicadores.
 </p>
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

<section class="panel">
 <div class="section">VOLUMEN</div>
 <div id="volumeChart" class="chart small-chart"></div>
</section>

<section class="panel">
 <div class="section">RSI 14</div>
 <div id="rsiChart" class="chart small-chart"></div>
</section>

<section class="panel">
 <div class="section">MACD</div>
 <div id="macdChart" class="chart small-chart"></div>
</section>

</div>

<div>

<section class="panel">
 <div class="section">MERCADO KALSHI · BTC 15 MINUTOS</div>
 <div id="marketTitle" style="font-size:13px;font-weight:bold;margin-bottom:8px">
  Buscando contrato...
 </div>
 <div class="muted" id="marketTicker">Ticker: --</div>

 <div class="two" style="margin-top:10px">
  <div class="quote">
   <b>YES · COMPRA</b>
   <strong class="green" id="yesAsk">--¢</strong>
   <span class="muted" id="yesBid">Bid: --</span>
  </div>
  <div class="quote">
   <b>NO · COMPRA</b>
   <strong class="red" id="noAsk">--¢</strong>
   <span class="muted" id="noBid">Bid: --</span>
  </div>
 </div>

 <p class="note" id="marketMessage">
  Consultando la API pública de Kalshi.
 </p>
 <div class="muted" id="strikeInfo"></div>
</section>

<section class="panel">
 <div class="section">MI OPERACIÓN · SEGUIMIENTO MANUAL</div>

 <div class="formgrid">
  <div>
   <label>LADO COMPRADO</label>
   <select id="side">
    <option value="yes">YES</option>
    <option value="no">NO</option>
   </select>
  </div>

  <div>
   <label>CONTRATOS</label>
   <input id="quantity" type="number" min="1" step="1" value="1">
  </div>

  <div>
   <label>PRECIO DE ENTRADA (¢)</label>
   <input id="entry" type="number" min="1" max="99" placeholder="Ej. 52">
  </div>

  <div>
   <label>OBJETIVO DE SALIDA (¢)</label>
   <input id="target" type="number" min="1" max="99" placeholder="Ej. 70">
  </div>

  <div>
   <label>LÍMITE DE PÉRDIDA (¢)</label>
   <input id="stop" type="number" min="1" max="99" placeholder="Ej. 40">
  </div>

  <div>
   <label>ALERTA SONORA</label>
   <select id="sound">
    <option value="on">Activada</option>
    <option value="off">Desactivada</option>
   </select>
  </div>
 </div>

 <div class="actionrow">
  <button onclick="saveTrade()">GUARDAR OPERACIÓN</button>
  <button onclick="clearTrade()">CERRAR / BORRAR</button>
 </div>

 <table style="margin-top:10px">
  <tr>
   <td>Precio actual de salida estimado</td>
   <td id="currentExit">--</td>
  </tr>
  <tr>
   <td>Ganancia / pérdida estimada</td>
   <td id="pnl">--</td>
  </tr>
  <tr>
   <td>Estado</td>
   <td id="tradeStatus">Sin operación guardada</td>
  </tr>
 </table>

 <p class="note">
  Estimación antes de comisiones y deslizamiento. El panel no compra
  ni vende por ti.
 </p>
</section>

<section class="panel">
 <div class="section">ALERTAS INTELIGENTES</div>

 <div class="alertrow">
  <span class="pill">SEÑAL</span>
  Aviso cuando cambia SUBE / BAJA / ESPERAR
 </div>
 <div class="alertrow">
  <span class="pill">OBJETIVO</span>
  Aviso al alcanzar el precio objetivo
 </div>
 <div class="alertrow">
  <span class="pill">STOP</span>
  Aviso al alcanzar el límite de pérdida
 </div>
 <div class="alertrow">
  <span class="pill">SALIDA</span>
  Aviso si se debilita la señal técnica
 </div>

 <div class="actionrow">
  <button onclick="enableSound()">ACTIVAR SONIDO</button>
 </div>

 <p class="note" id="alertStatus">
  Las alertas iguales no se repiten en cada actualización.
 </p>
</section>

</div>
</div>

<footer>
 Datos informativos. Las señales técnicas no garantizan resultados.
 Los contratos YES/NO dependen de las reglas del mercado seleccionado.
</footer>

</main>

<script>
const $ = id => document.getElementById(id);

const fmtUSD = v => v != null ? '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}) : '--';
const fmtPct = v => v != null ? (v > 0 ? '+' : '') + Number(v).toFixed(2) + '%' : '--';

let chart, priceSeries, volumeSeries, rsiSeries, macdSeries, macdSignalSeries, macdHistSeries;
let ema9Series, ema21Series, vwapSeries;

function initCharts() {
    const chartOptions = {
        layout: {
            background: { type: 'solid', color: '#111a2a' },
            textColor: '#91a4bd',
        },
        grid: {
            vertLines: { color: '#182438' },
            horzLines: { color: '#182438' },
        },
        rightPriceScale: { borderColor: '#263750' },
        timeScale: { borderColor: '#263750', timeVisible: true, secondsVisible: false },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal }
    };

    const createChartInstance = (containerId) => {
        const container = $(containerId);
        if (!container) return null;
        const c = LightweightCharts.createChart(container, {
            ...chartOptions,
            width: container.clientWidth,
            height: container.clientHeight
        });
        window.addEventListener('resize', () => {
            if (container) c.resize(container.clientWidth, container.clientHeight);
        });
        return c;
    };

    const priceChart = createChartInstance('priceChart');
    if (priceChart) {
        priceSeries = priceChart.addCandlestickSeries({
            upColor: '#00d69a', downColor: '#ff5268',
            borderUpColor: '#00d69a', borderDownColor: '#ff5268',
            wickUpColor: '#00d69a', wickDownColor: '#ff5268'
        });
        ema9Series = priceChart.addLineSeries({ color: '#ffc857', lineWidth: 2 });
        ema21Series = priceChart.addLineSeries({ color: '#36a3ff', lineWidth: 2 });
        vwapSeries = priceChart.addLineSeries({ color: '#c58cff', lineWidth: 2 });
    }

    const volChart = createChartInstance('volumeChart');
    if (volChart) {
        volumeSeries = volChart.addHistogramSeries({
            priceFormat: { type: 'volume' },
            priceScaleId: ''
        });
        volChart.priceScale('').applyOptions({ scaleMargins: { top: 0.1, bottom: 0 } });
    }

    const rsiChart = createChartInstance('rsiChart');
    if (rsiChart) {
        rsiSeries = rsiChart.addLineSeries({ color: '#36a3ff', lineWidth: 2 });
    }

    const macdChart = createChartInstance('macdChart');
    if (macdChart) {
        macdSeries = macdChart.addLineSeries({ color: '#36a3ff', lineWidth: 1.5 });
        macdSignalSeries = macdChart.addLineSeries({ color: '#ffc857', lineWidth: 1.5 });
        macdHistSeries = macdChart.addHistogramSeries();
    }
}

function updateUI(data) {
    $('connection').textContent = `● CONECTADO (${data.source})`;
    $('btcPrice').textContent = fmtUSD(data.price);
    $('rsiValue').textContent = data.rsi != null ? data.rsi.toFixed(1) : '--';

    const signal = data.signal;
    $('mainSignal').textContent = signal;
    $('mainSignal').className = signal === 'SUBE' ? 'green' : signal === 'BAJA' ? 'red' : 'yellow';

    $('upSignal').className = 'signal' + (signal === 'SUBE' ? ' active-up' : '');
    $('downSignal').className = 'signal' + (signal === 'BAJA' ? ' active-down' : '');
    $('waitSignal').className = 'signal' + (signal === 'ESPERAR' ? ' active-wait' : '');

    $('explanation').textContent = signal === 'SUBE' 
        ? 'Tendencia alcista confirmada por EMA9, RSI, MACD y VWAP.'
        : signal === 'BAJA'
        ? 'Tendencia bajista confirmada por los indicadores técnicos.'
        : 'Mercado sin dirección clara o en zona de consolidación.';

    if (priceSeries) priceSeries.setData(data.candles);
    if (volumeSeries) volumeSeries.setData(data.volume);
    if (ema9Series) ema9Series.setData(data.ema9);
    if (ema21Series) ema21Series.setData(data.ema21);
    if (vwapSeries) vwapSeries.setData(data.vwap);
    if (rsiSeries) rsiSeries.setData(data.rsi_series);
    if (macdSeries) macdSeries.setData(data.macd);
    if (macdSignalSeries) macdSignalSeries.setData(data.macd_signal);
    if (macdHistSeries) macdHistSeries.setData(data.macd_hist);

    const m = data.market;
    if (m && m.available) {
        $('marketTitle').textContent = m.title;
        $('marketTicker').textContent = `Ticker: ${m.ticker}`;
        $('yesAsk').textContent = m.yes_ask != null ? Math.round(m.yes_ask * 100) + '¢' : '--¢';
        $('yesBid').textContent = 'Bid: ' + (m.yes_bid != null ? Math.round(m.yes_bid * 100) + '¢' : '--');
        $('noAsk').textContent = m.no_ask != null ? Math.round(m.no_ask * 100) + '¢' : '--¢';
        $('noBid').textContent = 'Bid: ' + (m.no_bid != null ? Math.round(m.no_bid * 100) + '¢' : '--');
        $('marketMessage').textContent = m.rules || 'Contrato activo de 15 minutos en Kalshi.';
        $('strikeInfo').textContent = m.floor_strike ? `Strike base: $${m.floor_strike}` : '';

        if (m.close_ts) {
            startCountdown(m.close_ts);
        }
    } else {
        $('marketTitle').textContent = 'Sin contrato activo';
        $('marketMessage').textContent = m ? m.message : 'No se encontró información de mercado.';
    }

    calculateTrade(data.price);
}

let countdownInterval = null;
function startCountdown(targetTimestamp) {
    if (countdownInterval) clearInterval(countdownInterval);
    
    const updateTimer = () => {
        const now = Math.floor(Date.now() / 1000);
        const diff = targetTimestamp - now;
        if (diff <= 0) {
            $('timer').textContent = '00:00';
            return;
        }
        const m = Math.floor(diff / 60);
        const s = diff % 60;
        $('timer').textContent = `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    };
    updateTimer();
    countdownInterval = setInterval(updateTimer, 1000);
}

function updateAll() {
    fetch('/api/data')
        .then(res => res.json())
        .then(data => {
            if (data.error) {
                $('connection').textContent = '● ERROR: ' + data.error;
                return;
            }
            updateUI(data);
        })
        .catch(err => {
            $('connection').textContent = '● ERROR DE CONEXIÓN';
            console.error(err);
        });
}

function saveTrade() {
    const trade = {
        side: $('side').value,
        quantity: parseFloat($('quantity').value) || 1,
        entry: parseFloat($('entry').value) || 0,
        target: parseFloat($('target').value) || 0,
        stop: parseFloat($('stop').value) || 0
    };
    localStorage.setItem('btc_radar_trade', JSON.stringify(trade));
    alert('Operación guardada para seguimiento.');
    updateAll();
}

function clearTrade() {
    localStorage.removeItem('btc_radar_trade');
    $('entry').value = '';
    $('target').value = '';
    $('stop').value = '';
    $('currentExit').textContent = '--';
    $('pnl').textContent = '--';
    $('tradeStatus').textContent = 'Sin operación guardada';
}

function calculateTrade(currentPrice) {
    const saved = localStorage.getItem('btc_radar_trade');
    if (!saved) return;
    const t = JSON.parse(saved);
    $('side').value = t.side;
    $('quantity').value = t.quantity;
    $('entry').value = t.entry;
    $('target').value = t.target;
    $('stop').value = t.stop;

    const currentValInput = t.entry; 
    $('currentExit').textContent = currentValInput + '¢ (Ref)';
    
    const diff = (t.target - t.entry) * t.quantity;
    $('pnl').textContent = (diff >= 0 ? '+' : '') + diff + '¢ USD aprox';
    $('tradeStatus').textContent = `Activa (${t.side.toUpperCase()} x ${t.quantity})`;
}

function enableSound() {
    alert('Alertas sonoras habilitadas en este navegador.');
    $('alertStatus').textContent = 'Alertas activas y sonando al cumplirse condiciones.';
}

document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    updateAll();
    setInterval(updateAll, 15000);
});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
    
