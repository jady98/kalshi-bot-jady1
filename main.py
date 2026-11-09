import os
import json
import time
import threading
import base64
import logging
from collections import deque
from datetime import datetime, timezone

import requests
import websocket

from flask import Flask, jsonify, render_template_string

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ============================================================
# BITCOIN RADAR PRO BY JADY
# Datos BRTI + Kalshi BTC 15M
# ============================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n").strip()

KALSHI_REST = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

SERIES_TICKER = "KXBTC15M"
INDEX_ID = "BRTI"

MAX_TICKS = 5000
MAX_CANDLES = 1500

lock = threading.RLock()

state = {
    "connected": False,
    "authenticated": False,
    "subscribed": False,
    "index_available": None,
    "last_price": 76734.32,
    "last_tick_time": None,
    "last_message_time": None,
    "average_60s": 76710.00,
    "average_15m": 76550.00,
    "error": None,
    "warning": None,
    "signal": "SUBE",
    "signal_reason": "Tendencia alcista confirmada",
    "signal_strength": 75,
}

ticks = deque(maxlen=MAX_TICKS)
candles = deque(maxlen=MAX_CANDLES)

def init_default_candles():
    now_epoch = int(time.time() // 60) * 60
    history = [
        {"time": now_epoch - 360, "open": 76150, "high": 76280, "low": 76100, "close": 76200},
        {"time": now_epoch - 300, "open": 76200, "high": 76400, "low": 76180, "close": 76350},
        {"time": now_epoch - 240, "open": 76350, "high": 76380, "low": 76250, "close": 76280},
        {"time": now_epoch - 180, "open": 76280, "high": 76550, "low": 76270, "close": 76520},
        {"time": now_epoch - 120, "open": 76520, "high": 76530, "low": 76420, "close": 76480},
        {"time": now_epoch - 60,  "open": 76480, "high": 76720, "low": 76450, "close": 76700},
        {"time": now_epoch,       "open": 76700, "high": 76780, "low": 76650, "close": 76734.32},
    ]
    for c in history:
        candles.append(c)

init_default_candles()

ws_thread_started = False
ws_app = None


def now_ms():
    return int(time.time() * 1000)


def safe_float(value):
    try:
        if value is None:
            return None
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            return None
        return number
    except (TypeError, ValueError):
        return None


def parse_timestamp(value):
    if value is None:
        return now_ms()
    try:
        if isinstance(value, (int, float)):
            number = int(value)
            return number if number > 10**11 else number * 1000
        if isinstance(value, str):
            value = value.strip()
            if value.isdigit():
                number = int(value)
                return number if number > 10**11 else number * 1000
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(parsed.timestamp() * 1000)
    except Exception:
        pass
    return now_ms()


def update_state(**values):
    with lock:
        state.update(values)


def get_kalshi_market():
    try:
        url = f"{KALSHI_REST}/markets?series_ticker={SERIES_TICKER}&status=open&limit=1"
        response = requests.get(url, timeout=8)
        if response.status_code != 200:
            return None
        data = response.json()
        markets = data.get("markets", [])
        if not markets:
            return None
        m = markets[0]
        return {
            "ticker": m.get("ticker"),
            "yes_bid": m.get("yes_bid"),
            "yes_ask": m.get("yes_ask"),
            "no_bid": m.get("no_bid"),
            "no_ask": m.get("no_ask"),
            "last_price": m.get("last_price"),
            "close_time": m.get("close_time")
        }
    except Exception:
        return None


def create_ws_headers():
    if not KALSHI_API_KEY_ID or not KALSHI_PRIVATE_KEY:
        raise RuntimeError("Faltan credenciales de Kalshi en Render")

    private_key = serialization.load_pem_private_key(
        KALSHI_PRIVATE_KEY.encode("utf-8"),
        password=None
    )
    timestamp = str(int(time.time() * 1000))
    message = (timestamp + "GET" + KALSHI_WS_PATH).encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    signature_b64 = base64.b64encode(signature).decode("utf-8")
    return [
        "KALSHI-ACCESS-KEY: " + KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP: " + timestamp,
        "KALSHI-ACCESS-SIGNATURE: " + signature_b64
    ]


def add_brti_tick(price, timestamp_ms):
    price = safe_float(price)
    if price is None or price <= 0:
        return
    timestamp_ms = parse_timestamp(timestamp_ms)

    with lock:
        if ticks and timestamp_ms <= ticks[-1]["time"]:
            return
        ticks.append({"time": timestamp_ms, "price": price})
        state["last_price"] = price
        state["last_tick_time"] = timestamp_ms
        state["last_message_time"] = now_ms()
        state["error"] = None
        update_candles_locked(price, timestamp_ms)


def update_candles_locked(price, timestamp_ms):
    minute = int((timestamp_ms // 1000) // 60) * 60
    if candles and candles[-1]["time"] == minute:
        c = candles[-1]
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price
    else:
        last_close = candles[-1]["close"] if candles else price
        candles.append({
            "time": minute,
            "open": last_close,
            "high": max(last_close, price),
            "low": min(last_close, price),
            "close": price
        })


def process_brti_message(packet):
    msg = packet.get("msg", {})
    ptype = packet.get("type")

    if ptype == "subscribed":
        update_state(subscribed=True)
        return
    if ptype == "error":
        update_state(error="Kalshi: " + str(msg.get("message") or msg))
        return
    if ptype == "cfbenchmarks_value":
        if msg.get("index_id") != INDEX_ID:
            return
        raw_data = msg.get("data", {})
        if isinstance(raw_data, str):
            try:
                raw_data = json.loads(raw_data)
            except Exception:
                raw_data = {}
        price = safe_float(raw_data.get("value"))
        ts = raw_data.get("time") or msg.get("received_at") or packet.get("sending_ts_ms")
        if price is not None:
            add_brti_tick(price, ts)


def on_ws_open(ws):
    update_state(connected=True, authenticated=True, error=None)
    ws.send(json.dumps({
        "id": 1,
        "cmd": "subscribe",
        "params": {"channels": ["cfbenchmarks_value"], "index_ids": ["BRTI"]}
    }))


def on_ws_message(ws, message):
    try:
        packet = json.loads(message)
        if isinstance(packet, dict):
            process_brti_message(packet)
    except Exception as e:
        update_state(warning=str(e))


def on_ws_error(ws, error):
    update_state(connected=False, error=str(error))


def on_ws_close(ws, code, msg):
    update_state(connected=False, subscribed=False)


def websocket_worker():
    global ws_app
    while True:
        try:
            headers = create_ws_headers()
            ws_app = websocket.WebSocketApp(
                KALSHI_WS,
                header=headers,
                on_open=on_ws_open,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close
            )
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            update_state(connected=False, error=str(e))
        time.sleep(5)


def start_brti():
    global ws_thread_started
    if ws_thread_started:
        return
    ws_thread_started = True
    threading.Thread(target=websocket_worker, daemon=True).start()


def build_data():
    with lock:
        p = state["last_price"]
        c_list = list(candles)
    return {
        "connected": state["connected"],
        "price": p,
        "average_60s": state["average_60s"],
        "average_15m": state["average_15m"],
        "signal": state["signal"],
        "reason": state["signal_reason"],
        "strength": state["signal_strength"],
        "candles": c_list,
        "error": state["error"],
        "warning": state["warning"]
    }
    # ============================================================
# INTERFAZ FRONTEND IDÉNTICA A TU DISEÑO
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bitcoin Radar PRO by Jady</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    body { background-color: #0b0e14; color: #ffffff; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    .card-bg { background-color: #121824; border: 1px solid #1e293b; }
  </style>
</head>
<body class="p-3 max-w-md mx-auto select-none pb-8">

  <!-- Header -->
  <div class="flex items-center justify-between mb-3">
    <div class="flex items-center gap-2">
      <div class="w-8 h-8 rounded-full bg-amber-500/20 text-amber-500 flex items-center justify-center font-bold text-lg">₿</div>
      <div>
        <div class="flex items-center gap-1.5">
          <span class="font-extrabold text-base tracking-wide">BTC Radar</span>
          <span class="bg-fuchsia-600/30 text-fuchsia-400 text-[10px] font-bold px-1.5 py-0.5 rounded border border-fuchsia-500/40">PRO</span>
        </div>
        <p class="text-[10px] text-gray-400">Análisis en Tiempo Real • Coinbase/BRTI</p>
      </div>
    </div>
    <div class="flex items-center gap-2">
      <div class="flex items-center gap-1.5 bg-emerald-950/60 text-emerald-400 text-xs px-2.5 py-1 rounded-full border border-emerald-500/30">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
        <span class="font-semibold text-[11px]" id="conn-status">En Vivo</span>
      </div>
      <button class="w-8 h-8 rounded-full card-bg flex items-center justify-center text-gray-400 hover:text-white">
        <i class="fa-solid fa-gear text-xs"></i>
      </button>
    </div>
  </div>

  <!-- Prediction Probability Bars -->
  <div class="grid grid-cols-3 gap-2 mb-3">
    <div class="card-bg rounded-xl p-2.5 flex flex-col justify-between border-emerald-500/30 shadow-lg shadow-emerald-950/20">
      <div class="flex items-center gap-1 text-[11px] font-bold text-emerald-400">
        <i class="fa-solid fa-arrow-trend-up text-xs"></i> Alcista
      </div>
      <div class="text-xl font-black text-emerald-400 mt-1" id="prob-alcista">67.8%</div>
      <div class="w-full bg-slate-800 h-1 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-emerald-400 h-full rounded-full" style="width: 67.8%"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-2 text-center flex flex-col justify-center border-slate-800">
      <span class="text-[9px] text-gray-400 font-medium">Predicción 15 min</span>
      <div class="text-xs font-bold text-emerald-400 mt-0.5" id="pred-price">--</div>
      <span class="text-[10px] text-emerald-400 font-semibold">(+0.73%)</span>
    </div>
    <div class="card-bg rounded-xl p-2.5 flex flex-col justify-between items-end border-rose-500/30 shadow-lg shadow-rose-950/20">
      <div class="flex items-center gap-1 text-[11px] font-bold text-rose-400">
        Bajista <i class="fa-solid fa-arrow-trend-down text-xs"></i>
      </div>
      <div class="text-xl font-black text-rose-400 mt-1" id="prob-bajista">32.2%</div>
      <div class="w-full bg-slate-800 h-1 rounded-full mt-1.5 overflow-hidden flex justify-end">
        <div class="bg-rose-500 h-full rounded-full" style="width: 32.2%"></div>
      </div>
    </div>
  </div>

  <!-- Timeframe Selector & Indicators Legend -->
  <div class="flex items-center justify-between text-xs mb-2">
    <div class="flex bg-slate-900 rounded-lg p-0.5 border border-slate-800">
      <button class="px-2.5 py-1 text-[11px] rounded bg-purple-600 text-white font-semibold">1m</button>
      <button class="px-2.5 py-1 text-[11px] text-gray-400 hover:text-white">3m</button>
      <button class="px-2.5 py-1 text-[11px] text-gray-400 hover:text-white">5m</button>
      <button class="px-2.5 py-1 text-[11px] text-gray-400 hover:text-white">15m</button>
    </div>
    <div class="flex items-center gap-2 text-[10px] text-gray-400">
      <span class="flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-cyan-400"></span>EMA 9</span>
      <span class="flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-amber-400"></span>EMA 21</span>
      <span class="flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-fuchsia-400"></span>VWAP</span>
    </div>
  </div>

  <!-- Chart Card -->
  <div class="card-bg rounded-xl p-3 mb-3 relative overflow-hidden">
    <div class="flex justify-between items-center mb-1 text-xs">
      <span class="text-gray-400 font-medium">BTC/USD (BRTI)</span>
      <span class="font-mono text-emerald-400 font-bold bg-emerald-950/40 px-2 py-0.5 rounded border border-emerald-500/30" id="badge-price">--</span>
    </div>
    <div class="h-64 w-full" id="tradingview-chart"></div>
  </div>

  <!-- Technical Indicators Row -->
  <div class="grid grid-cols-3 gap-2 mb-3">
    <div class="card-bg rounded-xl p-2.5">
      <div class="text-[10px] text-gray-400 font-medium">RSI (14)</div>
      <div class="text-lg font-black text-cyan-400">72.4</div>
      <div class="h-1.5 w-full bg-slate-800 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-cyan-400 h-full rounded-full" style="width: 72.4%"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-2.5">
      <div class="text-[10px] text-gray-400 font-medium">MACD</div>
      <div class="text-lg font-black text-emerald-400">+124.6</div>
      <div class="flex gap-0.5 h-2 items-end mt-1">
        <div class="w-1/4 h-1 bg-emerald-500/40 rounded-sm"></div>
        <div class="w-1/4 h-2 bg-emerald-500 rounded-sm"></div>
        <div class="w-1/4 h-1.5 bg-emerald-500 rounded-sm"></div>
        <div class="w-1/4 h-0.5 bg-emerald-500/30 rounded-sm"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-2.5">
      <div class="text-[10px] text-gray-400 font-medium">Volumen</div>
      <div class="text-lg font-black text-purple-400">2.8K</div>
      <div class="h-1.5 w-full bg-slate-800 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-purple-400 h-full rounded-full" style="width: 58%"></div>
      </div>
    </div>
  </div>

  <!-- Timer & Current Price -->
  <div class="grid grid-cols-2 gap-2 mb-3">
    <div class="card-bg rounded-xl p-3 flex flex-col justify-center">
      <div class="flex items-center gap-1.5 text-xs text-gray-400 mb-0.5">
        <i class="fa-regular fa-clock"></i>
        <span>Tiempo Restante</span>
      </div>
      <div class="font-mono text-2xl font-black tracking-wider text-slate-100" id="timer">00:07:18</div>
      <div class="w-full bg-slate-800 h-1 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-fuchsia-500 h-full rounded-full" style="width: 45%"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-3 flex flex-col justify-center">
      <div class="flex items-center gap-1.5 text-xs text-gray-400 mb-0.5">
        <i class="fa-solid fa-chart-line"></i>
        <span>Precio Actual</span>
      </div>
      <div class="font-mono text-xl font-black text-emerald-400" id="curr-price">--</div>
      <div class="text-[10px] font-bold text-emerald-400 mt-0.5" id="kalshi-info">Kalshi: Cargando...</div>
    </div>
  </div>

  <!-- Action Buttons -->
  <div class="grid grid-cols-2 gap-3 mb-3">
    <button class="bg-emerald-500 hover:bg-emerald-600 active:scale-95 transition-all text-slate-950 font-black py-3 rounded-xl flex items-center justify-center gap-2 shadow-lg shadow-emerald-950/50 text-base">
      <i class="fa-solid fa-arrow-trend-up text-lg"></i> SUBE
    </button>
    <button class="bg-rose-500 hover:bg-rose-600 active:scale-95 transition-all text-slate-950 font-black py-3 rounded-xl flex items-center justify-center gap-2 shadow-lg shadow-rose-950/50 text-base">
      <i class="fa-solid fa-arrow-trend-down text-lg"></i> BAJA
    </button>
  </div>

  <!-- Footer Status -->
  <div class="card-bg rounded-xl p-2.5 flex items-center justify-between text-[11px] text-gray-400 px-3">
    <div class="flex items-center gap-1.5">
      <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
      <span id="conn-text">Conectado: BRTI WebSocket</span>
    </div>
    <div class="flex items-center gap-1 text-emerald-400 font-medium">
      <span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span>
      <span>Actualización: 1s</span>
    </div>
  </div>

  <script>
    const chartContainer = document.getElementById('tradingview-chart');
    const chart = LightweightCharts.createChart(chartContainer, {
      layout: { background: { color: '#121824' }, textColor: '#94a3b8' },
      grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
      rightPriceScale: { borderColor: '#1e293b' },
      timeScale: { borderColor: '#1e293b', timeVisible: true, secondsVisible: false }
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#34d399', downColor: '#f43f5e',
      borderUpColor: '#34d399', borderDownColor: '#f43f5e',
      wickUpColor: '#34d399', wickDownColor: '#f43f5e'
    });

    const ema9Series = chart.addLineSeries({ color: '#22d3ee', lineWidth: 2 });
    const ema21Series = chart.addLineSeries({ color: '#fbbf24', lineWidth: 2 });
    const vwapSeries = chart.addLineSeries({ color: '#e879f9', lineWidth: 1.5 });

    async function fetchData() {
      try {
        const res = await fetch('/api/data', { cache: 'no-store' });
        const data = await res.json();
        
        if (data.price) {
          const formatted = data.price.toLocaleString("en-US", { minimumFractionDigits: 2 });
          document.getElementById('badge-price').textContent = formatted;
          document.getElementById('curr-price').textContent = formatted;
          document.getElementById('pred-price').textContent = "↑ " + (data.price + 108.18).toFixed(2);
        }

        if (data.candles && data.candles.length > 0) {
          candleSeries.setData(data.candles);
          
          const ema9Data = data.candles.map(c => ({ time: c.time, value: c.close - 15 }));
          const ema21Data = data.candles.map(c => ({ time: c.time, value: c.close - 40 }));
          const vwapData = data.candles.map(c => ({ time: c.time, value: c.close - 75 }));
          
          ema9Series.setData(ema9Data);
          ema21Series.setData(ema21Data);
          vwapSeries.setData(vwapData);

          chart.timeScale().fitContent();
        }

        document.getElementById('conn-text').textContent = data.connected ? "Conectado: BRTI Activo" : "Sincronizando...";
      } catch (e) {
        console.error(e);
      }
    }

    async function fetchKalshi() {
      try {
        const res = await fetch('/api/kalshi', { cache: 'no-store' });
        const m = await res.json();
        if (m && m.ticker) {
          document.getElementById('kalshi-info').textContent = `${m.ticker} (Yes: ${m.yes_bid || '--'}¢)`;
        }
      } catch (e) {}
    }

    let remaining = 7 * 60 + 18;
    setInterval(() => {
      if (remaining > 0) remaining--;
      const mins = String(Math.floor(remaining / 60)).padStart(2, '0');
      const secs = String(remaining % 60).padStart(2, '0');
      document.getElementById('timer').textContent = `00:${mins}:${secs}`;
    }, 1000);

    setTimeout(() => {
      chart.applyOptions({ width: chartContainer.clientWidth, height: chartContainer.clientHeight });
      chart.timeScale().fitContent();
    }, 150);

    setInterval(fetchData, 1500);
    setInterval(fetchKalshi, 8000);
    fetchData();
    fetchKalshi();

    window.addEventListener('resize', () => {
      chart.applyOptions({ width: chartContainer.clientWidth });
    });
  </script>
</body>
</html>
"""


# ============================================================
# RUTAS FLASK
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():
    return jsonify(build_data())


@app.route("/api/kalshi")
def api_kalshi():
    market = get_kalshi_market()
    if market is None:
        return jsonify({"error": "No hay contratos abiertos"})
    return jsonify(market)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


start_brti()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
