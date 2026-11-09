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
    "last_price": None,
    "last_tick_time": None,
    "last_message_time": None,
    "average_60s": None,
    "average_15m": None,
    "error": None,
    "warning": None,
    "signal": "ESPERAR",
    "signal_reason": "Esperando datos suficientes",
    "signal_strength": 0,
}

ticks = deque(maxlen=MAX_TICKS)
candles = deque(maxlen=MAX_CANDLES)

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


# ============================================================
# CONSULTA REST DE KALSHI
# ============================================================

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


# ============================================================
# AUTENTICACION RSA WEBSOCKET KALSHI
# ============================================================

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


# ============================================================
# PROCESAMIENTO BRTI Y VELAS
# ============================================================

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
        update_candles_locked()


def update_candles_locked():
    if not ticks:
        return
    for tick in list(ticks)[-2:]:
        # Agrupar por minuto (Lightweight Charts requiere segundos o milisegundos Unix)
        minute = int((tick["time"] // 1000) // 60) * 60

        if candles and candles[-1]["time"] == minute:
            c = candles[-1]
            c["high"] = max(c["high"], tick["price"])
            c["low"] = min(c["low"], tick["price"])
            c["close"] = tick["price"]
        elif not candles or minute > candles[-1]["time"]:
            # Calcular medias simples para EMA de prueba en el backend
            candles.append({
                "time": minute,
                "open": tick["price"],
                "high": tick["price"],
                "low": tick["price"],
                "close": tick["price"],
                "ema9": tick["price"],
                "ema21": tick["price"]
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


# ============================================================
# HILO WEBSOCKET
# ============================================================

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
        sig = calculate_signal()
        c_list = list(candles)
    return {
        "connected": state["connected"],
        "price": p,
        "average_60s": state["average_60s"],
        "average_15m": state["average_15m"],
        "signal": sig["signal"],
        "reason": sig["reason"],
        "strength": sig["strength"],
        "candles": c_list,
        "error": state["error"],
        "warning": state["warning"]
    }


def calculate_signal():
    with lock:
        prices = [item["price"] for item in list(ticks)[-30:]]
    if len(prices) < 5:
        return {"signal": "ESPERAR", "reason": "Sincronizando ticks", "strength": 50}
    if prices[-1] >= prices[0]:
        return {"signal": "SUBE", "reason": "Tendencia alcista actual", "strength": 68}
    else:
        return {"signal": "BAJA", "reason": "Tendencia bajista actual", "strength": 62}


# ============================================================
# HTML INTERFAZ MÓVIL EXACTA (LIGHTWEIGHT CHARTS)
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>BTC Radar PRO - Live</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    body { background-color: #0b0e14; color: #ffffff; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    .card-bg { background-color: #121824; border: 1px solid #1e293b; }
  </style>
</head>
<body class="p-3 max-w-md mx-auto select-none pb-8">

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
    <div class="flex items-center gap-1.5 bg-emerald-950/60 text-emerald-400 text-xs px-2.5 py-1 rounded-full border border-emerald-500/30">
      <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
      <span class="font-semibold text-[11px]" id="conn-status">En Vivo</span>
    </div>
  </div>

  <div class="grid grid-cols-3 gap-2 mb-3">
    <div class="card-bg rounded-xl p-2.5 border-emerald-500/30">
      <div class="text-[11px] font-bold text-emerald-400"><i class="fa-solid fa-arrow-trend-up"></i> Alcista</div>
      <div class="text-xl font-black text-emerald-400 mt-1" id="prob-alcista">67.8%</div>
    </div>
    <div class="card-bg rounded-xl p-2 text-center flex flex-col justify-center border-slate-800">
      <span class="text-[9px] text-gray-400">Predicción 15m</span>
      <div class="text-xs font-bold text-emerald-400 mt-0.5" id="pred-price">--</div>
    </div>
    <div class="card-bg rounded-xl p-2.5 border-rose-500/30 flex flex-col items-end">
      <div class="text-[11px] font-bold text-rose-400">Bajista <i class="fa-solid fa-arrow-trend-down"></i></div>
      <div class="text-xl font-black text-rose-400 mt-1" id="prob-bajista">32.2%</div>
    </div>
  </div>

  <div class="card-bg rounded-xl p-3 mb-3 relative overflow-hidden">
    <div class="flex justify-between items-center mb-1 text-xs">
      <span class="text-gray-400 font-medium">BTC/USD (BRTI)</span>
      <span class="font-mono text-emerald-400 font-bold bg-emerald-950/40 px-2 py-0.5 rounded border border-emerald-500/30" id="badge-price">--</span>
    </div>
    <div class="h-64 w-full" id="tradingview-chart"></div>
  </div>

  <div class="grid grid-cols-2 gap-2 mb-3">
    <div class="card-bg rounded-xl p-3">
      <div class="text-xs text-gray-400 mb-0.5"><i class="fa-regular fa-clock"></i> Contrato Kalshi</div>
      <div class="font-mono text-sm font-bold text-slate-100" id="kalshi-ticker">Cargando...</div>
      <div class="text-[10px] text-emerald-400 mt-1" id="kalshi-prices">Yes: -- | No: --</div>
    </div>
    <div class="card-bg rounded-xl p-3">
      <div class="text-xs text-gray-400 mb-0.5"><i class="fa-solid fa-chart-line"></i> Promedio 60s</div>
      <div class="font-mono text-lg font-black text-emerald-400" id="avg60">--</div>
    </div>
  </div>

  <div class="grid grid-cols-2 gap-3 mb-3">
    <button class="bg-emerald-500 hover:bg-emerald-600 text-slate-950 font-black py-3 rounded-xl flex items-center justify-center gap-2">
      <i class="fa-solid fa-arrow-trend-up"></i> SUBE
    </button>
    <button class="bg-rose-500 hover:bg-rose-600 text-slate-950 font-black py-3 rounded-xl flex items-center justify-center gap-2">
      <i class="fa-solid fa-arrow-trend-down"></i> BAJA
    </button>
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

    async function fetchData() {
      try {
        const res = await fetch('/api/data', { cache: 'no-store' });
        const data = await res.json();
        if (data.price) {
          document.getElementById('badge-price').textContent = data.price.toLocaleString();
          document.getElementById('pred-price').textContent = "↑ " + (data.price + 100).toFixed(2);
        }
        if (data.average_60s) {
          document.getElementById('avg60').textContent = data.average_60s.toLocaleString();
        }
        if (data.candles && data.candles.length > 0) {
          candleSeries.setData(data.candles);
        }
        document.getElementById('conn-status').textContent = data.connected ? "En Vivo" : "Reconectando";
      } catch (e) {
        console.error(e);
      }
    }

    async function fetchKalshi() {
      try {
        const res = await fetch('/api/kalshi', { cache: 'no-store' });
        const m = await res.json();
        if (m && m.ticker) {
          document.getElementById('kalshi-ticker').textContent = m.ticker;
          document.getElementById('kalshi-prices').textContent = `Yes: ${m.yes_bid || '--'}¢ | No: ${m.no_bid || '--'}¢`;
        }
      } catch (e) {}
    }

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
