import os
import time
import threading
import requests
import websocket
import json
import math

from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# =========================================================
# BTC RADAR PRO — COINBASE + POLYMARKET US
# Solo lectura. No realiza operaciones ni envía órdenes.
# =========================================================

COINBASE_PRODUCT = "BTC-USD"

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_SPOT = (
    "https://api.coinbase.com/v2/prices/BTC-USD/spot"
)
COINBASE_CANDLES = (
    "https://api.exchange.coinbase.com/"
    "products/BTC-USD/candles"
)

POLYMARKET_GATEWAY = "https://gateway.polymarket.us"

lock = threading.Lock()

state = {
    "btc": None,
    "btc_change": 0,
    "coinbase_connected": False,
    "coinbase_last_update": None,
    "candles": [],
    "market": None,
    "market_data": None,
    "polymarket_connected": False,
    "last_error": "",
    "last_market_update": None,
    "last_candle_update": 0,
    "last_market_list_update": 0,
    "last_bbo_update": 0,
}

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Radar-PRO/1.0",
    "Accept": "application/json",
})


def safe_float(value, default=None):
    try:
        if isinstance(value, dict):
            value = value.get("value")
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except Exception:
        return None


def coinbase_spot():
    response = session.get(COINBASE_SPOT, timeout=10)
    response.raise_for_status()
    data = response.json()
    return float(data["data"]["amount"])


def update_candles():
    response = session.get(
        COINBASE_CANDLES,
        params={"granularity": 60},
        timeout=12,
    )
    response.raise_for_status()

    raw = response.json()

    # Coinbase entrega:
    # [time, low, high, open, close, volume]
    candles = []

    for item in reversed(raw):
        if len(item) < 6:
            continue

        candles.append({
            "time": int(item[0]),
            "low": float(item[1]),
            "high": float(item[2]),
            "open": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
        })

    with lock:
        state["candles"] = candles[-300:]
        state["last_candle_update"] = time.time()


def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            value * multiplier
            + result * (1 - multiplier)
        )

    return result


def calculate_rsi(values, period=14):
    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        difference = values[i] - values[i - 1]
        gains.append(max(difference, 0))
        losses.append(max(-difference, 0))

    average_gain = sum(gains[-period:]) / period
    average_loss = sum(losses[-period:]) / period

    if average_loss == 0:
        return 100

    rs = average_gain / average_loss
    return 100 - (100 / (1 + rs))


def calculate_vwap(candles):
    if not candles:
        return None

    total_price_volume = 0
    total_volume = 0

    for candle in candles[-60:]:
        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3

        volume = candle["volume"]
        total_price_volume += typical * volume
        total_volume += volume

    if total_volume == 0:
        return None

    return total_price_volume / total_volume


def calculate_analysis():
    with lock:
        candles = list(state["candles"])
        price = state["btc"]

    if not candles or price is None:
        return {
            "ready": False,
            "message": "Esperando velas de Coinbase",
        }

    closes = [c["close"] for c in candles]

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    rsi = calculate_rsi(closes)
    vwap = calculate_vwap(candles)

    score = 0

    if ema9 is not None and ema21 is not None:
        score += 2 if ema9 > ema21 else -2

    if ema9 is not None:
        score += 1 if price > ema9 else -1

    if vwap is not None:
        score += 1 if price > vwap else -1

    if rsi is not None:
        if 50 < rsi < 70:
            score += 1
        elif 30 < rsi < 50:
            score -= 1
        elif rsi >= 70:
            score -= 1
        elif rsi <= 30:
            score += 1

    # Probabilidad técnica estimada, no garantía.
    probability = 50 + (score * 5)
    probability = max(10, min(90, probability))

    return {
        "ready": True,
        "ema9": ema9,
        "ema21": ema21,
        "rsi": rsi,
        "vwap": vwap,
        "bullish": probability,
        "bearish": 100 - probability,
        "direction": (
            "ALCISTA" if score > 0
            else "BAJISTA" if score < 0
            else "NEUTRAL"
        ),
        "score": score,
    }


def extract_markets(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("markets", "data", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def find_btc_15m_market():
    response = session.get(
        f"{POLYMARKET_GATEWAY}/v1/markets",
        params={
            "active": "true",
            "closed": "false",
            "categories": "crypto",
            "limit": 100,
        },
        timeout=15,
    )
    response.raise_for_status()

    markets = extract_markets(response.json())
    now = datetime.now(timezone.utc)

    candidates = []

    for market in markets:
        terms = market.get("assetPriceTerms") or {}
        asset = terms.get("asset") or {}

        symbol = str(
            asset.get("symbol", "")
        ).lower()

        horizon = str(
            terms.get("horizon", "")
        ).lower()

        window_end = parse_time(terms.get("windowEnd"))

        if symbol not in ("btc", "crypto/btc"):
            continue

        if horizon not in ("15m", "15min", "15_minutes"):
            continue

        if not window_end or window_end <= now:
            continue

        if market.get("closed") is True:
            continue

        candidates.append((window_end, market))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def update_market():
    market = find_btc_15m_market()

    with lock:
        state["market"] = market
        state["last_market_list_update"] = time.time()

    if not market:
        with lock:
            state["market_data"] = None
            state["polymarket_connected"] = False
        return

    slug = market.get("slug")

    if not slug:
        return

    response = session.get(
        f"{POLYMARKET_GATEWAY}/v1/markets/{slug}/bbo",
        timeout=12,
    )
    response.raise_for_status()

    payload = response.json()
    market_data = payload.get("marketData", payload)

    with lock:
        state["market_data"] = market_data
        state["polymarket_connected"] = True
        state["last_market_update"] = time.time()
        state["last_bbo_update"] = time.time()


def update_btc_price(price):
    with lock:
        previous = state["btc"]

        if previous:
            state["btc_change"] = (
                (price - previous) / previous
            ) * 100

        state["btc"] = price
        state["coinbase_connected"] = True
        state["coinbase_last_update"] = time.time()


def coinbase_websocket():
    def on_open(ws):
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [COINBASE_PRODUCT],
            "channel": "ticker",
        }))

        with lock:
            state["coinbase_connected"] = True

    def on_message(ws, message):
        try:
            data = json.loads(message)

            if data.get("channel") != "ticker":
                return

            for event in data.get("events", []):
                for ticker in event.get("tickers", []):
                    if ticker.get("product_id") != COINBASE_PRODUCT:
                        continue

                    price = safe_float(ticker.get("price"))

                    if price:
                        update_btc_price(price)

        except Exception as error:
            with lock:
                state["last_error"] = str(error)

    def on_error(ws, error):
        with lock:
            state["coinbase_connected"] = False
            state["last_error"] = str(error)

    def on_close(ws, *args):
        with lock:
            state["coinbase_connected"] = False

    while True:
        try:
            ws = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(ping_interval=20, ping_timeout=10)

        except Exception as error:
            with lock:
                state["coinbase_connected"] = False
                state["last_error"] = str(error)

        time.sleep(5)


def background_worker():
    while True:
        try:
            with lock:
                candle_age = (
                    time.time() - state["last_candle_update"]
                )
                market_age = (
                    time.time() - state["last_market_list_update"]
                )
                bbo_age = (
                    time.time() - state["last_bbo_update"]
                )

            if candle_age > 30:
                update_candles()

            if market_age > 30:
                update_market()

            elif bbo_age > 5:
                with lock:
                    market = state["market"]

                if market and market.get("slug"):
                    slug = market["slug"]
                    response = session.get(
                        f"{POLYMARKET_GATEWAY}/v1/markets/{slug}/bbo",
                        timeout=12,
                    )
                    response.raise_for_status()
                    payload = response.json()

                    with lock:
                        state["market_data"] = (
                            payload.get("marketData", payload)
                        )
                        state["polymarket_connected"] = True
                        state["last_bbo_update"] = time.time()
                        state["last_market_update"] = time.time()

        except Exception as error:
            with lock:
                state["last_error"] = str(error)

        time.sleep(2)


threading.Thread(
    target=coinbase_websocket,
    daemon=True,
).start()

threading.Thread(
    target=background_worker,
    daemon=True,
).start()
# =========================================================
# RUTAS Y PANEL VISUAL
# =========================================================

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/health")
def health():
    with lock:
        return jsonify({
            "ok": True,
            "coinbase_connected": state["coinbase_connected"],
            "polymarket_connected": state["polymarket_connected"],
            "btc": state["btc"],
            "last_error": state["last_error"],
        })


@app.route("/api/data")
def api_data():
    with lock:
        candles = list(state["candles"])
        price = state["btc"]
        change = state["btc_change"]
        coinbase = state["coinbase_connected"]
        polymarket = state["polymarket_connected"]
        market = state["market"]
        market_data = state["market_data"]
        error = state["last_error"]
        market_update = state["last_market_update"]

    analysis = calculate_analysis()

    terms = (
        market.get("assetPriceTerms", {})
        if market else {}
    )

    best_bid = None
    best_ask = None
    yes_price = None
    no_price = None
    last_trade = None
    bid_depth = None
    ask_depth = None

    if market_data:
        best_bid = safe_float(market_data.get("bestBid"))
        best_ask = safe_float(market_data.get("bestAsk"))
        yes_price = safe_float(market_data.get("longQuote"))
        no_price = safe_float(market_data.get("shortQuote"))
        last_trade = safe_float(market_data.get("lastTradePx"))
        bid_depth = market_data.get("bidDepth")
        ask_depth = market_data.get("askDepth")

    window_end = terms.get("windowEnd")
    price_to_beat = safe_float(terms.get("priceToBeat"))

    return jsonify({
        "btc": price,
        "btc_change": change,
        "coinbase_connected": coinbase,
        "polymarket_connected": polymarket,
        "analysis": analysis,
        "candles": candles,
        "market": {
            "question": market.get("question"),
            "slug": market.get("slug"),
            "window_end": window_end,
            "price_to_beat": price_to_beat,
            "yes": yes_price,
            "no": no_price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "last_trade": last_trade,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "state": market_data.get("state")
            if market_data else None,
        } if market else None,
        "market_update": market_update,
        "error": error,
    })


HTML = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport"
content="width=device-width, initial-scale=1">
<title>BTC Radar PRO</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>
*{box-sizing:border-box}
body{
    margin:0;
    background:#080d17;
    color:#eaf0fa;
    font-family:Arial,sans-serif;
}
header{
    padding:18px;
    display:flex;
    justify-content:space-between;
    align-items:center;
    border-bottom:1px solid #202a3b;
}
.logo{font-size:21px;font-weight:bold}
.logo span{color:#21c9a5}
.status{font-size:12px;color:#9aa9bd}
.container{padding:14px;max-width:1000px;margin:auto}
.card{
    background:#111a29;
    border:1px solid #243148;
    border-radius:14px;
    padding:16px;
    margin-bottom:13px;
}
.label{
    color:#94a6bd;
    font-size:12px;
    margin-bottom:9px;
}
.price{
    font-size:32px;
    font-weight:bold;
}
.small{font-size:12px;color:#94a6bd}
.grid{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:12px;
}
.metric{
    background:#0b1320;
    border-radius:10px;
    padding:13px;
}
.metric strong{
    display:block;
    font-size:22px;
    margin-top:7px;
}
.green{color:#20d6a4}
.red{color:#ff647c}
.neutral{color:#e5bd61}
.bar{
    height:12px;
    display:flex;
    overflow:hidden;
    background:#273248;
    border-radius:20px;
    margin-top:12px;
}
.bull{background:#16bd91}
.bear{background:#f05270}
canvas{width:100%!important;height:240px!important}
.market-price{font-size:23px;font-weight:bold}
.footer{
    text-align:center;
    color:#72839a;
    font-size:11px;
    padding:12px;
}
@media(max-width:450px){
    .price{font-size:27px}
    .metric strong{font-size:19px}
}
</style>
</head>

<body>

<header>
    <div class="logo">₿ BTC Radar <span>PRO</span></div>
    <div class="status" id="overall">Iniciando...</div>
</header>

<div class="container">

    <div class="card">
        <div class="label">BITCOIN / USD · COINBASE</div>
        <div class="price" id="btc">$--</div>
        <div class="small" id="change">Esperando datos...</div>
        <div class="small" id="coinbaseStatus">
            Coinbase: conectando
        </div>
    </div>

    <div class="card">
        <div class="label">ANÁLISIS DIRECCIONAL · PRÓXIMOS 15 MIN</div>

        <div class="grid">
            <div class="metric">
                <span class="green">↑ ALCISTA</span>
                <strong id="bull">--%</strong>
            </div>

            <div class="metric">
                <span class="red">↓ BAJISTA</span>
                <strong id="bear">--%</strong>
            </div>
        </div>

        <div class="bar">
            <div class="bull" id="bullBar" style="width:50%"></div>
            <div class="bear" id="bearBar" style="width:50%"></div>
        </div>

        <h2 id="direction" class="neutral">ESPERANDO DATOS</h2>
        <div class="small">
            Estimación técnica; no garantiza el resultado.
        </div>
    </div>

    <div class="card">
        <div class="label">GRÁFICA BTC/USD · VELAS DE 1 MINUTO</div>
        <canvas id="btcChart"></canvas>
    </div>

    <div class="card">
        <div class="label">INDICADORES TÉCNICOS</div>

        <div class="grid">
            <div class="metric">
                <span class="small">RSI 14</span>
                <strong id="rsi">--</strong>
            </div>

            <div class="metric">
                <span class="small">EMA 9</span>
                <strong id="ema9">--</strong>
            </div>

            <div class="metric">
                <span class="small">EMA 21</span>
                <strong id="ema21">--</strong>
            </div>

            <div class="metric">
                <span class="small">VWAP</span>
                <strong id="vwap">--</strong>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="label">POLYMARKET US · CONTRATO BTC 15 MIN</div>
        <div id="marketQuestion">Buscando contrato BTC...</div>

        <div class="grid" style="margin-top:14px">
            <div class="metric">
                <span class="green">YES / LONG</span>
                <div class="market-price" id="yes">--</div>
            </div>

            <div class="metric">
                <span class="red">NO / SHORT</span>
                <div class="market-price" id="no">--</div>
            </div>

            <div class="metric">
                <span class="small">MEJOR BID</span>
                <strong id="bid">--</strong>
            </div>

            <div class="metric">
                <span class="small">MEJOR ASK</span>
                <strong id="ask">--</strong>
            </div>
        </div>

        <p class="small" id="target">
            Precio objetivo: esperando datos
        </p>

        <p class="small" id="remaining">
            Tiempo restante: --
        </p>

        <p class="small" id="polyStatus">
            Polymarket: consultando API pública
        </p>
    </div>

    <div class="card">
        <div class="label">ESTADO DE CONEXIONES</div>
        <p id="connectionCoinbase">Coinbase: --</p>
        <p id="connectionPoly">Polymarket: --</p>
        <p class="small" id="updated">Última actualización: --</p>
    </div>

    <div class="footer">
        BTC RADAR PRO · Solo análisis y lectura de datos.
        No ejecuta operaciones.
    </div>
</div>

<script>
let chart = null;

function money(value){
    if(value === null || value === undefined)
        return "--";

    return "$" + Number(value).toLocaleString(
        "en-US",
        {minimumFractionDigits:2, maximumFractionDigits:2}
    );
}

function percent(value){
    if(value === null || value === undefined)
        return "--";

    return Number(value).toFixed(1) + "%";
}

function initChart(){
    const ctx = document.getElementById("btcChart");

    chart = new Chart(ctx,{
        type:"line",
        data:{
            labels:[],
            datasets:[{
                label:"BTC/USD",
                data:[],
                borderColor:"#20c9a5",
                backgroundColor:"rgba(32,201,165,.10)",
                fill:true,
                pointRadius:0,
                borderWidth:2,
                tension:.2
            }]
        },
        options:{
            responsive:true,
            maintainAspectRatio:false,
            animation:false,
            plugins:{
                legend:{display:false}
            },
            scales:{
                x:{
                    ticks:{color:"#8293aa",maxTicksLimit:6},
                    grid:{color:"#202b3d"}
                },
                y:{
                    ticks:{color:"#8293aa"},
                    grid:{color:"#202b3d"}
                }
            }
        }
    });
}

function updateChart(candles){
    if(!chart || !candles) return;

    chart.data.labels = candles.map(c =>
        new Date(c.time*1000).toLocaleTimeString(
            "es-US",
            {hour:"2-digit",minute:"2-digit"}
        )
    );

    chart.data.datasets[0].data =
        candles.map(c => c.close);

    chart.update("none");
}

function updateRemaining(end){
    if(!end){
        document.getElementById("remaining").textContent =
            "Tiempo restante: no disponible";
        return;
    }

    const seconds = Math.max(
        0,
        Math.floor((new Date(end).getTime()-Date.now())/1000)
    );

    const minutes = Math.floor(seconds/60);
    const rest = seconds%60;

    document.getElementById("remaining").textContent =
        "Tiempo restante: " + minutes + "m " + rest + "s";
}

async function refresh(){
    try{
        const response = await fetch("/api/data",{
            cache:"no-store"
        });

        if(!response.ok)
            throw new Error("Error HTTP " + response.status);

        const data = await response.json();

        document.getElementById("btc").textContent =
            money(data.btc);

        document.getElementById("change").textContent =
            percent(data.btc_change) + " desde la última actualización";

        document.getElementById("coinbaseStatus").textContent =
            data.coinbase_connected
            ? "Coinbase: conectado"
            : "Coinbase: sin conexión";

        document.getElementById("connectionCoinbase").textContent =
            "Coinbase: " +
            (data.coinbase_connected ? "Conectado" : "Desconectado");

        document.getElementById("connectionPoly").textContent =
            "Polymarket: " +
            (data.polymarket_connected ? "Datos recibidos" : "Sin datos");

        document.getElementById("overall").textContent =
            data.coinbase_connected
            ? "Radar activo"
            : "Esperando conexión";

        if(data.analysis && data.analysis.ready){
            const a = data.analysis;

            document.getElementById("bull").textContent =
                percent(a.bullish);

            document.getElementById("bear").textContent =
                percent(a.bearish);

            document.getElementById("bullBar").style.width =
                a.bullish + "%";

            document.getElementById("bearBar").style.width =
                a.bearish + "%";

            document.getElementById("direction").textContent =
                a.direction;

            document.getElementById("direction").className =
                a.direction === "ALCISTA" ? "green" :
                a.direction === "BAJISTA" ? "red" : "neutral";

            document.getElementById("rsi").textContent =
                a.rsi === null ? "--" : Number(a.rsi).toFixed(1);

            document.getElementById("ema9").textContent =
                money(a.ema9);

            document.getElementById("ema21").textContent =
                money(a.ema21);

            document.getElementById("vwap").textContent =
                money(a.vwap);

        }else{
            document.getElementById("direction").textContent =
                "ESPERANDO DATOS";
        }

        updateChart(data.candles);

        if(data.market){
            const m = data.market;

            document.getElementById("marketQuestion").textContent =
                m.question || m.slug || "Contrato BTC encontrado";

            document.getElementById("yes").textContent =
                m.yes === null ? "--" : money(m.yes);

            document.getElementById("no").textContent =
                m.no === null ? "--" : money(m.no);

            document.getElementById("bid").textContent =
                m.best_bid === null ? "--" : money(m.best_bid);

            document.getElementById("ask").textContent =
                m.best_ask === null ? "--" : money(m.best_ask);

            document.getElementById("target").textContent =
                "Precio objetivo: " +
                (m.price_to_beat === null
                    ? "aún no publicado"
                    : money(m.price_to_beat));

            updateRemaining(m.window_end);

            document.getElementById("polyStatus").textContent =
                "Polymarket: datos recibidos";
        }else{
            document.getElementById("marketQuestion").textContent =
                "No se encontró un contrato BTC de 15 minutos.";

            document.getElementById("polyStatus").textContent =
                "Polymarket: buscando contrato disponible";
        }

        document.getElementById("updated").textContent =
            "Última actualización: " +
            new Date().toLocaleTimeString();

    }catch(error){
        document.getElementById("overall").textContent =
            "Error de lectura";

        document.getElementById("updated").textContent =
            error.message;
    }
}

initChart();
refresh();
setInterval(refresh,3000);
</script>

</body>
</html>
"""


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
