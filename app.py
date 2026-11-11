
import os
import time
import json
import threading
import urllib.request
from collections import deque
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
COINBASE_SPOT = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
KRAKEN = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"

lock = threading.Lock()
price = None
provider = "Desconectado"
last_update = 0
last_error = ""
history = deque(maxlen=120)
cache_time = 0

def get_json(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "BTC-Radar/1.0",
            "Accept": "application/json"
        }
    )

    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))

def get_coinbase():
    data = get_json(COINBASE)
    return float(data["price"])

def get_coinbase_spot():
    data = get_json(COINBASE_SPOT)
    return float(data["data"]["amount"])

def get_kraken():
    data = get_json(KRAKEN)

    if data.get("error"):
        raise RuntimeError(str(data["error"]))

    result = data["result"]
    pair = next(iter(result))
    return float(result[pair]["c"][0])

def update_price():
    global price, provider, last_update
    global last_error, cache_time

    with lock:
        if time.time() - cache_time < 0.8 and price is not None:
            return

    sources = [
        ("Coinbase Exchange", get_coinbase),
        ("Coinbase Spot", get_coinbase_spot),
        ("Kraken", get_kraken)
    ]

    errors = []

    for name, function in sources:
        try:
            new_price = function()

            if new_price <= 0:
                raise ValueError("Precio inválido")

            now = time.time()

            with lock:
                price = new_price
                provider = name
                last_update = now
                cache_time = now
                last_error = ""

                history.append((now, new_price))

            return

        except Exception as error:
            errors.append(name + ": " + str(error))

    with lock:
        cache_time = time.time()
        last_error = " | ".join(errors)

def get_analysis():
    with lock:
        current = price
        source = provider
        updated = last_update
        error = last_error
        samples = list(history)

    if current is None:
        return {
            "connected": False,
            "price": None,
            "direction": "ESPERANDO DATOS",
            "change": 0,
            "provider": source,
            "updated": None,
            "error": error
        }

    now = time.time()
    age = round(now - updated, 1)
    connected = age < 10

    reference = None

    for timestamp, value in samples:
        if timestamp <= now - 10:
            reference = value
        else:
            break

    if reference is None and len(samples) >= 2:
        reference = samples[-2][1]

    if reference is None or reference == 0:
        change = 0
        direction = "ESPERANDO DATOS"
    else:
        change = (current - reference) / reference * 100

        if change > 0.001:
            direction = "SUBE"
        elif change < -0.001:
            direction = "BAJA"
        else:
            direction = "ESTABLE"

    return {
        "connected": connected,
        "price": round(current, 2),
        "direction": direction,
        "change": round(change, 4),
        "provider": source if connected else "Desconectado",
        "updated": age,
        "error": error if not connected else ""
    }

@app.route("/")
def home():
    return render_template_string(PAGE)

@app.route("/api/data")
def api_data():
    update_price()

    response = jsonify(get_analysis())
    response.headers["Cache-Control"] = "no-store"
    return response

@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "BTC RADAR"})

PAGE = """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC RADAR</title>

<style>
* { box-sizing:border-box; }

body {
    margin:0;
    padding:20px;
    background:#080d18;
    color:white;
    font-family:Arial,sans-serif;
    text-align:center;
}

.container {
    max-width:500px;
    margin:30px auto;
    padding:25px;
    background:#111a2c;
    border:1px solid #293b5b;
    border-radius:18px;
}

h1 {
    font-size:27px;
    margin-bottom:5px;
}

.subtitle {
    color:#9daecc;
    font-size:13px;
    margin-bottom:30px;
}

.status {
    font-size:14px;
    color:#ff536b;
    margin-bottom:25px;
}

.price-label {
    color:#aebbd4;
    font-size:15px;
}

.price {
    font-size:clamp(32px,9vw,43px);
    font-weight:bold;
    margin:12px 0 30px;
    overflow-wrap:anywhere;
}

.direction {
    font-size:40px;
    font-weight:900;
    padding:22px 5px;
    border-radius:13px;
    background:#202b40;
    margin-bottom:20px;
}

.up { color:#00e58b; }
.down { color:#ff4562; }
.neutral { color:#ffc44d; }

.change {
    font-size:20px;
    margin:15px 0;
}

.info {
    border-top:1px solid #34415a;
    padding-top:18px;
    margin-top:20px;
    color:#b7c4dc;
    font-size:13px;
    line-height:2;
}

.error {
    color:#ff8798;
    font-size:11px;
    overflow-wrap:anywhere;
    margin-top:15px;
}

</style>
</head>

<body>

<div class="container">

<h1>₿ BTC RADAR</h1>

<div class="subtitle">
Bitcoin · Coinbase / Kraken
</div>

<div class="status" id="status">
● CONECTANDO
</div>

<div class="price-label">PRECIO ACTUAL BTC/USD</div>

<div class="price" id="price">--</div>

<div class="direction neutral" id="direction">
ESPERANDO
</div>

<div class="change" id="change">
0.0000%
</div>

<div class="info">
<div>Proveedor: <span id="provider">--</span></div>
<div>Última actualización: <span id="updated">--</span></div>
<div>Frecuencia: 1 segundo</div>
</div>

<div class="error" id="error"></div>

</div>

<script>
const $ = id => document.getElementById(id);

function money(value) {
    return "$" + Number(value).toLocaleString(
        "en-US",
        {
            minimumFractionDigits:2,
            maximumFractionDigits:2
        }
    );
}

async function refresh() {
    try {
        const response = await fetch(
            "/api/data?t=" + Date.now(),
            {cache:"no-store"}
        );

        if (!response.ok) {
            throw new Error("HTTP " + response.status);
        }

        const data = await response.json();

        if (data.connected) {
            $("status").textContent = "● CONECTADO";
            $("status").style.color = "#00e58b";
        } else {
            $("status").textContent = "● SIN CONEXIÓN";
            $("status").style.color = "#ff536b";
        }

        $("price").textContent =
            data.price !== null ? money(data.price) : "--";

        $("direction").textContent = data.direction;

        $("direction").className = "direction " +
            (data.direction === "SUBE" ? "up" :
             data.direction === "BAJA" ? "down" :
             "neutral");

        const sign = data.change > 0 ? "+" : "";

        $("change").textContent =
            sign + Number(data.change).toFixed(4) + "%";

        $("change").style.color =
            data.change > 0 ? "#00e58b" :
            data.change < 0 ? "#ff4562" : "#ffc44d";

        $("provider").textContent = data.provider;

        $("updated").textContent =
            data.updated === null ? "--" : data.updated + " s";

        $("error").textContent =
            data.error || "";

    } catch (error) {
        $("status").textContent = "● ERROR DEL SERVIDOR";
        $("status").style.color = "#ff536b";
        $("error").textContent = error.toString();
    }
}

refresh();
setInterval(refresh, 1000);
</script>

</body>
</html>
"""

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
    
