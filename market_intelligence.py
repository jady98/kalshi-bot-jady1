"""Contexto informativo BTC desde fuentes RSS públicas.
No modifica probabilidades ni señales: es una capa contextual hasta ser validada.
"""
import email.utils
import threading
import time
import xml.etree.ElementTree as ET

import requests

_LOCK = threading.RLock()
_STARTED = False
_STATE = {
    "status": "INICIANDO",
    "updated_ts": None,
    "last_error": None,
    "headline_count": 0,
    "bias": "SIN DATOS",
    "bias_score": 0.0,
    "sources": [],
    "headlines": [],
    "model_impact": "INFORMATIVO_SOLAMENTE"
}

_FEEDS = (
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
    ("Bitcoin Magazine", "https://bitcoinmagazine.com/.rss/full/"),
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml"),
)

_POSITIVE = (
    "surge", "rally", "rises", "rallying", "record high", "bullish",
    "inflow", "adoption", "approval", "approved", "突破", "上涨",
    "sube", "subida", "alcista", "adopcion", "aprob", "entrada neta"
)
_NEGATIVE = (
    "crash", "plunge", "falls", "selloff", "bearish", "outflow",
    "hack", "lawsuit", "ban", "liquidation", "rejected", "inflation",
    "rate hike", "war", "caída", "baja", "bajista", "prohib",
    "demanda", "hackeo", "liquidación", "rechaz"
)

def _sentiment(title, description=""):
    text = (str(title) + " " + str(description)).lower()
    pos = sum(1 for word in _POSITIVE if word in text)
    neg = sum(1 for word in _NEGATIVE if word in text)
    total = pos + neg
    score = (pos - neg) / total if total else 0.0
    label = "ALCISTA" if score >= 0.25 else "BAJISTA" if score <= -0.25 else "NEUTRAL"
    return round(score, 3), label

def _parse_date(value):
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        return parsed.timestamp() if parsed else None
    except Exception:
        return None

def refresh():
    headlines = []
    sources = []
    errors = []
    session = requests.Session()
    session.headers.update({"User-Agent": "BTC-Radar-Market-Intelligence/1.0"})
    for source, url in _FEEDS:
        try:
            response = session.get(url, timeout=8)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            found = 0
            for item in root.findall(".//item")[:15]:
                title = (item.findtext("title") or "").strip()
                if not title:
                    continue
                description = item.findtext("description") or ""
                link = (item.findtext("link") or "").strip()
                pub_date = item.findtext("pubDate") or ""
                score, label = _sentiment(title, description)
                headlines.append({
                    "source": source,
                    "title": title[:280],
                    "url": link,
                    "published_ts": _parse_date(pub_date),
                    "sentiment": label,
                    "sentiment_score": score
                })
                found += 1
            sources.append({"name": source, "ok": True, "items": found})
            if found == 0:
                errors.append(source + ": feed sin titulares")
        except Exception as exc:
            sources.append({"name": source, "ok": False, "items": 0})
            errors.append(source + ": " + str(exc)[:120])

    # Evitar duplicados por título y ordenar por fecha más reciente.
    unique = {}
    for item in headlines:
        unique[item["title"].lower()] = item
    headlines = sorted(
        unique.values(),
        key=lambda row: row.get("published_ts") or 0,
        reverse=True
    )[:30]
    scored = [row["sentiment_score"] for row in headlines if row["sentiment_score"]]
    average = sum(scored) / len(scored) if scored else 0.0
    bias = "ALCISTA" if average >= 0.20 else "BAJISTA" if average <= -0.20 else "MIXTA/NEUTRAL"
    with _LOCK:
        _STATE.update({
            "status": "ACTUALIZADA" if headlines else "SIN TITULARES",
            "updated_ts": time.time(),
            "last_error": "; ".join(errors)[:500] if errors else None,
            "headline_count": len(headlines),
            "bias": bias,
            "bias_score": round(average, 3),
            "sources": sources,
            "headlines": headlines,
            "model_impact": "INFORMATIVO_SOLAMENTE"
        })
    return snapshot()

def snapshot():
    with _LOCK:
        data = dict(_STATE)
        data["sources"] = [dict(row) for row in _STATE["sources"]]
        data["headlines"] = [dict(row) for row in _STATE["headlines"]]
        age = time.time() - data["updated_ts"] if data["updated_ts"] else None
        data["age_seconds"] = round(age, 1) if age is not None else None
        return data

def _worker():
    while True:
        refresh()
        time.sleep(300)

def start_worker():
    global _STARTED
    with _LOCK:
        if _STARTED:
            return
        _STARTED = True
    threading.Thread(target=_worker, name="btc-news-intelligence", daemon=True).start()
