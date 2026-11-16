import os
import time
import threading
import math

import psycopg
from psycopg.types.json import Jsonb


_lock = threading.RLock()
_last_tick_second = 0
_db_status = "PENDIENTE"


def _connect():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("Falta DATABASE_URL")
    return psycopg.connect(
        url,
        connect_timeout=5,
        sslmode="require"
    )


def init_db():
    global _db_status

    try:
        with _connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS btc_signal_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    snapshot_key TEXT UNIQUE NOT NULL,
                    signal_ts DOUBLE PRECISION NOT NULL,
                    market_ticker TEXT,
                    market_close_ts DOUBLE PRECISION,
                    price DOUBLE PRECISION NOT NULL,
                    direction TEXT,
                    bullish_score DOUBLE PRECISION,
                    bearish_score DOUBLE PRECISION,
                    predicted_price DOUBLE PRECISION,
                    ema9 DOUBLE PRECISION,
                    ema21 DOUBLE PRECISION,
                    rsi DOUBLE PRECISION,
                    momentum_pct DOUBLE PRECISION,
                    pattern TEXT,
                    features JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS btc_signal_outcomes (
                    id BIGSERIAL PRIMARY KEY,
                    snapshot_id BIGINT NOT NULL
                        REFERENCES btc_signal_snapshots(id)
                        ON DELETE CASCADE,
                    horizon_seconds INTEGER NOT NULL,
                    target_ts DOUBLE PRECISION NOT NULL,
                    future_price DOUBLE PRECISION,
                    change_pct DOUBLE PRECISION,
                    result TEXT,
                    resolved_ts DOUBLE PRECISION,
                    UNIQUE(snapshot_id, horizon_seconds)
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_btc_outcomes_pending
                ON btc_signal_outcomes(target_ts)
                WHERE future_price IS NULL
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_btc_snapshots_pattern
                ON btc_signal_snapshots(pattern, direction)
            """)

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def save_snapshot(data):
    """
    Guarda una señal como máximo una vez por minuto y contrato.
    Los resultados futuros se crean pendientes, nunca se inventan.
    """
    global _db_status

    try:
        signal_ts = float(data["signal_ts"])
        price = float(data["price"])

        if not math.isfinite(signal_ts) or not math.isfinite(price):
            return False

        if signal_ts <= 0 or price <= 0:
            return False

        ticker = str(data.get("market_ticker") or "SIN_CONTRATO")
        minute_bucket = int(signal_ts // 60)
        snapshot_key = f"{ticker}:{minute_bucket}"

        with _connect() as conn:
            row = conn.execute("""
                INSERT INTO btc_signal_snapshots (
                    snapshot_key, signal_ts, market_ticker,
                    market_close_ts, price, direction,
                    bullish_score, bearish_score, predicted_price,
                    ema9, ema21, rsi, momentum_pct, pattern, features
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (snapshot_key) DO NOTHING
                RETURNING id
            """, (
                snapshot_key,
                signal_ts,
                ticker,
                data.get("market_close_ts"),
                price,
                data.get("direction"),
                data.get("bullish_score"),
                data.get("bearish_score"),
                data.get("predicted_price"),
                data.get("ema9"),
                data.get("ema21"),
                data.get("rsi"),
                data.get("momentum_pct"),
                data.get("pattern"),
                Jsonb(data.get("features") or {})
            )).fetchone()

            if row is None:
                _db_status = "CONECTADA"
                return False

            snapshot_id = row[0]

            for horizon in (60, 180, 300, 900):
                conn.execute("""
                    INSERT INTO btc_signal_outcomes (
                        snapshot_id, horizon_seconds, target_ts
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (snapshot_id, horizon_seconds)
                    DO NOTHING
                """, (
                    snapshot_id,
                    horizon,
                    signal_ts + horizon
                ))

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def record_price(price, timestamp=None):
    """
    Resuelve objetivos solo con un precio de Coinbase cuyo
    timestamp sea igual o posterior al momento objetivo.
    Guarda como máximo una muestra por segundo.
    """
    global _last_tick_second, _db_status

    try:
        price = float(price)
        ts = float(timestamp if timestamp is not None else time.time())

        if not math.isfinite(price) or not math.isfinite(ts):
            return False

        if price <= 0 or ts <= 0 or ts > time.time() + 2:
            return False

        second = int(ts)

        with _lock:
            if second < _last_tick_second + 5:
                return False
            _last_tick_second = second

        with _connect() as conn:
            conn.execute("""
                UPDATE btc_signal_outcomes AS o
                SET future_price = %s,
                    change_pct = ((%s / s.price) - 1) * 100,
                    result = CASE
                        WHEN ((%s / s.price) - 1) * 100 > 0.01
                            THEN 'ALCISTA'
                        WHEN ((%s / s.price) - 1) * 100 < -0.01
                            THEN 'BAJISTA'
                        ELSE 'SIN CAMBIO'
                    END,
                    resolved_ts = %s
                FROM btc_signal_snapshots AS s
                WHERE o.snapshot_id = s.id
                  AND o.future_price IS NULL
                  AND o.target_ts <= %s
                  AND %s > s.signal_ts
            """, (price, price, price, price, ts, ts, ts))

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_pattern_stats(pattern):
    """
    Estadística histórica del mismo patrón a 15 minutos.
    No devuelve porcentajes si hay menos de 10 resultados.
    """
    global _db_status

    empty = {
        "samples": 0,
        "bullish_pct": None,
        "bearish_pct": None
    }

    try:
        if not pattern:
            return empty

        with _connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE o.result = 'ALCISTA'
                    ) AS bullish,
                    COUNT(*) FILTER (
                        WHERE o.result = 'BAJISTA'
                    ) AS bearish
                FROM btc_signal_outcomes AS o
                JOIN btc_signal_snapshots AS s
                  ON s.id = o.snapshot_id
                WHERE s.pattern = %s
                  AND o.horizon_seconds = 900
                  AND o.future_price IS NOT NULL
            """, (str(pattern),)).fetchone()

        total, bullish, bearish = row

        _db_status = "CONECTADA"

        if total < 10:
            return {
                "samples": total,
                "bullish_pct": None,
                "bearish_pct": None
            }

        return {
            "samples": total,
            "bullish_pct": round(bullish * 100 / total, 1),
            "bearish_pct": round(bearish * 100 / total, 1)
        }

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return empty


def database_status():
    return _db_status
