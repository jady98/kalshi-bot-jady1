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

            conn.execute("""
                CREATE TABLE IF NOT EXISTS btc_official_market_results (
                market_ticker TEXT PRIMARY KEY,
                official_result TEXT NOT NULL
                CHECK (official_result IN ('SI', 'NO')),
                signal_direction TEXT NOT NULL
                CHECK (signal_direction IN ('ALCISTA', 'BAJISTA')),
                source TEXT NOT NULL DEFAULT 'confirmacion_manual',
                recorded_ts DOUBLE PRECISION NOT NULL
                )
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

        ticker = str(data.get("market_ticker") or "").strip()
        try:
            market_close_ts = float(data.get("market_close_ts"))
        except (TypeError, ValueError):
            return False

        if (
            not ticker
            or ticker == "SIN_CONTRATO"
            or not math.isfinite(market_close_ts)
            or market_close_ts <= signal_ts
        ):
            return False

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
                market_close_ts,
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
                target_ts = signal_ts + horizon

                if target_ts > market_close_ts:
                    continue

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
                    target_ts
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
                  AND s.market_close_ts IS NOT NULL
                  AND %s <= s.market_close_ts + 5 
          """, (price, price, price, price, ts, ts, ts, ts))

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
def get_prediction_accuracy():
    empty = {
        "predictions_evaluated": 0,
        "correct": 0,
        "incorrect": 0,
        "no_change": 0,
        "accuracy_pct": None
    }

    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE s.direction IN ('ALCISTA', 'BAJISTA')
                          AND o.result IN ('ALCISTA', 'BAJISTA')
                    ),
                    COUNT(*) FILTER (
                        WHERE s.direction IN ('ALCISTA', 'BAJISTA')
                          AND s.direction = o.result
                    ),
                    COUNT(*) FILTER (
                        WHERE s.direction IN ('ALCISTA', 'BAJISTA')
                          AND o.result IN ('ALCISTA', 'BAJISTA')
                          AND s.direction <> o.result
                    ),
                    COUNT(*) FILTER (
                        WHERE s.direction IN ('ALCISTA', 'BAJISTA')
                          AND o.result = 'SIN CAMBIO'
                    )
                FROM btc_signal_outcomes AS o
                JOIN btc_signal_snapshots AS s
                  ON s.id = o.snapshot_id
                WHERE o.horizon_seconds = 900
                  AND o.future_price IS NOT NULL
                  AND s.features->>'direction_method' = 'technical_only_v1'
            """).fetchone()

        evaluated, correct, incorrect, no_change = row
        total = correct + incorrect

        return {
            "predictions_evaluated": evaluated,
            "correct": correct,
            "incorrect": incorrect,
            "no_change": no_change,
            "accuracy_pct": round(correct * 100 / total, 1)
                if total else None
        }

    except Exception as exc:
        return {
            **empty,
            "error": str(exc)[:180]
        }

def database_status():
    return _db_status

def get_database_counts():
    empty = {
        "signals_saved": 0,
        "outcomes_resolved_15m": 0,
        "outcomes_pending_15m": 0
    }

    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT
                    (SELECT COUNT(*)
                     FROM btc_signal_snapshots),

                    (SELECT COUNT(*)
                     FROM btc_signal_outcomes
                     WHERE horizon_seconds = 900
                     AND future_price IS NOT NULL),

                    (SELECT COUNT(*)
                     FROM btc_signal_outcomes
                     WHERE horizon_seconds = 900
                     AND future_price IS NULL)
            """).fetchone()

        return {
            "signals_saved": row[0],
            "outcomes_resolved_15m": row[1],
            "outcomes_pending_15m": row[2]
        }

    except Exception as exc:
        return {
            **empty,
            "error": str(exc)[:180]
        }

def record_official_market_result(
    market_ticker,
    official_result,
    signal_direction,
    source="confirmacion_manual"
):
    global _db_status

    ticker = str(market_ticker or "").strip()
    result = str(official_result or "").strip().upper()
    direction = str(signal_direction or "").strip().upper()

    if not ticker or result not in ("SI", "NO"):
        return False

    if direction not in ("ALCISTA", "BAJISTA"):
        return False

    try:
        with _connect() as conn:
            conn.execute("""
                INSERT INTO btc_official_market_results (
                    market_ticker,
                    official_result,
                    signal_direction,
                    source,
                    recorded_ts
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (market_ticker) DO UPDATE SET
                    official_result = EXCLUDED.official_result,
                    signal_direction = EXCLUDED.signal_direction,
                    source = EXCLUDED.source,
                    recorded_ts = EXCLUDED.recorded_ts
            """, (
                ticker,
                result,
                direction,
                str(source or "confirmacion_manual")[:80],
                time.time()
            ))

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_official_market_accuracy():

    empty = {
        "contracts_evaluated": 0,
        "correct": 0,
        "incorrect": 0,
        "accuracy_pct": None,
        "results": []
    }

    try:
        with _connect() as conn:
            rows = conn.execute("""
                SELECT
                    market_ticker,
                    official_result,
                    signal_direction,
                    CASE
                        WHEN (
                            official_result = 'SI'
                            AND signal_direction = 'ALCISTA'
                        )
                        OR (
                            official_result = 'NO'
                            AND signal_direction = 'BAJISTA'
                        )
                        THEN TRUE
                        ELSE FALSE
                    END AS correct
                FROM btc_official_market_results
                ORDER BY recorded_ts DESC
            """).fetchall()

        correct = sum(1 for row in rows if row[3])
        total = len(rows)

        return {
            "contracts_evaluated": total,
            "correct": correct,
            "incorrect": total - correct,
            "accuracy_pct": (
                round(correct * 100 / total, 1)
                if total else None
            ),
            "results": [
                {
                    "market_ticker": row[0],
                    "official_result": row[1],
                    "signal_direction": row[2],
                    "evaluation": (
                        "ACIERTO" if row[3] else "FALLO"
                    )
                }
                for row in rows[:20]
            ]
        }

    except Exception as exc:
        return {
            **empty,
            "error": str(exc)[:180]
            }
