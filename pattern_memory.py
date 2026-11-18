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
                CREATE TABLE IF NOT EXISTS brti_minute_candles (
                    candle_time BIGINT PRIMARY KEY,
                    open_price DOUBLE PRECISION NOT NULL,
                    high_price DOUBLE PRECISION NOT NULL,
                    low_price DOUBLE PRECISION NOT NULL,
                    close_price DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                    open_ts DOUBLE PRECISION NOT NULL,
                    close_ts DOUBLE PRECISION NOT NULL
                )
            """)

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


def save_brti_candle(candle):
    """Persiste una vela BRTI cerrada para sobrevivir a reinicios de Render."""
    global _db_status
    try:
        row = {
            "time": int(candle["time"]),
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
            "volume": float(candle.get("volume", 0) or 0),
            "_open_ts": float(candle.get("_open_ts", candle["time"])),
            "_close_ts": float(candle.get("_close_ts", candle["time"]))
        }
        if row["time"] <= 0 or min(row["open"], row["high"], row["low"], row["close"]) <= 0:
            return False
        with _connect() as conn:
            conn.execute("""
                INSERT INTO brti_minute_candles
                    (candle_time, open_price, high_price, low_price,
                     close_price, volume, open_ts, close_ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (candle_time) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    open_ts = EXCLUDED.open_ts,
                    close_ts = EXCLUDED.close_ts
            """, (
                row["time"], row["open"], row["high"], row["low"],
                row["close"], row["volume"], row["_open_ts"], row["_close_ts"]
            ))
        _db_status = "CONECTADA"
        return True
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_brti_candles(limit=300):
    """Carga las últimas velas BRTI persistidas, en orden cronológico."""
    global _db_status
    try:
        limit = max(1, min(int(limit), 300))
        with _connect() as conn:
            rows = conn.execute("""
                SELECT candle_time, open_price, high_price, low_price,
                       close_price, volume, open_ts, close_ts
                FROM brti_minute_candles
                ORDER BY candle_time DESC
                LIMIT %s
            """, (limit,)).fetchall()
        candles = [{
            "time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
            "low": float(r[3]), "close": float(r[4]), "volume": float(r[5] or 0),
            "_open_ts": float(r[6]), "_close_ts": float(r[7])
        } for r in reversed(rows)]
        _db_status = "CONECTADA"
        return candles
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return []


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

            # Cada horizonte se mide desde la señal, aunque el contrato
            # de Kalshi cierre antes. Así 15m significa siempre +900 segundos.
            for horizon in (60, 180, 300, 900):
                target_ts = signal_ts + horizon

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

            # Resultado principal: precio observado al cierre real del contrato.
            conn.execute("""
                INSERT INTO btc_signal_outcomes (
                    snapshot_id, horizon_seconds, target_ts
                )
                VALUES (%s, -1, %s)
                ON CONFLICT (snapshot_id, horizon_seconds)
                DO NOTHING
            """, (snapshot_id, market_close_ts))

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def record_price(price, timestamp=None):
    """
    Resuelve objetivos con el precio BRTI observado igual o después
    del momento objetivo. BRTI es la fuente única de evaluación.
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
                  AND o.horizon_seconds > 0
                  AND o.target_ts <= %s
                  AND %s > s.signal_ts
                  AND s.market_close_ts IS NOT NULL
                  AND %s <= o.target_ts + 60 
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
def record_market_close(market_ticker, closing_average, resolved_ts=None):
    """Grade KXBTC15M with the BRTI average in the 60 seconds before close."""
    global _db_status
    ticker = str(market_ticker or "").strip()
    try:
        average = float(closing_average)
        ts = float(resolved_ts if resolved_ts is not None else time.time())
    except (TypeError, ValueError):
        return False
    if not ticker or not math.isfinite(average) or average <= 0 or not math.isfinite(ts):
        return False
    try:
        with _connect() as conn:
            conn.execute("""
                UPDATE btc_signal_outcomes AS o
                SET future_price = %s,
                    change_pct = CASE
                        WHEN NULLIF(s.features->>'market_target', '') IS NOT NULL
                         AND (s.features->>'market_target')::DOUBLE PRECISION > 0
                        THEN ((%s / (s.features->>'market_target')::DOUBLE PRECISION) - 1) * 100
                        ELSE NULL
                    END,
                    result = CASE
                        WHEN NULLIF(s.features->>'market_target', '') IS NULL
                          OR (s.features->>'market_target')::DOUBLE PRECISION <= 0
                        THEN 'SIN CAMBIO'
                        WHEN %s >= (s.features->>'market_target')::DOUBLE PRECISION
                        THEN 'ALCISTA'
                        ELSE 'BAJISTA'
                    END,
                    resolved_ts = %s
                FROM btc_signal_snapshots AS s
                WHERE o.snapshot_id = s.id
                  AND o.horizon_seconds = -1
                  AND o.future_price IS NULL
                  AND s.market_ticker = %s
            """, (average, average, average, ts, ticker))
        _db_status = "CONECTADA"
        return True
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_prediction_accuracy():
    """
    Precisión por contrato: usa una sola señal, la más reciente guardada
    antes del cierre de cada ticker. Evita contar actualizaciones repetidas
    del mismo contrato como predicciones independientes.
    """
    empty = {
        "predictions_evaluated": 0,
        "correct": 0,
        "incorrect": 0,
        "no_change": 0,
        "indecisive_contracts": 0,
        "contracts_evaluated": 0,
        "accuracy_pct": None,
        "measurement": "UNA ÚLTIMA SEÑAL POR CONTRATO"
    }

    try:
        with _connect() as conn:
            row = conn.execute("""
                WITH latest AS (
                    SELECT DISTINCT ON (s.market_ticker)
                        s.market_ticker,
                        s.direction,
                        o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = -1
                     AND o.future_price IS NOT NULL
                    WHERE s.market_ticker IS NOT NULL
                    ORDER BY s.market_ticker, s.signal_ts DESC, s.id DESC
                )
                SELECT
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                          AND direction <> result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ),
                    COUNT(*) FILTER (
                        WHERE direction = 'INDECISO'
                    )
                FROM latest
            """).fetchone()

        evaluated, correct, incorrect, no_change, indecisive = row
        total = correct + incorrect

        return {
            "predictions_evaluated": evaluated,
            "correct": correct,
            "incorrect": incorrect,
            "no_change": no_change,
            "indecisive_contracts": indecisive,
            "contracts_evaluated": evaluated,
            "accuracy_pct": round(correct * 100 / total, 1)
                if total else None,
            "measurement": "UNA ÚLTIMA SEÑAL POR CONTRATO"
        }

    except Exception as exc:
        return {
            **empty,
            "error": str(exc)[:180]
        }


def get_prediction_accuracy_by_source():
    """
    Precisión por fuente usando una sola señal (la más reciente) por contrato.
    Las señales antiguas sin fuente identificable se separan como
    TECNICA_SIN_FUENTE; INDECISO es una dirección, nunca una fuente.
    """
    try:
        with _connect() as conn:
            rows = conn.execute("""
                WITH latest AS (
                    SELECT DISTINCT ON (s.market_ticker)
                        s.market_ticker,
                        s.signal_ts,
                        s.direction,
                        s.features,
                        o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = -1
                     AND o.future_price IS NOT NULL
                    WHERE s.market_ticker IS NOT NULL
                    ORDER BY s.market_ticker, s.signal_ts DESC, s.id DESC
                ),
                classified AS (
                    SELECT
                        CASE
                            WHEN UPPER(COALESCE(features->>'signal_source', ''))
                                 LIKE '%BRTI%'
                              OR UPPER(COALESCE(features->>'direction_method', ''))
                                 LIKE '%BRTI%' THEN 'BRTI'
                            WHEN UPPER(COALESCE(features->>'signal_source', ''))
                                 LIKE '%COINBASE%'
                              OR UPPER(COALESCE(features->>'direction_method', ''))
                                 LIKE '%COINBASE%'
                              OR features->>'direction_method' = 'technical_only_v1'
                                THEN 'COINBASE'
                            ELSE 'TECNICA_SIN_FUENTE'
                        END AS source,
                        direction,
                        result
                    FROM latest
                )
                SELECT
                    source,
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                    ) AS evaluated,
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ) AS correct,
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                          AND direction <> result
                    ) AS incorrect,
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ) AS no_change,
                    COUNT(*) FILTER (
                        WHERE direction = 'INDECISO'
                    ) AS indecisive
                FROM classified
                GROUP BY source
                ORDER BY source
            """).fetchall()

        result = []
        for source, evaluated, correct, incorrect, no_change, indecisive in rows:
            total = correct + incorrect
            result.append({
                "source": source,
                "predictions_evaluated": evaluated,
                "contracts_evaluated": evaluated,
                "correct": correct,
                "incorrect": incorrect,
                "no_change": no_change,
                "indecisive_contracts": indecisive,
                "accuracy_pct": round(correct * 100 / total, 1)
                    if total else None,
                "measurement": "UNA ÚLTIMA SEÑAL POR CONTRATO"
            })
        return {
            "sources": result,
            "error": None,
            "measurement": "UNA ÚLTIMA SEÑAL POR CONTRATO"
        }

    except Exception as exc:
        return {"sources": [], "error": str(exc)[:180]}


def get_15m_prediction_accuracy():
    """Precisión direccional a 15 minutos: evalúa cada señal individual al cumplirse +900 segundos."""
    empty = {
        "predictions_evaluated": 0, "correct": 0, "incorrect": 0,
        "no_change": 0, "indecisive_contracts": 0,
        "contracts_evaluated": 0, "accuracy_pct": None,
        "measurement": "TODAS LAS SEÑALES RESUELTAS · HORIZONTE 900S"
    }
    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                          AND direction <> result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    )
                FROM btc_signal_snapshots AS s
                JOIN btc_signal_outcomes AS o
                  ON o.snapshot_id = s.id
                 AND o.horizon_seconds = 900
                 AND o.future_price IS NOT NULL
                WHERE s.market_ticker IS NOT NULL
            """).fetchone()
        evaluated, correct, incorrect, no_change, indecisive = row
        total = correct + incorrect
        return {
            "predictions_evaluated": evaluated,
            "correct": correct,
            "incorrect": incorrect,
            "no_change": no_change,
            "indecisive_contracts": indecisive,
            "contracts_evaluated": evaluated,
            "accuracy_pct": round(correct * 100 / total, 1) if total else None,
            "measurement": "TODAS LAS SEÑALES RESUELTAS · HORIZONTE 900S"
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}


def get_15m_prediction_accuracy_by_source():
    """Precisión a 15 minutos por fuente, contando cada señal individual resuelta a +900 segundos."""
    try:
        with _connect() as conn:
            rows = conn.execute("""
                WITH candidates AS (
                    SELECT s.market_ticker, s.signal_ts, s.direction, o.result,
                        source.source
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = 900
                     AND o.future_price IS NOT NULL
                    CROSS JOIN LATERAL (
                        SELECT CASE
                            WHEN UPPER(COALESCE(s.features->>'signal_source', '')) LIKE '%BRTI%'
                              OR UPPER(COALESCE(s.features->>'direction_method', '')) LIKE '%BRTI%'
                                THEN 'BRTI'
                            WHEN UPPER(COALESCE(s.features->>'signal_source', '')) LIKE '%COINBASE%'
                              OR UPPER(COALESCE(s.features->>'direction_method', '')) LIKE '%COINBASE%'
                              OR s.features->>'direction_method' = 'technical_only_v1'
                                THEN 'COINBASE'
                            ELSE 'TECNICA_SIN_FUENTE'
                        END AS source
                    ) AS source
                    WHERE s.market_ticker IS NOT NULL
                )
                SELECT
                    source,
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA')
                          AND direction <> result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    )
                FROM candidates
                GROUP BY source
                ORDER BY source
            """).fetchall()
        result = []
        for source, evaluated, correct, incorrect, no_change, indecisive in rows:
            total = correct + incorrect
            result.append({
                "source": source,
                "predictions_evaluated": evaluated,
                "contracts_evaluated": evaluated,
                "correct": correct,
                "incorrect": incorrect,
                "no_change": no_change,
                "indecisive_contracts": indecisive,
                "accuracy_pct": round(correct * 100 / total, 1) if total else None,
                "measurement": "TODAS LAS SEÑALES RESUELTAS POR FUENTE · HORIZONTE 900S"
            })
        return {
            "sources": result,
            "error": None,
            "measurement": "TODAS LAS SEÑALES RESUELTAS POR FUENTE · HORIZONTE 900S"
        }
    except Exception as exc:
        return {"sources": [], "error": str(exc)[:180]}


def get_recent_predictions(limit=50):
    """Devuelve señales recientes y su resultado real a 15 minutos."""
    try:
        limit = max(1, min(int(limit), 100))

        with _connect() as conn:
            rows = conn.execute("""
                SELECT
                    s.id,
                    s.signal_ts,
                    s.market_ticker,
                    s.price,
                    s.direction,
                    s.bullish_score,
                    s.bearish_score,
                    s.predicted_price,
                    s.ema9,
                    s.ema21,
                    s.rsi,
                    s.momentum_pct,
                    s.pattern,
                    s.features,
                    o.future_price,
                    o.change_pct,
                    o.result
                FROM btc_signal_snapshots AS s
                LEFT JOIN btc_signal_outcomes AS o
                  ON o.snapshot_id = s.id
                 AND o.horizon_seconds = 900
                ORDER BY s.signal_ts DESC
                LIMIT %s
            """, (limit,)).fetchall()

        items = []

        for r in rows:
            items.append({
                "id": r[0],
                "signal_ts": r[1],
                "market_ticker": r[2],
                "price": r[3],
                "direction": r[4],
                "bullish_score": r[5],
                "bearish_score": r[6],
                "predicted_price": r[7],
                "ema9": r[8],
                "ema21": r[9],
                "rsi": r[10],
                "momentum_pct": r[11],
                "pattern": r[12],
                "features": r[13],
                "future_price_15m": r[14],
                "change_pct_15m": r[15],
                "result_15m": r[16]
            })

        return {
            "items": items,
            "count": len(items),
            "error": None
        }

    except Exception as exc:
        return {
            "items": [],
            "count": 0,
            "error": str(exc)[:180]
        }



def database_status():
    return _db_status

def get_database_counts():
    empty = {
        "signals_saved": 0,
        "outcomes_resolved_15m": 0,
        "outcomes_pending_15m": 0,
        "close_outcomes_resolved": 0,
        "close_outcomes_pending": 0
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
                     AND future_price IS NULL),

                    (SELECT COUNT(*)
                     FROM btc_signal_outcomes
                     WHERE horizon_seconds = -1
                     AND future_price IS NOT NULL),

                    (SELECT COUNT(*)
                     FROM btc_signal_outcomes
                     WHERE horizon_seconds = -1
                     AND future_price IS NULL)
            """).fetchone()

        return {
            "signals_saved": row[0],
            "outcomes_resolved_15m": row[1],
            "outcomes_pending_15m": row[2],
            "close_outcomes_resolved": row[3],
            "close_outcomes_pending": row[4]
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
