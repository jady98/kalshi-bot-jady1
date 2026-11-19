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


def get_brti_candle_diagnostics():
    """Resumen de solo lectura del historial BRTI persistido."""
    empty = {
        "count": 0,
        "first_candle_ts": None,
        "last_candle_ts": None,
        "span_hours": 0,
        "gaps_over_90_seconds": 0,
        "missing_minutes_estimate": 0,
        "error": None
    }
    try:
        with _connect() as conn:
            row = conn.execute("""
                WITH ordered AS (
                    SELECT candle_time,
                           LAG(candle_time) OVER (ORDER BY candle_time) AS prev_time
                    FROM brti_minute_candles
                )
                SELECT
                    COUNT(*),
                    MIN(candle_time),
                    MAX(candle_time),
                    COUNT(*) FILTER (
                        WHERE prev_time IS NOT NULL
                          AND candle_time - prev_time > 90
                    ),
                    COALESCE(SUM(
                        GREATEST(
                            FLOOR((candle_time - prev_time) / 60.0) - 1,
                            0
                        )
                    ) FILTER (WHERE prev_time IS NOT NULL), 0)
                FROM ordered
            """).fetchone()

        count = int(row[0] or 0)
        first_ts = int(row[1]) if row[1] is not None else None
        last_ts = int(row[2]) if row[2] is not None else None
        span_hours = (
            round((last_ts - first_ts) / 3600.0, 2)
            if first_ts is not None and last_ts is not None else 0
        )
        return {
            "count": count,
            "first_candle_ts": first_ts,
            "last_candle_ts": last_ts,
            "span_hours": span_hours,
            "gaps_over_90_seconds": int(row[3] or 0),
            "missing_minutes_estimate": int(row[4] or 0),
            "error": None
        }
    except Exception as exc:
        result = dict(empty)
        result["error"] = str(exc)[:180]
        return result


def save_snapshot(data):
    """
    Guarda una predicción BRTI por minuto, independientemente de Kalshi.
    Crea objetivos +1m, +3m, +5m y +15m desde la hora de la señal.
    El resultado de cierre del contrato se registra solo si existe contrato.
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
        if not ticker or ticker == "SIN_CONTRATO":
            ticker = None

        try:
            market_close_ts = float(data.get("market_close_ts"))
            if (
                not math.isfinite(market_close_ts)
                or market_close_ts <= signal_ts
                or ticker is None
            ):
                market_close_ts = None
        except (TypeError, ValueError):
            market_close_ts = None

        minute_bucket = int(signal_ts // 60)
        snapshot_kind = str(data.get("snapshot_kind") or "LEGACY").strip().upper()
        # Mantiene separados el pronóstico fijo de 15m y la señal al cierre
        # de Kalshi, incluso si ambos se generan en el mismo minuto.
        snapshot_key = f"BRTI:{minute_bucket}:{ticker or 'GLOBAL'}:{snapshot_kind}"

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
                conn.execute("""
                    INSERT INTO btc_signal_outcomes (
                        snapshot_id, horizon_seconds, target_ts
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (snapshot_id, horizon_seconds)
                    DO NOTHING
                """, (snapshot_id, horizon, signal_ts + horizon))

            # El resultado al cierre de Kalshi es independiente del horizonte +15m.
            if ticker is not None and market_close_ts is not None:
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
                  AND %s <= o.target_ts + 60 
          """, (price, price, price, price, ts, ts, ts, ts))

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_pattern_learning_diagnostics():
    """Diagnóstico del aprendizaje con resultados BRTI resueltos a +900s."""
    empty = {
        "official_total": 0,
        "kalshi_api_total": 0,
        "manual_total": 0,
        "snapshots_total": 0,
        "snapshots_with_ticker": 0,
        "snapshots_directional_with_close": 0,
        "eligible_contracts": 0,
        "eligible_with_pattern": 0,
        "eligible_without_pattern": 0,
        "patterns": [],
        "pattern_learning_source": "KALSHI_CONTRACT_CLOSE_BRTI_AVERAGE",
        "pattern_outcomes_15m": 0,
        "error": None
    }
    try:
        with _connect() as conn:
            totals = conn.execute("""
                SELECT
                    (SELECT COUNT(*) FROM btc_official_market_results),
                    (SELECT COUNT(*) FROM btc_official_market_results
                     WHERE source = 'kalshi_api'),
                    (SELECT COUNT(*) FROM btc_official_market_results
                     WHERE source <> 'kalshi_api'),
                    (SELECT COUNT(*) FROM btc_signal_snapshots),
                    (SELECT COUNT(*) FROM btc_signal_snapshots
                     WHERE market_ticker IS NOT NULL),
                    (SELECT COUNT(*) FROM btc_signal_snapshots
                     WHERE market_ticker IS NOT NULL
                       AND market_close_ts IS NOT NULL
                       AND direction IN ('ALCISTA', 'BAJISTA')
                       AND signal_ts <= market_close_ts)
            """).fetchone()

            eligible = conn.execute("""
                WITH last_contract_signal AS (
                    SELECT DISTINCT ON (s.market_ticker)
                        s.market_ticker, s.pattern
                    FROM btc_signal_snapshots AS s
                    JOIN btc_official_market_results AS o
                      ON o.market_ticker = s.market_ticker
                    WHERE s.market_ticker IS NOT NULL
                      AND s.market_close_ts IS NOT NULL
                      AND s.direction IN ('ALCISTA', 'BAJISTA')
                      AND s.signal_ts <= s.market_close_ts
                      AND o.source = 'kalshi_api'
                      AND o.official_result IN ('SI', 'NO')
                    ORDER BY s.market_ticker, s.signal_ts DESC, s.id DESC
                )
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE pattern IS NOT NULL AND BTRIM(pattern) <> ''
                    ),
                    COUNT(*) FILTER (
                        WHERE pattern IS NULL OR BTRIM(pattern) = ''
                    )
                FROM last_contract_signal
            """).fetchone()

            patterns = conn.execute("""
                WITH contract_patterns AS (
                    SELECT DISTINCT ON (
                        s.market_ticker,
                        BTRIM(COALESCE(s.pattern, ''))
                    )
                        s.market_ticker,
                        BTRIM(COALESCE(s.pattern, '')) AS pattern,
                        o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = -1
                     AND o.future_price IS NOT NULL
                    WHERE s.market_ticker IS NOT NULL
                      AND BTRIM(COALESCE(s.pattern, '')) <> ''
                      AND o.result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ORDER BY s.market_ticker,
                             BTRIM(COALESCE(s.pattern, '')),
                             s.signal_ts DESC, s.id DESC
                )
                SELECT pattern, COUNT(*) FILTER (
                    WHERE result IN ('ALCISTA', 'BAJISTA')
                )
                FROM contract_patterns
                GROUP BY pattern
                ORDER BY COUNT(*) FILTER (
                    WHERE result IN ('ALCISTA', 'BAJISTA')
                ) DESC
                LIMIT 10
            """).fetchall()
            pattern_outcomes_15m = conn.execute("""
                SELECT COUNT(*)
                FROM btc_signal_outcomes
                WHERE horizon_seconds = 900
                  AND future_price IS NOT NULL
            """).fetchone()[0]

        return {
            "official_total": int(totals[0] or 0),
            "kalshi_api_total": int(totals[1] or 0),
            "manual_total": int(totals[2] or 0),
            "snapshots_total": int(totals[3] or 0),
            "snapshots_with_ticker": int(totals[4] or 0),
            "snapshots_directional_with_close": int(totals[5] or 0),
            "eligible_contracts": int(eligible[0] or 0),
            "eligible_with_pattern": int(eligible[1] or 0),
            "eligible_without_pattern": int(eligible[2] or 0),
            "patterns": [
                {"pattern": str(row[0]), "samples": int(row[1])}
                for row in patterns
            ],
            "pattern_learning_source": "KALSHI_CONTRACT_CLOSE_BRTI_AVERAGE",
            "pattern_contract_close_outcomes": int(pattern_outcomes_15m or 0),
            "error": None
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}


def get_pattern_stats(pattern):
    """
    Aprende qué resultado de contrato Kalshi se asocia a cada patrón.
    Cada contrato aporta como máximo una muestra por patrón, usando el
    resultado BRTI medio de los 60 segundos previos al cierre frente al strike.
    """
    global _db_status
    empty = {
        "samples": 0,
        "bullish_pct": None,
        "bearish_pct": None,
        "source": "KALSHI_CONTRACT_CLOSE_BRTI_AVERAGE"
    }
    try:
        if not pattern:
            return empty
        with _connect() as conn:
            row = conn.execute("""
                WITH contract_patterns AS (
                    SELECT DISTINCT ON (
                        s.market_ticker,
                        BTRIM(COALESCE(s.pattern, ''))
                    )
                        s.market_ticker,
                        BTRIM(COALESCE(s.pattern, '')) AS pattern,
                        o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = -1
                     AND o.future_price IS NOT NULL
                    WHERE s.market_ticker IS NOT NULL
                      AND BTRIM(COALESCE(s.pattern, '')) <> ''
                      AND o.result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ORDER BY s.market_ticker,
                             BTRIM(COALESCE(s.pattern, '')),
                             s.signal_ts DESC, s.id DESC
                )
                SELECT
                    COUNT(*) FILTER (
                        WHERE result IN ('ALCISTA', 'BAJISTA')
                    ),
                    COUNT(*) FILTER (WHERE result = 'ALCISTA'),
                    COUNT(*) FILTER (WHERE result = 'BAJISTA')
                FROM contract_patterns
                WHERE pattern = BTRIM(%s)
            """, (str(pattern),)).fetchone()
        total, bullish, bearish = row
        total = int(total or 0)
        bullish = int(bullish or 0)
        bearish = int(bearish or 0)
        _db_status = "CONECTADA"
        if total == 0:
            return empty
        return {
            "samples": total,
            "bullish_pct": round(bullish * 100 / total, 1),
            "bearish_pct": round(bearish * 100 / total, 1),
            "source": "KALSHI_CONTRACT_CLOSE_BRTI_AVERAGE"
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


def get_15m_probability_calibration():
    """
    Precisión empírica por bandas de confianza, usando una sola señal BRTI
    por contrato y únicamente pronósticos guardados con horizonte real de 900s.
    """
    try:
        with _connect() as conn:
            rows = conn.execute("""
                WITH one_per_contract AS (
                    SELECT DISTINCT ON (s.market_ticker)
                        s.market_ticker,
                        s.direction,
                        COALESCE(
                            NULLIF(s.features->>'raw_bullish_score', '')::DOUBLE PRECISION,
                            s.bullish_score
                        ) AS bullish_score,
                        COALESCE(
                            NULLIF(s.features->>'raw_bearish_score', '')::DOUBLE PRECISION,
                            s.bearish_score
                        ) AS bearish_score,
                        o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = 900
                     AND o.future_price IS NOT NULL
                    WHERE s.market_ticker IS NOT NULL
                      AND s.direction IN ('ALCISTA', 'BAJISTA')
                      AND s.features->>'forecast_horizon_seconds' = '900'
                      AND (
                        UPPER(COALESCE(s.features->>'signal_source', '')) LIKE '%BRTI%'
                        OR UPPER(COALESCE(s.features->>'direction_method', '')) LIKE '%BRTI%'
                      )
                      AND o.result IN ('ALCISTA', 'BAJISTA')
                    ORDER BY s.market_ticker, s.signal_ts ASC, s.id ASC
                ),
                scored AS (
                    SELECT
                        (FLOOR((GREATEST(bullish_score, bearish_score) - 50) / 5.0) * 5 + 50)::INTEGER AS band_floor,
                        CASE WHEN direction = result THEN 1 ELSE 0 END AS correct
                    FROM one_per_contract
                    WHERE bullish_score IS NOT NULL AND bearish_score IS NOT NULL
                )
                SELECT band_floor, COUNT(*) AS samples, SUM(correct) AS correct
                FROM scored
                GROUP BY band_floor
                ORDER BY band_floor
            """).fetchall()
        return [
            {
                "band_floor": int(row[0]),
                "samples": int(row[1]),
                "correct": int(row[2])
            }
            for row in rows
        ]
    except Exception:
        return []


def get_15m_prediction_accuracy():
    """Precisión direccional a 15 minutos: evalúa cada señal individual al cumplirse +900 segundos."""
    empty = {
        "predictions_evaluated": 0, "correct": 0, "incorrect": 0,
        "no_change": 0, "indecisive_contracts": 0,
        "contracts_evaluated": 0, "accuracy_pct": None,
        "measurement": "SOLO PRONÓSTICOS CON HORIZONTE REAL DE 900S"
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
                 AND s.features->>'forecast_horizon_seconds' = '900'
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
            "measurement": "SOLO PRONÓSTICOS CON HORIZONTE REAL DE 900S"
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
                     AND s.features->>'forecast_horizon_seconds' = '900'
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
                "measurement": "SOLO PRONÓSTICOS CON HORIZONTE REAL DE 900S · POR FUENTE"
            })
        return {
            "sources": result,
            "error": None,
            "measurement": "SOLO PRONÓSTICOS CON HORIZONTE REAL DE 900S · POR FUENTE"
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
                WHERE s.features->>'forecast_horizon_seconds' = '900'
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
                     FROM btc_signal_outcomes AS o
                     JOIN btc_signal_snapshots AS s ON s.id = o.snapshot_id
                     WHERE o.horizon_seconds = 900
                       AND o.future_price IS NOT NULL
                       AND s.features->>'forecast_horizon_seconds' = '900'),

                    (SELECT COUNT(*)
                     FROM btc_signal_outcomes AS o
                     JOIN btc_signal_snapshots AS s ON s.id = o.snapshot_id
                     WHERE o.horizon_seconds = 900
                       AND o.future_price IS NULL
                       AND s.features->>'forecast_horizon_seconds' = '900'),

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

def get_last_contract_signal(market_ticker):
    """Última dirección predictiva válida guardada antes del cierre del contrato."""
    ticker = str(market_ticker or "").strip()
    if not ticker:
        return None
    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT direction
                FROM btc_signal_snapshots
                WHERE market_ticker = %s
                  AND direction IN ('ALCISTA', 'BAJISTA')
                  AND market_close_ts IS NOT NULL
                  AND signal_ts <= market_close_ts
                ORDER BY signal_ts DESC, id DESC
                LIMIT 1
            """, (ticker,)).fetchone()
        return row[0] if row else None
    except Exception as exc:
        global _db_status
        _db_status = "ERROR: " + str(exc)[:180]
        return None


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
                    source,
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

        correct = sum(1 for row in rows if row[4])
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
                    "source": row[3],
                    "evaluation": (
                        "ACIERTO" if row[4] else "FALLO"
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
