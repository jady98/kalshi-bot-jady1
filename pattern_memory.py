import os
import time
import threading
import math

import psycopg
from psycopg.types.json import Jsonb


_lock = threading.RLock()
_last_tick_second = 0
_tick_save_count = 0
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

            # Muestras BRTI espaciadas por 5 segundos para reconstruir
            # ventanas de 1 y 3 minutos después de reinicios de Render.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS brti_price_ticks (
                    tick_second BIGINT PRIMARY KEY,
                    tick_ts DOUBLE PRECISION NOT NULL,
                    price DOUBLE PRECISION NOT NULL
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
                    resolution_delay_seconds DOUBLE PRECISION,
                    UNIQUE(snapshot_id, horizon_seconds)
                )
            """)

            conn.execute("""
                ALTER TABLE btc_signal_outcomes
                ADD COLUMN IF NOT EXISTS resolution_delay_seconds DOUBLE PRECISION;

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

            conn.execute("""
                CREATE TABLE IF NOT EXISTS btc_model_comparison_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    market_ticker TEXT NOT NULL,
                    signal_ts DOUBLE PRECISION NOT NULL,
                    market_close_ts DOUBLE PRECISION NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    market_target DOUBLE PRECISION,
                    main_direction TEXT,
                    gbm_yes_probability DOUBLE PRECISION,
                    UNIQUE (market_ticker, signal_ts)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS btc_paper_trades (
                    market_ticker TEXT PRIMARY KEY,
                    side TEXT NOT NULL CHECK (side IN ('YES', 'NO')),
                    direction TEXT NOT NULL CHECK (direction IN ('ALCISTA', 'BAJISTA')),
                    entry_ts DOUBLE PRECISION NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL CHECK (entry_price > 0 AND entry_price < 1),
                    estimated_fee DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (estimated_fee >= 0),
                    total_cost DOUBLE PRECISION NOT NULL CHECK (total_cost > 0),
                    model_probability DOUBLE PRECISION NOT NULL CHECK (model_probability >= 0 AND model_probability <= 1),
                    estimated_ev DOUBLE PRECISION,
                    official_result TEXT CHECK (official_result IN ('SI', 'NO')),
                    resolved_ts DOUBLE PRECISION,
                    pnl DOUBLE PRECISION,
                    status TEXT NOT NULL DEFAULT 'PENDIENTE'
                        CHECK (status IN ('PENDIENTE', 'GANADA', 'PERDIDA', 'ANULADA'))
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS btc_manual_positions (
                    market_ticker TEXT PRIMARY KEY,
                    side TEXT NOT NULL CHECK (side IN ('YES', 'NO')),
                    entry_price DOUBLE PRECISION CHECK (entry_price IS NULL OR (entry_price > 0 AND entry_price < 1)),
                    entry_ts DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDIENTE'
                        CHECK (status IN ('PENDIENTE', 'GANADA', 'PERDIDA', 'ANULADA')),
                    official_result TEXT CHECK (official_result IN ('SI', 'NO')),
                    pnl_per_contract DOUBLE PRECISION,
                    resolved_ts DOUBLE PRECISION
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_btc_manual_positions_status
                ON btc_manual_positions(status, entry_ts DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_btc_paper_trades_status
                ON btc_paper_trades(status, entry_ts DESC)
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


def rebuild_brti_candles_from_ticks():
    """
    Reconstruye únicamente minutos ausentes usando muestras BRTI reales
    persistidas cada cinco segundos. No genera precios para minutos sin ticks.
    """
    global _db_status
    try:
        with _connect() as conn:
            row = conn.execute("""
                WITH minute_samples AS (
                    SELECT
                        FLOOR(tick_ts / 60.0)::BIGINT * 60 AS candle_time,
                        (ARRAY_AGG(price ORDER BY tick_ts))[1] AS open_price,
                        MAX(price) AS high_price,
                        MIN(price) AS low_price,
                        (ARRAY_AGG(price ORDER BY tick_ts DESC))[1] AS close_price,
                        MIN(tick_ts) AS open_ts,
                        MAX(tick_ts) AS close_ts,
                        COUNT(*) AS sample_count
                    FROM brti_price_ticks
                    WHERE tick_ts > EXTRACT(EPOCH FROM NOW()) - 259200
                    GROUP BY FLOOR(tick_ts / 60.0)::BIGINT * 60
                    HAVING COUNT(*) >= 2
                ),
                inserted AS (
                    INSERT INTO brti_minute_candles
                        (candle_time, open_price, high_price, low_price,
                         close_price, volume, open_ts, close_ts)
                    SELECT candle_time, open_price, high_price, low_price,
                           close_price, 0, open_ts, close_ts
                    FROM minute_samples
                    ON CONFLICT (candle_time) DO NOTHING
                    RETURNING candle_time
                )
                SELECT COUNT(*) FROM inserted
            """).fetchone()
        _db_status = "CONECTADA"
        return int(row[0] or 0)
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return 0


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


def get_brti_ticks(limit=3600):
    """Restaura muestras BRTI recientes, en orden cronológico."""
    global _db_status
    try:
        limit = max(1, min(int(limit), 10000))
        with _connect() as conn:
            rows = conn.execute("""
                SELECT tick_ts, price
                FROM brti_price_ticks
                ORDER BY tick_second DESC
                LIMIT %s
            """, (limit,)).fetchall()
        _db_status = "CONECTADA"
        return [(float(row[0]), float(row[1])) for row in reversed(rows)]
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
        "tick_samples_72h": 0,
        "first_tick_ts_72h": None,
        "last_tick_ts_72h": None,
        "tick_minutes_rebuildable_72h": 0,
        "missing_candles_rebuildable_72h": 0,
        "error": None
    }
    try:
        with _connect() as conn:
            row = conn.execute("""
                WITH ordered AS (
                    SELECT candle_time,
                           LAG(candle_time) OVER (ORDER BY candle_time) AS prev_time
                    FROM brti_minute_candles
                ),
                tick_minutes AS (
                    SELECT
                        FLOOR(tick_ts / 60.0)::BIGINT * 60 AS candle_time,
                        COUNT(*) AS sample_count
                    FROM brti_price_ticks
                    WHERE tick_ts > EXTRACT(EPOCH FROM NOW()) - 259200
                    GROUP BY FLOOR(tick_ts / 60.0)::BIGINT * 60
                    HAVING COUNT(*) >= 2
                )
                SELECT
                    (SELECT COUNT(*) FROM ordered),
                    (SELECT MIN(candle_time) FROM ordered),
                    (SELECT MAX(candle_time) FROM ordered),
                    (SELECT COUNT(*) FROM ordered
                     WHERE prev_time IS NOT NULL AND candle_time - prev_time > 90),
                    (SELECT COALESCE(SUM(GREATEST(
                        FLOOR((candle_time - prev_time) / 60.0) - 1, 0
                    )), 0) FROM ordered WHERE prev_time IS NOT NULL),
                    (SELECT COUNT(*) FROM brti_price_ticks
                     WHERE tick_ts > EXTRACT(EPOCH FROM NOW()) - 259200),
                    (SELECT MIN(tick_ts) FROM brti_price_ticks
                     WHERE tick_ts > EXTRACT(EPOCH FROM NOW()) - 259200),
                    (SELECT MAX(tick_ts) FROM brti_price_ticks
                     WHERE tick_ts > EXTRACT(EPOCH FROM NOW()) - 259200),
                    (SELECT COUNT(*) FROM tick_minutes),
                    (SELECT COUNT(*) FROM tick_minutes tm
                     WHERE NOT EXISTS (
                         SELECT 1 FROM brti_minute_candles c
                         WHERE c.candle_time = tm.candle_time
                     ))

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
            "tick_samples_72h": int(row[5] or 0),
            "first_tick_ts_72h": int(row[6]) if row[6] is not None else None,
            "last_tick_ts_72h": int(row[7]) if row[7] is not None else None,
            "tick_minutes_rebuildable_72h": int(row[8] or 0),
            "missing_candles_rebuildable_72h": int(row[9] or 0),
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
    Persiste una muestra BRTI cada cinco segundos y resuelve objetivos.
    """
    global _last_tick_second, _db_status, _tick_save_count

    try:
        price = float(price)
        ts = float(timestamp if timestamp is not None else time.time())

        if not math.isfinite(price) or not math.isfinite(ts):
            return False

        if price <= 0 or ts <= 0 or ts > time.time() + 2:
            return False

        second = int(ts)

        # No adelantar el marcador de persistencia antes de confirmar
        # PostgreSQL: si la escritura falla, la siguiente muestra puede reintentar
        # sin esperar otros cinco segundos. La clave por segundo hace idempotente
        # una posible escritura repetida.
        with _lock:
            if second < _last_tick_second + 5:
                return False

        with _connect() as conn:
            conn.execute("""
                INSERT INTO brti_price_ticks (tick_second, tick_ts, price)
                VALUES (%s, %s, %s)
                ON CONFLICT (tick_second) DO UPDATE SET
                    tick_ts = EXCLUDED.tick_ts,
                    price = EXCLUDED.price
            """, (second, ts, price))

            conn.execute("""
                UPDATE btc_signal_outcomes AS o
                SET future_price = %s,
                    change_pct = ((%s / s.price) - 1) * 100,
                    result = CASE
                        WHEN %s - o.target_ts > 60 THEN
                            CASE
                                WHEN ((%s / s.price) - 1) * 100 > 0.01
                                    THEN 'TARDIO_ALCISTA'
                                WHEN ((%s / s.price) - 1) * 100 < -0.01
                                    THEN 'TARDIO_BAJISTA'
                                ELSE 'TARDIO_SIN_CAMBIO'
                            END
                        WHEN ((%s / s.price) - 1) * 100 > 0.01
                            THEN 'ALCISTA'
                        WHEN ((%s / s.price) - 1) * 100 < -0.01
                            THEN 'BAJISTA'
                        ELSE 'SIN CAMBIO'
                    END,
                    resolved_ts = %s,
                    resolution_delay_seconds = GREATEST(0, %s - o.target_ts)
                FROM btc_signal_snapshots AS s
                WHERE o.snapshot_id = s.id
                  AND o.future_price IS NULL
                  AND o.horizon_seconds > 0
                  AND o.target_ts <= %s
                  AND %s > s.signal_ts
          """, (price, price, ts, price, price, price, price, ts, ts, ts, ts))

        with _lock:
            _last_tick_second = max(_last_tick_second, second)
            _tick_save_count += 1
            prune_ticks = (_tick_save_count % 1000 == 0)
        if prune_ticks:
            with _connect() as conn:
                conn.execute(
                    "DELETE FROM brti_price_ticks WHERE tick_ts < %s",
                    (ts - 259200,)
                )

        _db_status = "CONECTADA"
        return True

    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False



def get_resolution_diagnostics():
    """Audita la migración y los resultados tardíos sin alterar métricas predictivas."""
    empty = {
        "schema_ready": False, "late_total": 0, "late_15m_total": 0,
        "timely_total": 0, "pending_over_60s": 0,
        "average_delay_seconds": None, "max_delay_seconds": None,
        "late_labels": [], "recent_late": [], "error": None
    }
    try:
        with _connect() as conn:
            schema = conn.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'btc_signal_outcomes'
                      AND column_name = 'resolution_delay_seconds'
                )
            """).fetchone()[0]
            if not schema:
                return {**empty, "error": "Falta la columna resolution_delay_seconds"}

            totals = conn.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE result IN (
                        'TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO'
                    )),
                    COUNT(*) FILTER (WHERE horizon_seconds = 900 AND result IN (
                        'TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO'
                    )),
                    COUNT(*) FILTER (
                        WHERE future_price IS NOT NULL
                          AND resolution_delay_seconds <= 60
                    ),
                    COUNT(*) FILTER (
                        WHERE future_price IS NULL AND horizon_seconds > 0
                          AND target_ts < EXTRACT(EPOCH FROM NOW()) - 60
                    ),
                    AVG(resolution_delay_seconds) FILTER (WHERE future_price IS NOT NULL),
                    MAX(resolution_delay_seconds) FILTER (WHERE future_price IS NOT NULL)
                FROM btc_signal_outcomes
                WHERE horizon_seconds > 0
            """).fetchone()

            labels = conn.execute("""
                SELECT result, COUNT(*)
                FROM btc_signal_outcomes
                WHERE result IN ('TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO')
                GROUP BY result ORDER BY result
            """).fetchall()

            recent = conn.execute("""
                SELECT s.market_ticker, s.pattern, o.horizon_seconds, o.result,
                       o.resolution_delay_seconds, o.target_ts, o.resolved_ts
                FROM btc_signal_outcomes AS o
                JOIN btc_signal_snapshots AS s ON s.id = o.snapshot_id
                WHERE o.result IN ('TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO')
                ORDER BY o.resolved_ts DESC
                LIMIT 10
            """).fetchall()

        return {
            "schema_ready": True,
            "late_total": int(totals[0] or 0),
            "late_15m_total": int(totals[1] or 0),
            "timely_total": int(totals[2] or 0),
            "pending_over_60s": int(totals[3] or 0),
            "average_delay_seconds": round(float(totals[4]), 2) if totals[4] is not None else None,
            "max_delay_seconds": round(float(totals[5]), 2) if totals[5] is not None else None,
            "late_labels": [{"result": str(r[0]), "count": int(r[1])} for r in labels],
            "recent_late": [
                {
                    "market_ticker": r[0], "pattern": r[1],
                    "horizon_seconds": int(r[2]), "result": r[3],
                    "resolution_delay_seconds": float(r[4]) if r[4] is not None else None,
                    "target_ts": float(r[5]) if r[5] is not None else None,
                    "resolved_ts": float(r[6]) if r[6] is not None else None
                } for r in recent
            ],
            "error": None
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}

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
                "indecisive_predictions": indecisive,
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
                    ORDER BY s.market_ticker, s.signal_ts DESC, s.id DESC
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
    """Precisión direccional a 15 minutos; SIN CAMBIO cuenta como fallo y los tardíos se excluyen."""
    empty = {
        "predictions_evaluated": 0, "correct": 0, "incorrect": 0,
        "no_change": 0, "indecisive_predictions": 0,
        "contracts_evaluated": 0, "indecisive_contracts": 0,
        "late_predictions_excluded": 0, "accuracy_pct": None,
        "measurement": "SEÑALES A +900S; SIN CAMBIO CUENTA COMO FALLO; TARDIAS EXCLUIDAS"
    }
    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND (
                              (result IN ('ALCISTA', 'BAJISTA') AND direction <> result)
                              OR result = 'SIN CAMBIO'
                          )
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    ),
                    COUNT(DISTINCT COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT)) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ),
                    COUNT(DISTINCT COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT)) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO')
                    )
                FROM btc_signal_snapshots AS s
                JOIN btc_signal_outcomes AS o
                  ON o.snapshot_id = s.id
                 AND o.horizon_seconds = 900
                 AND o.future_price IS NOT NULL
                WHERE s.features->>'forecast_horizon_seconds' = '900'
            """).fetchone()
        evaluated, correct, incorrect, no_change, indecisive, contracts, indecisive_contracts, late = row
        correct, incorrect = int(correct or 0), int(incorrect or 0)
        total = correct + incorrect
        return {
            "predictions_evaluated": int(evaluated or 0),
            "correct": correct,
            "incorrect": incorrect,
            "no_change": int(no_change or 0),
            "indecisive_predictions": int(indecisive or 0),
            "contracts_evaluated": int(contracts or 0),
            "indecisive_contracts": int(indecisive_contracts or 0),
            "late_predictions_excluded": int(late or 0),
            "accuracy_pct": round(correct * 100 / total, 1) if total else None,
            "measurement": "SEÑALES A +900S; SIN CAMBIO CUENTA COMO FALLO; TARDIAS EXCLUIDAS"
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}




def get_15m_prediction_accuracy_by_source():
    """Precisión a 15 minutos por fuente; SIN CAMBIO cuenta como fallo y tardíos se excluyen."""
    try:
        with _connect() as conn:
            rows = conn.execute("""
                WITH candidates AS (
                    SELECT s.market_ticker, s.snapshot_key, s.id, s.direction, o.result,
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
                          AND result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND (
                              (result IN ('ALCISTA', 'BAJISTA') AND direction <> result)
                              OR result = 'SIN CAMBIO'
                          )
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    ),
                    COUNT(DISTINCT COALESCE(NULLIF(market_ticker, ''), NULLIF(snapshot_key, ''), id::TEXT)) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ),
                    COUNT(DISTINCT COALESCE(NULLIF(market_ticker, ''), NULLIF(snapshot_key, ''), id::TEXT)) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO')
                    )
                FROM candidates
                GROUP BY source
                ORDER BY source
            """).fetchall()
        result = []
        for source, evaluated, correct, incorrect, no_change, indecisive, contracts, indecisive_contracts, late in rows:
            correct, incorrect = int(correct or 0), int(incorrect or 0)
            total = correct + incorrect
            result.append({
                "source": source,
                "predictions_evaluated": int(evaluated or 0),
                "contracts_evaluated": int(contracts or 0),
                "correct": correct,
                "incorrect": incorrect,
                "no_change": int(no_change or 0),
                "indecisive_predictions": int(indecisive or 0),
                "indecisive_contracts": int(indecisive_contracts or 0),
                "late_predictions_excluded": int(late or 0),
                "accuracy_pct": round(correct * 100 / total, 1) if total else None,
                "measurement": "SEÑALES A +900S; SIN CAMBIO CUENTA COMO FALLO; TARDIAS EXCLUIDAS · POR FUENTE"
            })
        return {
            "sources": result,
            "error": None,
            "measurement": "SEÑALES A +900S; SIN CAMBIO CUENTA COMO FALLO; TARDIAS EXCLUIDAS · POR FUENTE"
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



def save_model_comparison_snapshot(
    market_ticker, signal_ts, market_close_ts, price,
    market_target, main_direction, gbm_yes_probability
):
    """Guarda en paralelo las dos predicciones, sin alterar la señal principal."""
    global _db_status
    ticker = str(market_ticker or "").strip()
    direction = str(main_direction or "").strip().upper()
    try:
        signal_ts = float(signal_ts)
        # Una sola muestra por contrato y minuto, aunque get_data se ejecute varias veces.
        signal_ts = float(int(signal_ts // 60) * 60)
        market_close_ts = float(market_close_ts)
        price = float(price)
        target = float(market_target) if market_target is not None else None
        gbm_yes = (
            float(gbm_yes_probability)
            if gbm_yes_probability is not None else None
        )
        if (
            not ticker or signal_ts <= 0 or market_close_ts <= signal_ts
            or price <= 0 or not math.isfinite(price)
            or direction not in ("ALCISTA", "BAJISTA", "INDECISO", "RECOPILANDO")
            or (gbm_yes is not None and (
                not math.isfinite(gbm_yes) or gbm_yes < 0 or gbm_yes > 100
            ))
        ):
            return False
        with _connect() as conn:
            conn.execute("""
                INSERT INTO btc_model_comparison_snapshots (
                    market_ticker, signal_ts, market_close_ts, price,
                    market_target, main_direction, gbm_yes_probability
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_ticker, signal_ts) DO NOTHING
            """, (
                ticker, signal_ts, market_close_ts, price, target,
                direction, gbm_yes
            ))
        _db_status = "CONECTADA"
        return True
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_model_comparison_accuracy():
    """Compara GBM y predictor principal en exactamente la misma captura por contrato.

    Solo incluye contratos con resultado oficial de Kalshi y una captura anterior
    al cierre que contenga ambas predicciones. Así evita comparar instantes distintos.
    """
    empty = {
        "measurement": "MISMO CONTRATO · MISMA CAPTURA · RESULTADO OFICIAL KALSHI",
        "main_predictor": {
            "contracts_evaluated": 0, "correct": 0,
            "incorrect": 0, "accuracy_pct": None
        },
        "gbm": {
            "contracts_evaluated": 0, "correct": 0,
            "incorrect": 0, "accuracy_pct": None,
            "brier_score": None
        },
        "matched_contracts": 0,
        "contracts_with_comparison": 0,
        "results": [],
        "error": None
    }
    try:
        with _connect() as conn:
            rows = conn.execute("""
                SELECT DISTINCT ON (s.market_ticker)
                    s.market_ticker,
                    s.main_direction,
                    s.gbm_yes_probability,
                    o.official_result,
                    s.signal_ts
                FROM btc_model_comparison_snapshots AS s
                JOIN btc_official_market_results AS o
                  ON o.market_ticker = s.market_ticker
                 AND o.source = 'kalshi_api'
                WHERE s.signal_ts <= s.market_close_ts
                  AND s.main_direction IN ('ALCISTA', 'BAJISTA')
                  AND s.gbm_yes_probability IS NOT NULL
                  AND s.gbm_yes_probability >= 0
                  AND s.gbm_yes_probability <= 100
                ORDER BY s.market_ticker, s.signal_ts DESC, s.id DESC
            """).fetchall()

        def actual_direction(row):
            return "ALCISTA" if row[3] == "SI" else "BAJISTA"

        main_correct = 0
        gbm_correct = 0
        brier_total = 0.0
        results = []
        for row in rows:
            ticker, main_direction, gbm_probability, official, signal_ts = row
            gbm_probability = float(gbm_probability)
            gbm_direction = "ALCISTA" if gbm_probability >= 50 else "BAJISTA"
            actual = actual_direction(row)
            main_ok = main_direction == actual
            gbm_ok = gbm_direction == actual
            main_correct += int(main_ok)
            gbm_correct += int(gbm_ok)
            actual_yes = 1.0 if official == "SI" else 0.0
            brier_total += (gbm_probability / 100.0 - actual_yes) ** 2
            results.append({
                "market_ticker": ticker,
                "signal_ts": float(signal_ts),
                "main_direction": main_direction,
                "main_correct": main_ok,
                "gbm_yes_probability": round(gbm_probability, 2),
                "gbm_direction": gbm_direction,
                "gbm_correct": gbm_ok,
                "official_result": official
            })

        results.sort(key=lambda item: item["signal_ts"], reverse=True)
        total = len(rows)
        accuracy_main = round(main_correct * 100 / total, 1) if total else None
        accuracy_gbm = round(gbm_correct * 100 / total, 1) if total else None
        brier = round(brier_total / total, 4) if total else None

        return {
            "measurement": empty["measurement"],
            "main_predictor": {
                "contracts_evaluated": total,
                "correct": main_correct,
                "incorrect": total - main_correct,
                "accuracy_pct": accuracy_main
            },
            "gbm": {
                "contracts_evaluated": total,
                "correct": gbm_correct,
                "incorrect": total - gbm_correct,
                "accuracy_pct": accuracy_gbm,
                "brier_score": brier
            },
            "matched_contracts": total,
            "contracts_with_comparison": total,
            "results": results[:20],
            "error": None
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}

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


def record_paper_trade(
    market_ticker, side, direction, entry_ts, entry_price,
    estimated_fee, total_cost, model_probability_pct, estimated_ev
):
    """Guarda una única entrada simulada por contrato; nunca envía órdenes."""
    global _db_status
    try:
        ticker = str(market_ticker or "").strip()
        side = str(side or "").upper()
        direction = str(direction or "").upper()
        values = [
            float(entry_ts), float(entry_price), float(estimated_fee),
            float(total_cost), float(model_probability_pct) / 100.0,
            float(estimated_ev)
        ]
        entry_ts, price, fee, cost, probability, ev = values
        if (
            not ticker or side not in ("YES", "NO")
            or direction not in ("ALCISTA", "BAJISTA")
            or not all(math.isfinite(v) for v in values)
            or entry_ts <= 0 or not (0 < price < 1)
            or fee < 0 or cost <= 0 or not (0 <= probability <= 1)
            or abs((price + fee) - cost) > 0.011
        ):
            return False
        with _connect() as conn:
            row = conn.execute("""
                INSERT INTO btc_paper_trades (
                    market_ticker, side, direction, entry_ts, entry_price,
                    estimated_fee, total_cost, model_probability, estimated_ev
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_ticker) DO NOTHING
                RETURNING market_ticker
            """, (
                ticker, side, direction, entry_ts, price, fee, cost,
                probability, ev
            )).fetchone()
        _db_status = "CONECTADA"
        return row is not None
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def resolve_paper_trade(market_ticker, official_result):
    """Liquida una entrada simulada usando únicamente el resultado oficial SI/NO."""
    global _db_status
    ticker = str(market_ticker or "").strip()
    result = str(official_result or "").strip().upper()
    if not ticker or result not in ("SI", "NO"):
        return False
    try:
        with _connect() as conn:
            row = conn.execute("""
                UPDATE btc_paper_trades
                SET official_result = %s,
                    resolved_ts = %s,
                    pnl = CASE
                        WHEN side = %s THEN 1.0 - total_cost
                        ELSE -total_cost
                    END,
                    status = CASE
                        WHEN side = %s THEN 'GANADA'
                        ELSE 'PERDIDA'
                    END
                WHERE market_ticker = %s
                  AND status = 'PENDIENTE'
                RETURNING market_ticker
            """, (result, time.time(), result, result, ticker)).fetchone()
        _db_status = "CONECTADA"
        return row is not None
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def void_paper_trade(market_ticker):
    """Marca como anulada una entrada si Kalshi declara void/cancelación."""
    global _db_status
    ticker = str(market_ticker or "").strip()
    if not ticker:
        return False
    try:
        with _connect() as conn:
            row = conn.execute("""
                UPDATE btc_paper_trades
                SET status = 'ANULADA',
                    official_result = NULL,
                    resolved_ts = %s,
                    pnl = 0
                WHERE market_ticker = %s AND status = 'PENDIENTE'
                RETURNING market_ticker
            """, (time.time(), ticker)).fetchone()
        _db_status = "CONECTADA"
        return row is not None
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_paper_trade_stats():
    """Auditoría de rentabilidad simulada neta de comisiones."""
    empty = {
        "measurement": "ENTRADAS CONFIRMADAS SIMULADAS · PRECIO ASK OBSERVADO · RESULTADO OFICIAL KALSHI",
        "trades_total": 0, "resolved": 0, "pending": 0, "voided": 0,
        "wins": 0, "losses": 0, "win_rate_pct": None,
        "realized_pnl": 0.0, "roi_pct": None,
        "average_entry_price": None, "fees_total": 0.0,
        "average_estimated_ev": None, "by_side": [], "error": None
    }
    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status IN ('GANADA', 'PERDIDA')),
                    COUNT(*) FILTER (WHERE status = 'PENDIENTE'),
                    COUNT(*) FILTER (WHERE status = 'ANULADA'),
                    COUNT(*) FILTER (WHERE status = 'GANADA'),
                    COUNT(*) FILTER (WHERE status = 'PERDIDA'),
                    COALESCE(SUM(pnl) FILTER (WHERE status IN ('GANADA', 'PERDIDA')), 0),
                    SUM(total_cost) FILTER (WHERE status IN ('GANADA', 'PERDIDA')),
                    AVG(entry_price),
                    COALESCE(SUM(estimated_fee), 0),
                    AVG(estimated_ev)
                FROM btc_paper_trades
            """).fetchone()
            side_rows = conn.execute("""
                SELECT side, COUNT(*),
                       COUNT(*) FILTER (WHERE status IN ('GANADA', 'PERDIDA')),
                       COUNT(*) FILTER (WHERE status = 'GANADA'),
                       COUNT(*) FILTER (WHERE status = 'PERDIDA'),
                       COALESCE(SUM(pnl) FILTER (WHERE status IN ('GANADA', 'PERDIDA')), 0)
                FROM btc_paper_trades
                GROUP BY side ORDER BY side
            """).fetchall()
        total, resolved, pending, voided, wins, losses = [int(x or 0) for x in row[:6]]
        pnl = float(row[6] or 0)
        invested = float(row[7]) if row[7] is not None else None
        return {
            "measurement": empty["measurement"],
            "trades_total": total, "resolved": resolved, "pending": pending,
            "voided": voided, "wins": wins, "losses": losses,
            "win_rate_pct": round(wins * 100 / resolved, 1) if resolved else None,
            "realized_pnl": round(pnl, 4),
            "roi_pct": round(pnl * 100 / invested, 2) if invested else None,
            "average_entry_price": round(float(row[8]), 4) if row[8] is not None else None,
            "fees_total": round(float(row[9] or 0), 4),
            "average_estimated_ev": round(float(row[10]), 4) if row[10] is not None else None,
            "by_side": [
                {"side": str(r[0]), "trades": int(r[1]), "resolved": int(r[2]),
                 "wins": int(r[3]), "losses": int(r[4]), "realized_pnl": round(float(r[5] or 0), 4)}
                for r in side_rows
            ],
            "error": None
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}


def get_official_market_accuracy():

    empty = {
        "contracts_evaluated": 0,
        "correct": 0,
        "incorrect": 0,
        "accuracy_pct": None,
        "kalshi_api": {
            "contracts_evaluated": 0, "correct": 0,
            "incorrect": 0, "accuracy_pct": None
        },
        "manual_confirmation": {
            "contracts_evaluated": 0, "correct": 0,
            "incorrect": 0, "accuracy_pct": None
        },
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
        api_rows = [row for row in rows if row[3] == "kalshi_api"]
        manual_rows = [row for row in rows if row[3] != "kalshi_api"]
        api_correct = sum(1 for row in api_rows if row[4])
        manual_correct = sum(1 for row in manual_rows if row[4])

        return {
            "contracts_evaluated": total,
            "correct": correct,
            "incorrect": total - correct,
            "accuracy_pct": (
                round(correct * 100 / total, 1)
                if total else None
            ),
            # Separa los resultados confirmados por API de los registrados
            # manualmente para que una confirmación manual no infle el dato oficial.
            "kalshi_api": {
                "contracts_evaluated": len(api_rows),
                "correct": api_correct,
                "incorrect": len(api_rows) - api_correct,
                "accuracy_pct": round(api_correct * 100 / len(api_rows), 1) if api_rows else None
            },
            "manual_confirmation": {
                "contracts_evaluated": len(manual_rows),
                "correct": manual_correct,
                "incorrect": len(manual_rows) - manual_correct,
                "accuracy_pct": round(manual_correct * 100 / len(manual_rows), 1) if manual_rows else None
            },
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


def get_15m_prediction_accuracy_by_contract():
    """Evalúa el último pronóstico 15m resuelto por contrato; SIN CAMBIO es fallo."""
    empty = {
        "predictions_evaluated": 0, "correct": 0, "incorrect": 0,
        "no_change": 0, "indecisive_predictions": 0,
        "contracts_evaluated": 0, "indecisive_contracts": 0,
        "late_predictions_excluded": 0, "accuracy_pct": None,
        "measurement": "ULTIMO PRONOSTICO 15M RESUELTO POR CONTRATO; SIN CAMBIO CUENTA COMO FALLO"
    }
    try:
        with _connect() as conn:
            row = conn.execute("""
                WITH latest AS (
                    SELECT DISTINCT ON (
                        COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT)
                    )
                        COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT) AS contract_key,
                        s.direction,
                        o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = 900
                     AND o.future_price IS NOT NULL
                    WHERE s.features->>'forecast_horizon_seconds' = '900'
                    ORDER BY COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT),
                             s.signal_ts DESC, s.id DESC
                )
                SELECT
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND direction = result
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND (
                              (result IN ('ALCISTA', 'BAJISTA') AND direction <> result)
                              OR result = 'SIN CAMBIO'
                          )
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result = 'SIN CAMBIO'
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    ),
                    COUNT(DISTINCT contract_key) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                    ),
                    COUNT(DISTINCT contract_key) FILTER (
                        WHERE direction IN ('INDECISO', 'RECOPILANDO')
                           OR direction IS NULL
                    ),
                    COUNT(*) FILTER (
                        WHERE direction IN ('ALCISTA', 'BAJISTA')
                          AND result IN ('TARDIO_ALCISTA', 'TARDIO_BAJISTA', 'TARDIO_SIN_CAMBIO')
                    )
                FROM latest
            """).fetchone()
        evaluated, correct, incorrect, no_change, indecisive, contracts, indecisive_contracts, late = row
        correct, incorrect = int(correct or 0), int(incorrect or 0)
        total = correct + incorrect
        return {
            "predictions_evaluated": int(evaluated or 0),
            "correct": correct,
            "incorrect": incorrect,
            "no_change": int(no_change or 0),
            "indecisive_predictions": int(indecisive or 0),
            "contracts_evaluated": int(contracts or 0),
            "indecisive_contracts": int(indecisive_contracts or 0),
            "late_predictions_excluded": int(late or 0),
            "accuracy_pct": round(correct * 100 / total, 1) if total else None,
            "measurement": "ULTIMO PRONOSTICO 15M RESUELTO POR CONTRATO; SIN CAMBIO CUENTA COMO FALLO"
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}




def get_15m_error_diagnostics():
    """Diagnóstico de la última señal resuelta por contrato/ventana a +900s."""
    empty = {"by_pattern": [], "by_confidence": [], "recent_errors": [], "error": None}
    try:
        with _connect() as conn:
            base_cte = """
                WITH latest AS (
                    SELECT DISTINCT ON (
                        COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT)
                    )
                        s.signal_ts, s.market_ticker, s.snapshot_key, s.direction,
                        s.features, s.pattern, s.price,
                        o.future_price, o.change_pct, o.result
                    FROM btc_signal_snapshots AS s
                    JOIN btc_signal_outcomes AS o
                      ON o.snapshot_id = s.id
                     AND o.horizon_seconds = 900
                     AND o.future_price IS NOT NULL
                    WHERE s.features->>'forecast_horizon_seconds' = '900'
                      AND s.direction IN ('ALCISTA', 'BAJISTA')
                      AND o.result IN ('ALCISTA', 'BAJISTA', 'SIN CAMBIO')
                      AND (
                        UPPER(COALESCE(s.features->>'signal_source', '')) LIKE '%BRTI%'
                        OR UPPER(COALESCE(s.features->>'direction_method', '')) LIKE '%BRTI%'
                      )
                    ORDER BY
                        COALESCE(NULLIF(s.market_ticker, ''), NULLIF(s.snapshot_key, ''), s.id::TEXT),
                        s.signal_ts DESC, s.id DESC
                )
            """
            pattern_rows = conn.execute(base_cte + """
                SELECT
                    COALESCE(NULLIF(BTRIM(pattern), ''), 'SIN PATRON') AS pattern,
                    COUNT(*) AS samples,
                    SUM(CASE WHEN direction = result THEN 1 ELSE 0 END) AS correct
                FROM latest
                GROUP BY 1
                ORDER BY COUNT(*) DESC
                LIMIT 12
            """).fetchall()

            confidence_rows = conn.execute(base_cte + """
                SELECT
                    FLOOR((
                        GREATEST(
                            COALESCE(NULLIF(features->>'raw_bullish_score', '')::DOUBLE PRECISION, 50),
                            COALESCE(NULLIF(features->>'raw_bearish_score', '')::DOUBLE PRECISION, 50)
                        ) - 50
                    ) / 5.0) * 5 + 50 AS band_floor,
                    COUNT(*) AS samples,
                    SUM(CASE WHEN direction = result THEN 1 ELSE 0 END) AS correct
                FROM latest
                GROUP BY 1
                ORDER BY 1
            """).fetchall()

            error_rows = conn.execute(base_cte + """
                SELECT
                    signal_ts, market_ticker, direction,
                    GREATEST(
                        COALESCE(NULLIF(features->>'raw_bullish_score', '')::DOUBLE PRECISION, 50),
                        COALESCE(NULLIF(features->>'raw_bearish_score', '')::DOUBLE PRECISION, 50)
                    ) AS confidence,
                    pattern, price, future_price, change_pct, result
                FROM latest
                WHERE direction <> result
                ORDER BY signal_ts DESC
                LIMIT 20
            """).fetchall()

        return {
            "measurement": "ULTIMA SEÑAL BRTI RESUELTA POR CONTRATO/VENTANA · +900S",
            "by_pattern": [
                {"pattern": str(r[0]), "samples": int(r[1]), "correct": int(r[2]),
                 "incorrect": int(r[1]) - int(r[2]),
                 "accuracy_pct": round(int(r[2]) * 100 / int(r[1]), 1) if r[1] else None}
                for r in pattern_rows
            ],
            "by_confidence": [
                {"band_floor": int(r[0]), "samples": int(r[1]), "correct": int(r[2]),
                 "incorrect": int(r[1]) - int(r[2]),
                 "accuracy_pct": round(int(r[2]) * 100 / int(r[1]), 1) if r[1] else None}
                for r in confidence_rows if r[0] is not None
            ],
            "recent_errors": [
                {"signal_ts": float(r[0]), "market_ticker": r[1], "direction": r[2],
                 "confidence_pct": round(float(r[3]), 1) if r[3] is not None else None,
                 "pattern": r[4], "entry_price": float(r[5]),
                 "future_price_15m": float(r[6]),
                 "change_pct_15m": float(r[7]) if r[7] is not None else None,
                 "actual_result": r[8]}
                for r in error_rows
            ],
            "error": None
        }
    except Exception as exc:
        return {**empty, "error": str(exc)[:180]}



def record_manual_position(market_ticker, side, entry_price=None, entry_ts=None):
    """Registra la posición elegida por la usuaria; no envía órdenes a Kalshi."""
    global _db_status
    ticker = str(market_ticker or "").strip()
    side = str(side or "").strip().upper()
    try:
        ts = float(entry_ts if entry_ts is not None else time.time())
        price = None if entry_price in (None, "") else float(entry_price)
        if not ticker or side not in ("YES", "NO") or not math.isfinite(ts) or ts <= 0:
            return False
        if price is not None and (not math.isfinite(price) or not 0 < price < 1):
            return False
        with _connect() as conn:
            row = conn.execute("""
                INSERT INTO btc_manual_positions
                    (market_ticker, side, entry_price, entry_ts)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (market_ticker) DO NOTHING
                RETURNING market_ticker
            """, (ticker, side, price, ts)).fetchone()
        _db_status = "CONECTADA"
        return row is not None
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return False


def get_manual_position(market_ticker):
    """Devuelve la posición manual registrada para un contrato."""
    ticker = str(market_ticker or "").strip()
    if not ticker:
        return None
    try:
        with _connect() as conn:
            row = conn.execute("""
                SELECT market_ticker, side, entry_price, entry_ts, status,
                       official_result, pnl_per_contract
                FROM btc_manual_positions
                WHERE market_ticker = %s
            """, (ticker,)).fetchone()
        if not row:
            return None
        return {
            "ticker": row[0], "side": row[1],
            "entry_price": float(row[2]) if row[2] is not None else None,
            "entry_ts": float(row[3]), "status": row[4],
            "official_result": row[5],
            "pnl_per_contract": float(row[6]) if row[6] is not None else None
        }
    except Exception as exc:
        _db_status = "ERROR: " + str(exc)[:180]
        return None
