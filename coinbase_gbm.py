"""Independent GBM probability cross-check using Coinbase 1-minute volatility.

The contract strike and spot are supplied by the existing BRTI/Kalshi radar;
Coinbase candles are used only to estimate realized volatility. This keeps the
settlement reference aligned with the radar while adding an independent feed.
This module never places orders and never changes the primary signal.
"""
import math


def calculate_coinbase_gbm_baseline(spot, strike, seconds_remaining, candles):
    result = {
        "coinbase_gbm_status": "SIN OBJETIVO",
        "coinbase_gbm_yes_probability": None,
        "coinbase_gbm_no_probability": None,
        "coinbase_gbm_volatility_annualized_pct": None,
        "coinbase_gbm_volatility_samples": 0,
        "coinbase_gbm_source": "VOLATILIDAD COINBASE · REFERENCIA BRTI",
    }
    try:
        spot = float(spot)
        strike = float(strike)
        horizon = max(1.0, min(900.0, float(seconds_remaining)))
        if not all(math.isfinite(v) and v > 0 for v in (spot, strike)):
            return result

        by_minute = {}
        for candle in candles or []:
            try:
                ts = int(float(candle["time"]))
                close = float(candle["close"])
                if ts <= 0 or not math.isfinite(close) or close <= 0:
                    continue
                minute = ts // 60
                prior = by_minute.get(minute)
                if prior is None or ts > prior[0]:
                    by_minute[minute] = (ts, close)
            except (KeyError, TypeError, ValueError, OverflowError):
                continue

        ordered = sorted(by_minute.items())
        returns = []
        for i in range(1, len(ordered)):
            prev_minute, prev_row = ordered[i - 1]
            minute, row = ordered[i]
            if minute != prev_minute + 1:
                continue
            returns.append(math.log(row[1] / prev_row[1]))

        samples = len(returns)
        result["coinbase_gbm_volatility_samples"] = samples
        if samples < 20:
            result["coinbase_gbm_status"] = "HISTORIAL_INSUFICIENTE"
            return result

        mean = sum(returns) / samples
        variance = sum((r - mean) ** 2 for r in returns) / (samples - 1)
        sigma_per_second = math.sqrt(max(0.0, variance)) / math.sqrt(60.0)
        sigma_horizon = sigma_per_second * math.sqrt(horizon)
        annualized = sigma_per_second * math.sqrt(365 * 24 * 60 * 60) * 100
        result["coinbase_gbm_volatility_annualized_pct"] = round(annualized, 2)

        if sigma_horizon <= 1e-10:
            probability_yes = 0.99 if spot >= strike else 0.01
        else:
            z = (math.log(spot / strike) - 0.5 * sigma_horizon ** 2) / sigma_horizon
            probability_yes = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

        probability_yes = max(0.01, min(0.99, probability_yes))
        result.update({
            "coinbase_gbm_status": "OK",
            "coinbase_gbm_yes_probability": round(probability_yes * 100, 1),
            "coinbase_gbm_no_probability": round((1.0 - probability_yes) * 100, 1),
        })
        return result
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        result["coinbase_gbm_status"] = "ERROR_CALCULO"
        return result
