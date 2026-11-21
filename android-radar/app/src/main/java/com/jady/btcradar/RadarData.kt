package com.jady.btcradar

import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

data class Candle(val low: Float, val high: Float, val open: Float, val close: Float, val ema9: Float, val ema21: Float)

data class Snapshot(
    val price: Double,
    val prediction: Double,
    val up: Int,
    val time: String,
    val candles: List<Candle>,
    val direction: String,
    val alertType: String,
    val alertMessage: String,
    val alertKey: String,
    val alertTimestamp: Long,
    val entryStage: String,
    val entryDirection: String,
    val manualSide: String,
    val secondsToClose: Int
)

object RadarData {
    fun load(): Snapshot {
        val connection = URL("https://kalshi-bot-jady1.onrender.com/api").openConnection() as HttpURLConnection
        connection.connectTimeout = 8000
        connection.readTimeout = 8000
        connection.setRequestProperty("User-Agent", "BTC-Radar-Android")
        connection.setRequestProperty("Accept", "application/json")
        val data = try {
            JSONObject(connection.inputStream.bufferedReader().use { it.readText() })
        } finally {
            connection.disconnect()
        }

        // Coincide con la gráfica principal: velas Coinbase, precio BRTI y
        // los mismos campos de predicción y probabilidad que actualiza /api.
        val price = data.optDouble("brti_price", data.optDouble("coinbase_price", 0.0))
        val prediction = data.optDouble("predicted_price", price)
        val up = data.optDouble("bullish_score", 50.0).toInt().coerceIn(0, 100)
        val seconds = data.optInt("seconds_to_close", data.optInt("countdown", 0)).coerceAtLeast(0)
        val mm = (seconds / 60) % 60
        val ss = seconds % 60

        val sourceCandles = data.optJSONArray("coinbase_candles")
        val parsedCandles = mutableListOf<Candle>()
        if (sourceCandles != null) {
            val closes = mutableListOf<Double>()
            for (i in 0 until sourceCandles.length()) {
                val row = sourceCandles.optJSONObject(i) ?: continue
                val open = row.optDouble("open", Double.NaN)
                val high = row.optDouble("high", Double.NaN)
                val low = row.optDouble("low", Double.NaN)
                val close = row.optDouble("close", Double.NaN)
                if (listOf(open, high, low, close).all { it.isFinite() && it > 0 } && high >= low) {
                    closes.add(close)
                    fun ema(period: Int): Double {
                        if (closes.isEmpty()) return close
                        val alpha = 2.0 / (period + 1.0)
                        var value = closes.first()
                        for (v in closes.drop(1)) value = alpha * v + (1.0 - alpha) * value
                        return value
                    }
                    parsedCandles.add(Candle(low.toFloat(), high.toFloat(), open.toFloat(), close.toFloat(), ema(9).toFloat(), ema(21).toFloat()))
                }
            }
        }

        val direction = data.optString("direction", "INDECISO")
        val alert = data.optJSONObject("alert") ?: JSONObject()
        val alertType = alert.optString("type", "")
        val alertMessage = alert.optString("message", alert.optString("detail", ""))
        val ticker = alert.optString("ticker", data.optString("entry_ticker", ""))
        val timestamp = alert.optLong("timestamp", 0L)
        val alertKey = listOf(alertType, ticker, timestamp.toString()).joinToString("|")
        val entryStage = data.optString("entry_stage", data.optString("signal_stage", "")).uppercase()
        val entryDirection = data.optString("entry_direction", direction).uppercase()
        val manualPosition = data.optJSONObject("manual_position")
        val manualSide = manualPosition?.optString("side", "")?.uppercase() ?: ""

        return Snapshot(
            price = price,
            prediction = prediction,
            up = up,
            time = "%02d:%02d".format(mm, ss),
            candles = parsedCandles.takeLast(65),
            direction = direction,
            alertType = alertType,
            alertMessage = alertMessage,
            alertKey = alertKey,
            alertTimestamp = timestamp,
            entryStage = entryStage,
            entryDirection = entryDirection,
            manualSide = manualSide,
            secondsToClose = seconds
        )
    }
}