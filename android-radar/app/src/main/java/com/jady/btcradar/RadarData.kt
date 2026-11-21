package com.jady.btcradar

import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

data class Candle(val low: Float, val high: Float, val open: Float, val close: Float, val ema9: Float, val ema21: Float)

data class Snapshot(
    val price: Double,
    val prediction: Double,
    val up: Int,
    val buyers: Int,
    val time: String,
    val direction: String,
    val alertType: String,
    val alertMessage: String,
    val alertKey: String
)

object RadarData {
    fun load(): Snapshot {
        val connection = URL("https://kalshi-bot-jady1.onrender.com/api").openConnection() as HttpURLConnection
        connection.connectTimeout = 12000
        connection.readTimeout = 12000
        connection.setRequestProperty("User-Agent", "BTC-Radar-Android")
        connection.setRequestProperty("Accept", "application/json")
        val data = try {
            JSONObject(connection.inputStream.bufferedReader().use { it.readText() })
        } finally {
            connection.disconnect()
        }

        val price = data.optDouble("brti_price", data.optDouble("coinbase_price", 0.0))
        val prediction = data.optDouble("predicted_price", price)
        val up = data.optDouble("bullish_score", 50.0).toInt().coerceIn(0, 100)
        val direction = data.optString("direction", "INDECISO")
        val seconds = data.optInt("seconds_to_close", data.optInt("countdown", 0))
        val mm = (seconds.coerceAtLeast(0) / 60) % 60
        val ss = seconds.coerceAtLeast(0) % 60

        val alert = data.optJSONObject("alert") ?: JSONObject()
        val alertType = alert.optString("type", "")
        val alertMessage = alert.optString("message", alert.optString("detail", ""))
        val ticker = alert.optString("ticker", data.optString("entry_ticker", ""))
        val timestamp = alert.optLong("timestamp", 0L)
        val alertKey = listOf(alertType, ticker, timestamp.toString()).joinToString("|")

        // El endpoint actual de Render no publica un porcentaje de compradores/vendedores.
        // No se inventa ese dato en la burbuja.
        return Snapshot(
            price = price,
            prediction = prediction,
            up = up,
            buyers = 50,
            time = "%02d:%02d".format(mm, ss),
            direction = direction,
            alertType = alertType,
            alertMessage = alertMessage,
            alertKey = alertKey
        )
    }
}
