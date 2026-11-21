package com.jady.btcradar

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.view.View
import kotlin.math.max
import kotlin.math.min

class MiniChartView(context: Context) : View(context) {
    private val candles = mutableListOf<Candle>()
    private val wick = Paint(Paint.ANTI_ALIAS_FLAG).apply { strokeWidth = 1.6f }
    private val body = Paint(Paint.ANTI_ALIAS_FLAG)
    private val ema9Paint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.rgb(255, 190, 75); strokeWidth = 2f; style = Paint.Style.STROKE
    }
    private val ema21Paint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.rgb(91, 167, 255); strokeWidth = 2f; style = Paint.Style.STROKE
    }
    private val grid = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.argb(45, 155, 180, 205); strokeWidth = 1f
    }

    fun setCandles(rows: List<Candle>) {
        candles.clear()
        candles.addAll(rows.takeLast(65))
        invalidate()
    }

    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)
        val rows = candles
        val w = width.toFloat()
        val h = height.toFloat()
        if (w <= 0f || h <= 0f) return
        for (i in 1..3) canvas.drawLine(0f, h * i / 4f, w, h * i / 4f, grid)
        if (rows.isEmpty()) return

        val minPrice = rows.minOf { min(it.low, min(it.ema9, it.ema21)) }
        val maxPrice = rows.maxOf { max(it.high, max(it.ema9, it.ema21)) }
        val range = max(0.01f, maxPrice - minPrice)
        val pad = 5f
        fun y(value: Float) = h - pad - ((value - minPrice) / range) * (h - pad * 2f)
        val step = w / rows.size
        val candleWidth = max(2f, step * 0.58f)

        rows.forEachIndexed { i, c ->
            val x = step * (i + 0.5f)
            val rising = c.close >= c.open
            val color = if (rising) Color.rgb(0, 224, 145) else Color.rgb(238, 76, 103)
            wick.color = color
            body.color = color
            canvas.drawLine(x, y(c.high), x, y(c.low), wick)
            val top = min(y(c.open), y(c.close))
            val bottom = max(y(c.open), y(c.close))
            canvas.drawRect(x - candleWidth / 2f, top, x + candleWidth / 2f, max(top + 2f, bottom), body)
        }

        fun drawEma(selector: (Candle) -> Float, paint: Paint) {
            if (rows.size < 2) return
            val path = android.graphics.Path()
            rows.forEachIndexed { i, c ->
                val x = step * (i + 0.5f)
                if (i == 0) path.moveTo(x, y(selector(c))) else path.lineTo(x, y(selector(c)))
            }
            canvas.drawPath(path, paint)
        }
        drawEma({ it.ema9 }, ema9Paint)
        drawEma({ it.ema21 }, ema21Paint)
    }
}