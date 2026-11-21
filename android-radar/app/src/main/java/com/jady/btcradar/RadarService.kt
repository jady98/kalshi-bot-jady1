package com.jady.btcradar

import android.app.*
import android.content.Intent
import android.graphics.Color
import android.graphics.PixelFormat
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.*
import android.provider.Settings
import android.view.*
import android.widget.*
import android.text.SpannableString
import android.text.Spanned
import android.text.style.ForegroundColorSpan

class RadarService : Service() {
    companion object {
        const val ACTION_SHOW = "com.jady.btcradar.SHOW_BUBBLE"
        const val ACTION_HIDE = "com.jady.btcradar.HIDE_BUBBLE"
    }
    private lateinit var wm: WindowManager
    private var panel: LinearLayout? = null
    private val handler = Handler(Looper.getMainLooper())
    private var lastSignal = ""
    @Volatile private var loading = false
    private val task = object : Runnable {
        override fun run() {
            if (!loading) {
                loading = true
                Thread {
                try {
                    val d = RadarData.load()
                    handler.post {
                        panel?.findViewWithTag<TextView>("price")?.text = "$" + "%,.0f".format(d.price)
                        val signalText = "ALCISTA  ${d.up}%     BAJISTA  ${100-d.up}%"
                        val colored = SpannableString(signalText)
                        colored.setSpan(ForegroundColorSpan(Color.rgb(0, 230, 150)), 0, ("ALCISTA  ${d.up}%").length, Spanned.SPAN_EXCLUSIVE_EXCLUSIVE)
                        val bearishStart = signalText.indexOf("BAJISTA")
                        colored.setSpan(ForegroundColorSpan(Color.rgb(255, 86, 112)), bearishStart, signalText.length, Spanned.SPAN_EXCLUSIVE_EXCLUSIVE)
                        panel?.findViewWithTag<TextView>("updown")?.text = colored
                        panel?.findViewWithTag<ProgressBar>("trend")?.progress = d.up
                        panel?.findViewWithTag<MiniChartView>("mini_chart")?.setCandles(d.candles)
                        panel?.findViewWithTag<TextView>("prediction")?.text = "$" + "%,.2f".format(d.prediction)
                        panel?.findViewWithTag<TextView>("timer")?.text = d.time
                        val signal = if (d.up >= 75) "ALCISTA" else if (d.up <= 25) "BAJISTA" else ""
                        if (signal.isNotEmpty() && signal != lastSignal) {
                            val n = Notification.Builder(this@RadarService, "btc")
                                .setSmallIcon(android.R.drawable.ic_dialog_info)
                                .setContentTitle("BTC: $signal")
                                .setContentText("Probabilidad estimada " + (if (signal == "ALCISTA") d.up else 100-d.up) + "%")
                                .build()
                            getSystemService(NotificationManager::class.java).notify(9, n)
                            lastSignal = signal
                        }
                        if (signal.isEmpty()) lastSignal = ""
                    }
                } catch (_: Exception) {
                } finally {
                    loading = false
                }
            }.start()
            }
            handler.postDelayed(this, 1000)
        }
    }

    override fun onCreate() {
        super.onCreate()
        val nm = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= 26) nm.createNotificationChannel(NotificationChannel("btc", "Alertas BTC", NotificationManager.IMPORTANCE_DEFAULT))
        startForeground(1, Notification.Builder(this, "btc").setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle("BTC Radar activo").setContentText("Monitoreando BTC/USD").build())
        if (Settings.canDrawOverlays(this) && getSharedPreferences("radar_settings", MODE_PRIVATE).getBoolean("bubble_enabled", false)) show()
        handler.post(task)
    }

    private fun dp(v: Int) = (v * resources.displayMetrics.density).toInt()
    private fun bg(color: Int, radius: Int = 14, stroke: Int = 0): GradientDrawable =
        GradientDrawable().apply { setColor(color); cornerRadius = dp(radius).toFloat(); if (stroke != 0) setStroke(dp(1), stroke) }

    private fun txt(value: String, size: Float, color: Int = Color.WHITE, bold: Boolean = false, tagName: String? = null): TextView =
        TextView(this).apply {
            text = value; textSize = size; setTextColor(color)
            gravity = Gravity.CENTER_VERTICAL
            setPadding(dp(2), dp(3), dp(2), dp(3))
            if (bold) typeface = Typeface.create("sans-serif", Typeface.BOLD)
            if (tagName != null) tag = tagName
        }

    private fun horizontalBar(tagName: String): ProgressBar =
        ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal).apply {
            max = 100; progress = 50; tag = tagName
            progressDrawable = GradientDrawable().apply {
                shape = GradientDrawable.RECTANGLE
                cornerRadius = dp(7).toFloat()
                setColor(Color.rgb(0, 224, 145))
            }
            progressTintList = android.content.res.ColorStateList.valueOf(Color.rgb(0, 224, 145))
            progressBackgroundTintList = android.content.res.ColorStateList.valueOf(Color.rgb(238, 76, 103))
            minimumHeight = dp(9)
        }

    private fun card(title: String, value: TextView, accent: Int): LinearLayout =
        LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(10), dp(8), dp(10), dp(8))
            background = bg(Color.rgb(25, 38, 56), 13, Color.rgb(49, 69, 91))
            addView(txt(title, 9f, Color.rgb(157, 178, 202), true))
            value.setTextColor(accent)
            value.textSize = 17f
            value.typeface = Typeface.create("sans-serif", Typeface.BOLD)
            addView(value)
        }

    private fun show() {
        if (panel != null) return
        wm = getSystemService(WINDOW_SERVICE) as WindowManager
        val box = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(13), dp(11), dp(13), dp(12))
            background = GradientDrawable(
                GradientDrawable.Orientation.TOP_BOTTOM,
                intArrayOf(Color.rgb(21, 34, 53), Color.rgb(7, 12, 22))
            ).apply {
                cornerRadius = dp(19).toFloat()
                setStroke(dp(1), Color.rgb(67, 105, 139))
            }
            elevation = dp(16).toFloat()
        }

        val header = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            setPadding(dp(3), dp(0), dp(0), dp(7))
        }
        val brand = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            addView(txt("BTC RADAR", 13f, Color.rgb(111, 207, 255), true))
            addView(txt("SEÑAL EN VIVO  •  BTC/USD", 8f, Color.rgb(130, 151, 174)))
        }
        header.addView(brand, LinearLayout.LayoutParams(0, -2, 1f))
        val close = TextView(this).apply {
            text = "×"; textSize = 25f; gravity = Gravity.CENTER
            setTextColor(Color.WHITE); background = bg(Color.rgb(176, 47, 70), 22)
            contentDescription = "Cerrar ventana flotante"
        }
        header.addView(close, LinearLayout.LayoutParams(dp(31), dp(31)))
        close.setOnClickListener {
            getSharedPreferences("radar_settings", MODE_PRIVATE).edit().putBoolean("bubble_enabled", false).apply()
            removePanel()
        }
        box.addView(header)

        val price = txt("—", 21f, Color.WHITE, true, "price")
        val prediction = txt("—", 17f, Color.rgb(255, 210, 104), true, "prediction")
        val priceRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        priceRow.addView(card("PRECIO ACTUAL", price, Color.WHITE), LinearLayout.LayoutParams(0, -2, 1f).apply { marginEnd = dp(6) })
        priceRow.addView(card("PREDICCIÓN AL CIERRE", prediction, Color.rgb(255, 210, 104)), LinearLayout.LayoutParams(0, -2, 1f))
        box.addView(priceRow)

        val signalTitle = txt("PROBABILIDAD DE DIRECCIÓN", 9f, Color.rgb(157, 178, 202), true)
        signalTitle.setPadding(dp(2), dp(10), dp(2), dp(2))
        box.addView(signalTitle)
        box.addView(txt("ALCISTA  —%     BAJISTA  —%", 12f, Color.WHITE, true, "updown"))
        box.addView(horizontalBar("trend"), LinearLayout.LayoutParams(-1, dp(10)).apply { topMargin = dp(5); bottomMargin = dp(8) })

        val miniChart = MiniChartView(this).apply {
            tag = "mini_chart"
            setBackgroundColor(Color.rgb(10, 18, 29))
        }
        box.addView(miniChart, LinearLayout.LayoutParams(-1, dp(94)).apply {
            topMargin = dp(7)
            bottomMargin = dp(4)
        })

        val timerRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            setPadding(dp(10), dp(7), dp(10), dp(7))
            background = bg(Color.rgb(32, 42, 58), 11)
        }
        timerRow.addView(txt("CIERRE EN", 9f, Color.rgb(165, 185, 207), true), LinearLayout.LayoutParams(0, -2, 1f))
        timerRow.addView(txt("15:00", 15f, Color.rgb(255, 211, 107), true, "timer"))
        val timerParams = LinearLayout.LayoutParams(-1, -2)
        timerParams.topMargin = dp(7)
        box.addView(timerRow, timerParams)

        val type = if (Build.VERSION.SDK_INT >= 26) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE
        val lp = WindowManager.LayoutParams(
            dp(300), WindowManager.LayoutParams.WRAP_CONTENT, type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT
        ).apply { gravity = Gravity.TOP or Gravity.END; x = dp(10); y = dp(145) }

        // Tocar la burbuja abre el radar completo.
        box.setOnClickListener {
            val open = Intent(this, MainActivity::class.java).apply { addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP) }
            startActivity(open)
        }

        // Arrastrar desde la cabecera, sin bloquear el toque de la X.
        header.setOnTouchListener(object : View.OnTouchListener {
            var sx = 0f; var sy = 0f; var ox = 0; var oy = 0
            override fun onTouch(v: View, e: MotionEvent): Boolean {
                when (e.action) {
                    MotionEvent.ACTION_DOWN -> { sx=e.rawX; sy=e.rawY; ox=lp.x; oy=lp.y; return true }
                    MotionEvent.ACTION_MOVE -> { lp.x=(ox+sx-e.rawX).toInt(); lp.y=(oy+e.rawY-sy).toInt(); wm.updateViewLayout(box,lp); return true }
                    MotionEvent.ACTION_UP -> {
                        if (kotlin.math.abs(e.rawX-sx) < dp(8) && kotlin.math.abs(e.rawY-sy) < dp(8)) {
                            val open = Intent(this@RadarService, MainActivity::class.java).apply { addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP) }
                            startActivity(open)
                        }
                        return true
                    }
                }
                return false
            }
        })
        wm.addView(box, lp)
        panel = box
    }

    private fun removePanel() {
        try { panel?.let { wm.removeView(it) } } catch (_: Exception) {}
        panel = null
    }

    override fun onStartCommand(i: Intent?, f: Int, id: Int): Int {
        when (i?.action) {
            ACTION_HIDE -> removePanel()
            ACTION_SHOW -> {
                if (Settings.canDrawOverlays(this)) {
                    getSharedPreferences("radar_settings", MODE_PRIVATE).edit().putBoolean("bubble_enabled", true).apply()
                    if (panel == null) try { show() } catch (_: Exception) {}
                }
            }
            else -> {
                if (Settings.canDrawOverlays(this) && getSharedPreferences("radar_settings", MODE_PRIVATE).getBoolean("bubble_enabled", false) && panel == null) try { show() } catch (_: Exception) {}
            }
        }
        return START_STICKY
    }

    override fun onDestroy() {
        handler.removeCallbacks(task)
        removePanel()
        super.onDestroy()
    }
    override fun onBind(i: Intent?): IBinder? = null
}
