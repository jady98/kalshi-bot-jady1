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

class RadarService : Service() {
    private lateinit var wm: WindowManager
    private var panel: LinearLayout? = null
    private val handler = Handler(Looper.getMainLooper())
    private var lastAlertKey = ""

    private fun dp(value: Int): Int = (value * resources.displayMetrics.density).toInt()

    private fun rounded(color: Int, radius: Int, stroke: Int = 0): GradientDrawable =
        GradientDrawable().apply {
            setColor(color)
            cornerRadius = dp(radius).toFloat()
            if (stroke != 0) setStroke(dp(1), stroke)
        }

    private fun label(value: String, size: Float, color: Int, bold: Boolean = false): TextView =
        TextView(this).apply {
            text = value
            textSize = size
            setTextColor(color)
            if (bold) setTypeface(typeface, Typeface.BOLD)
            includeFontPadding = false
        }

    private fun addSpace(parent: LinearLayout, height: Int) {
        parent.addView(View(this), LinearLayout.LayoutParams(1, dp(height)))
    }

    private fun makeBar(up: Int): LinearLayout {
        val bar = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            tag = "bar"
            background = rounded(Color.rgb(40, 48, 61), 8)
            clipToOutline = true
        }
        val bullish = View(this).apply {
            tag = "green"
            setBackgroundColor(Color.rgb(0, 220, 145))
        }
        val bearish = View(this).apply {
            tag = "red"
            setBackgroundColor(Color.rgb(255, 65, 91))
        }
        bar.addView(bullish, LinearLayout.LayoutParams(0, dp(9), up.coerceAtLeast(1).toFloat()))
        bar.addView(bearish, LinearLayout.LayoutParams(0, dp(9), (100 - up).coerceAtLeast(1).toFloat()))
        return bar
    }

    private fun updateBar(up: Int) {
        val bar = panel?.findViewWithTag<LinearLayout>("bar") ?: return
        bar.removeAllViews()
        bar.addView(View(this).apply { setBackgroundColor(Color.rgb(0, 220, 145)) },
            LinearLayout.LayoutParams(0, dp(9), up.coerceAtLeast(1).toFloat()))
        bar.addView(View(this).apply { setBackgroundColor(Color.rgb(255, 65, 91)) },
            LinearLayout.LayoutParams(0, dp(9), (100 - up).coerceAtLeast(1).toFloat()))
    }

    private val task = object : Runnable {
        override fun run() {
            Thread {
                try {
                    val d = RadarData.load()
                    handler.post {
                        panel?.findViewWithTag<TextView>("price")?.text = "$" + "%,.2f".format(d.price)
                        panel?.findViewWithTag<TextView>("up")?.text = "ALCISTA  " + d.up + "%"
                        panel?.findViewWithTag<TextView>("down")?.text = "BAJISTA  " + (100 - d.up) + "%"
                        panel?.findViewWithTag<TextView>("close")?.text = "$" + "%,.2f".format(d.prediction)
                        panel?.findViewWithTag<TextView>("timer")?.text = d.time
                        panel?.findViewWithTag<TextView>("direction")?.text = d.direction.uppercase()
                        updateBar(d.up)

                        val eventType = d.alertType.uppercase()
                        val fresh = d.alertKey.isNotBlank() &&
                            d.alertKey != "||0" &&
                            d.alertKey != lastAlertKey &&
                            eventType in setOf("PREALERTA", "ENTRADA", "MANTENER", "SALIDA", "PROTEGER", "CAMBIO_DIRECCION")
                        if (fresh) {
                            val title = when (eventType) {
                                "PREALERTA" -> "BTC Radar · PREALERTA"
                                "ENTRADA" -> "BTC Radar · SEÑAL CONFIRMADA"
                                "MANTENER" -> "BTC Radar · SEÑAL ACTIVA"
                                "SALIDA", "PROTEGER" -> "BTC Radar · AVISO DE SALIDA"
                                else -> "BTC Radar · CAMBIO DE DIRECCIÓN"
                            }
                            val body = d.alertMessage.ifBlank {
                                "Dirección: " + d.direction + " · Alcista " + d.up + "%"
                            }
                            val openApp = PendingIntent.getActivity(
                                this@RadarService, 10,
                                Intent(this@RadarService, MainActivity::class.java),
                                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
                            )
                            val notification = Notification.Builder(this@RadarService, "btc")
                                .setSmallIcon(android.R.drawable.ic_dialog_info)
                                .setContentTitle(title)
                                .setContentText(body.take(180))
                                .setStyle(Notification.BigTextStyle().bigText(body))
                                .setContentIntent(openApp)
                                .setAutoCancel(true)
                                .build()
                            getSystemService(NotificationManager::class.java).notify(10, notification)
                            lastAlertKey = d.alertKey
                        }
                    }
                } catch (_: Exception) {
                    // Mantiene la última lectura si Render no responde temporalmente.
                }
            }.start()
            handler.postDelayed(this, 5000)
        }
    }

    override fun onCreate() {
        super.onCreate()
        val manager = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= 26) {
            manager.createNotificationChannel(
                NotificationChannel("btc", "Alertas BTC", NotificationManager.IMPORTANCE_HIGH)
            )
        }
        val openApp = PendingIntent.getActivity(
            this, 1, Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val foreground = Notification.Builder(this, "btc")
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .setContentTitle("BTC Radar activo")
            .setContentText("Vigilando las alertas de Render")
            .setContentIntent(openApp)
            .build()
        startForeground(1, foreground)
        if (Settings.canDrawOverlays(this)) show()
        handler.post(task)
    }

    private fun hidePanel() {
        val current = panel ?: return
        try { wm.removeView(current) } catch (_: Exception) {}
        panel = null
    }

    private fun show() {
        if (panel != null) return
        wm = getSystemService(WINDOW_SERVICE) as WindowManager

        val card = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(15), dp(12), dp(15), dp(14))
            background = rounded(Color.rgb(10, 17, 30), 18, Color.rgb(42, 65, 91))
            elevation = dp(12).toFloat()
        }

        val header = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        val title = label("BTC RADAR", 14f, Color.WHITE, true)
        header.addView(title, LinearLayout.LayoutParams(0, dp(30), 1f))
        val close = TextView(this).apply {
            text = "×"
            textSize = 25f
            gravity = Gravity.CENTER
            setTextColor(Color.WHITE)
            background = rounded(Color.rgb(43, 54, 72), 30)
            contentDescription = "Cerrar ventana flotante"
            setOnClickListener { hidePanel() }
        }
        header.addView(close, LinearLayout.LayoutParams(dp(34), dp(34)))
        card.addView(header)

        addSpace(card, 8)
        card.addView(label("PRECIO ACTUAL", 10f, Color.rgb(145, 163, 185), true))
        addSpace(card, 3)
        card.addView(label("BTC —", 24f, Color.WHITE, true).apply { tag = "price" })
        addSpace(card, 12)

        val percentages = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        percentages.addView(label("ALCISTA --%", 12f, Color.rgb(0, 230, 150), true).apply { tag = "up" },
            LinearLayout.LayoutParams(0, dp(22), 1f))
        percentages.addView(label("BAJISTA --%", 12f, Color.rgb(255, 82, 105), true).apply {
            tag = "down"
            gravity = Gravity.END
        }, LinearLayout.LayoutParams(0, dp(22), 1f))
        card.addView(percentages)
        addSpace(card, 4)
        card.addView(makeBar(50), LinearLayout.LayoutParams(-1, dp(9)))
        addSpace(card, 13)

        val closeRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            background = rounded(Color.rgb(20, 31, 48), 12)
            setPadding(dp(11), dp(9), dp(11), dp(9))
        }
        val closeLabels = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        closeLabels.addView(label("PREDICCIÓN AL CIERRE", 9f, Color.rgb(145, 163, 185), true))
        addSpace(closeLabels, 5)
        closeLabels.addView(label("—", 17f, Color.WHITE, true).apply { tag = "close" })
        closeRow.addView(closeLabels, LinearLayout.LayoutParams(0, -2, 1f))
        val timerBox = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            gravity = Gravity.END
        }
        timerBox.addView(label("CIERRE 15M", 9f, Color.rgb(145, 163, 185), true))
        addSpace(timerBox, 5)
        timerBox.addView(label("--:--", 17f, Color.rgb(255, 205, 75), true).apply { tag = "timer" })
        closeRow.addView(timerBox)
        card.addView(closeRow)
        addSpace(card, 8)
        card.addView(label("DIRECCIÓN: —", 10f, Color.rgb(177, 194, 214), true).apply { tag = "direction" })

        val type = if (Build.VERSION.SDK_INT >= 26)
            WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
        else WindowManager.LayoutParams.TYPE_PHONE
        val layout = WindowManager.LayoutParams(
            dp(290), WindowManager.LayoutParams.WRAP_CONTENT, type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE,
            PixelFormat.TRANSLUCENT
        ).apply {
            gravity = Gravity.TOP or Gravity.END
            x = dp(12)
            y = dp(150)
        }

        header.setOnTouchListener(object : View.OnTouchListener {
            var startX = 0f
            var startY = 0f
            var originalX = 0
            var originalY = 0
            override fun onTouch(view: View, event: MotionEvent): Boolean {
                when (event.action) {
                    MotionEvent.ACTION_DOWN -> {
                        startX = event.rawX
                        startY = event.rawY
                        originalX = layout.x
                        originalY = layout.y
                        return true
                    }
                    MotionEvent.ACTION_MOVE -> {
                        layout.x = (originalX + startX - event.rawX).toInt()
                        layout.y = (originalY + event.rawY - startY).toInt()
                        try { wm.updateViewLayout(card, layout) } catch (_: Exception) {}
                        return true
                    }
                }
                return false
            }
        })

        wm.addView(card, layout)
        panel = card
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (panel == null && Settings.canDrawOverlays(this)) show()
        return START_STICKY
    }

    override fun onDestroy() {
        handler.removeCallbacks(task)
        hidePanel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null
}
