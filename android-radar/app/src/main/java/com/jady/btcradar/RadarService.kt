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
    private var lastSignal = ""
    private val task = object : Runnable {
        override fun run() {
            Thread {
                try {
                    val d = RadarData.load()
                    handler.post {
                        panel?.findViewWithTag<TextView>("p")?.text = "BTC  $" + "%,.0f".format(d.price)
                        panel?.findViewWithTag<TextView>("t")?.text = "ALCISTA  ${d.up}%     BAJISTA  ${100-d.up}%"
                        panel?.findViewWithTag<ProgressBar>("tb")?.progress = d.up
                        panel?.findViewWithTag<TextView>("b")?.text = "FLUJO COMPRADOR / VENDEDOR: DATO NO DISPONIBLE"
                        panel?.findViewWithTag<ProgressBar>("bb")?.progress = d.buyers
                        panel?.findViewWithTag<TextView>("c")?.text = "CIERRE EN 15 MIN\n$" + "%,.2f".format(d.prediction) + "   •   " + d.time
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
                } catch (_: Exception) {}
            }.start()
            handler.postDelayed(this, 5000)
        }
    }

    override fun onCreate() {
        super.onCreate()
        val nm = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= 26) nm.createNotificationChannel(NotificationChannel("btc", "Alertas BTC", NotificationManager.IMPORTANCE_DEFAULT))
        startForeground(1, Notification.Builder(this, "btc").setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle("BTC Radar activo").setContentText("Monitoreando BTC/USD").build())
        if (Settings.canDrawOverlays(this)) show()
        handler.post(task)
    }

    private fun dp(v: Int) = (v * resources.displayMetrics.density).toInt()
    private fun rounded(color: Int, radius: Int = 18): GradientDrawable =
        GradientDrawable().apply { setColor(color); cornerRadius = dp(radius).toFloat() }

    private fun label(value: String, tagName: String, size: Float, color: Int = Color.WHITE): TextView =
        TextView(this).apply {
            text = value; tag = tagName; textSize = size; setTextColor(color)
            setPadding(dp(3), dp(5), dp(3), dp(5))
            if (size >= 16f) typeface = Typeface.create("sans-serif", Typeface.BOLD)
        }

    private fun bar(progressTag: String, green: Boolean): ProgressBar =
        ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal).apply {
            max = 100; progress = 50; tag = progressTag
            progressDrawable = GradientDrawable().apply {
                shape = GradientDrawable.RECTANGLE
                cornerRadius = dp(8).toFloat()
                setColor(if (green) Color.rgb(0, 220, 145) else Color.rgb(255, 72, 100))
            }
            progressTintList = android.content.res.ColorStateList.valueOf(if (green) Color.rgb(0, 220, 145) else Color.rgb(255, 72, 100))
            progressBackgroundTintList = android.content.res.ColorStateList.valueOf(Color.rgb(47, 58, 73))
            minimumHeight = dp(9)
        }

    private fun show() {
        if (panel != null) return
        wm = getSystemService(WINDOW_SERVICE) as WindowManager
        val box = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(14), dp(11), dp(14), dp(13))
            background = GradientDrawable(
                GradientDrawable.Orientation.TOP_BOTTOM,
                intArrayOf(Color.rgb(20, 31, 48), Color.rgb(7, 12, 21))
            ).apply {
                cornerRadius = dp(20).toFloat()
                setStroke(dp(1), Color.rgb(55, 93, 126))
            }
            elevation = dp(12).toFloat()
        }

        val header = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER_VERTICAL }
        val title = label("BTC RADAR  •  EN VIVO", "h", 13f, Color.rgb(100, 205, 255))
        header.addView(title, LinearLayout.LayoutParams(0, -2, 1f))
        val close = TextView(this).apply {
            text = "×"; textSize = 27f; gravity = Gravity.CENTER; setTextColor(Color.WHITE)
            background = rounded(Color.rgb(175, 43, 63), 30)
            contentDescription = "Cerrar ventana flotante"
        }
        header.addView(close, LinearLayout.LayoutParams(dp(34), dp(34)))
        close.setOnClickListener { try { wm.removeView(box) } catch (_: Exception) {}; panel = null }
        box.addView(header)
        box.addView(label("BTC —", "p", 21f))
        box.addView(label("ALCISTA  —%     BAJISTA  —%", "t", 13f))
        box.addView(bar("tb", true), LinearLayout.LayoutParams(-1, dp(10)).apply { bottomMargin = dp(10) })
        box.addView(label("FLUJO COMPRADOR / VENDEDOR: SIN DATO", "b", 10f, Color.rgb(190, 204, 220)))
        val closeInfo = label("CIERRE EN 15 MIN\n—", "c", 13f, Color.rgb(255, 213, 105))
        closeInfo.background = rounded(Color.rgb(29, 39, 55), 12)
        closeInfo.setPadding(dp(10), dp(8), dp(10), dp(8))
        box.addView(closeInfo)

        val type = if (Build.VERSION.SDK_INT >= 26) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE
        val lp = WindowManager.LayoutParams(
            dp(290), WindowManager.LayoutParams.WRAP_CONTENT, type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT
        ).apply { gravity = Gravity.TOP or Gravity.END; x = dp(12); y = dp(150) }

        box.setOnTouchListener(object : View.OnTouchListener {
            var sx = 0f; var sy = 0f; var ox = 0; var oy = 0
            override fun onTouch(v: View, e: MotionEvent): Boolean {
                when (e.action) {
                    MotionEvent.ACTION_DOWN -> { sx=e.rawX; sy=e.rawY; ox=lp.x; oy=lp.y; return true }
                    MotionEvent.ACTION_MOVE -> { lp.x=(ox+sx-e.rawX).toInt(); lp.y=(oy+e.rawY-sy).toInt(); wm.updateViewLayout(box,lp); return true }
                }
                return false
            }
        })
        wm.addView(box, lp)
        panel = box
    }

    override fun onStartCommand(i: Intent?, f: Int, id: Int) = START_STICKY
    override fun onDestroy() {
        handler.removeCallbacks(task)
        try { panel?.let { wm.removeView(it) } } catch (_: Exception) {}
        panel = null
        super.onDestroy()
    }
    override fun onBind(i: Intent?): IBinder? = null
}
