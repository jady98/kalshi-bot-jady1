package com.jady.btcradar

import android.app.*
import android.content.Intent
import android.graphics.Color
import android.graphics.PixelFormat
import android.os.*
import android.provider.Settings
import android.view.*
import android.widget.*

class RadarService : Service() {
    private lateinit var wm: WindowManager
    private var panel: LinearLayout? = null
    private val handler = Handler(Looper.getMainLooper())
    private var lastAlertKey = ""

    private val task = object : Runnable {
        override fun run() {
            Thread {
                try {
                    val d = RadarData.load()
                    handler.post {
                        panel?.findViewWithTag<TextView>("p")?.text =
                            "BTC $" + "%,.2f".format(d.price)
                        panel?.findViewWithTag<TextView>("t")?.text =
                            "ALCISTA " + d.up + "%  |  BAJISTA " + (100 - d.up) + "%"
                        panel?.findViewWithTag<ProgressBar>("tb")?.progress = d.up
                        panel?.findViewWithTag<TextView>("b")?.text =
                            "COMPRADORES / VENDEDORES: dato no publicado por Render"
                        panel?.findViewWithTag<ProgressBar>("bb")?.progress = 50
                        panel?.findViewWithTag<TextView>("c")?.text =
                            "CIERRE 15M  $" + "%,.2f".format(d.prediction) + "  •  " + d.time

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
                    // Se conserva la última lectura visible si Render no responde temporalmente.
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

    private fun show() {
        wm = getSystemService(WINDOW_SERVICE) as WindowManager
        val box = LinearLayout(this).apply {
            orientation = 1
            setPadding(14, 10, 14, 10)
            setBackgroundColor(Color.rgb(11, 17, 26))
        }
        fun line(value: String, tagName: String, size: Float) =
            TextView(this).apply {
                text = value
                tag = tagName
                textSize = size
                setTextColor(Color.WHITE)
                setPadding(3, 4, 3, 4)
            }

        box.addView(line("BTC RADAR  ⠿", "h", 15f))
        box.addView(line("BTC —", "p", 19f))
        box.addView(line("ALCISTA — | BAJISTA —", "t", 14f))
        box.addView(ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal).apply {
            max = 100
            progress = 50
            tag = "tb"
        })
        box.addView(line("COMPRADORES / VENDEDORES: --", "b", 10f))
        box.addView(ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal).apply {
            max = 100
            progress = 50
            tag = "bb"
        })
        box.addView(line("CIERRE 15M —", "c", 12f))

        val type = if (Build.VERSION.SDK_INT >= 26)
            WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
        else WindowManager.LayoutParams.TYPE_PHONE

        val layout = WindowManager.LayoutParams(
            WindowManager.LayoutParams.WRAP_CONTENT,
            WindowManager.LayoutParams.WRAP_CONTENT,
            type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE,
            PixelFormat.TRANSLUCENT
        ).apply {
            gravity = Gravity.TOP or Gravity.END
            x = 15
            y = 180
        }

        box.setOnTouchListener(object : View.OnTouchListener {
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
                        wm.updateViewLayout(box, layout)
                        return true
                    }
                }
                return true
            }
        })
        wm.addView(box, layout)
        panel = box
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int) = START_STICKY

    override fun onDestroy() {
        handler.removeCallbacks(task)
        if (panel != null) wm.removeView(panel)
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null
}
