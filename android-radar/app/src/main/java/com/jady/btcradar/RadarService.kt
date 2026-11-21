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
import android.media.RingtoneManager
import android.webkit.WebView
import android.widget.FrameLayout
import android.text.SpannableString
import android.text.Spanned
import android.text.style.ForegroundColorSpan

class RadarService : Service() {
    companion object {
        const val ACTION_SHOW = "com.jady.btcradar.SHOW_BUBBLE"
        const val ACTION_HIDE = "com.jady.btcradar.HIDE_BUBBLE"
    }
    private lateinit var wm: WindowManager
    private var panel: View? = null
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
                        // Las velas se reservan para la pantalla principal; aquí se muestra la alerta.
                        panel?.findViewWithTag<TextView>("prediction")?.text = "$" + "%,.2f".format(d.prediction)
                        panel?.findViewWithTag<TextView>("timer")?.text = d.time
                        panel?.findViewWithTag<TextView>("position")?.text =
                            if (d.manualSide == "YES" || d.manualSide == "NO") "TU POSICIÓN: ${d.manualSide}" else "POSICIÓN: SIN MARCAR"
                        notifyRadarEvent(d)
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
        if (Build.VERSION.SDK_INT >= 26) {
            nm.createNotificationChannel(NotificationChannel("btc", "Servicio BTC Radar", NotificationManager.IMPORTANCE_LOW))
            val eventChannel = NotificationChannel("btc_events_v2", "Alertas BTC Radar", NotificationManager.IMPORTANCE_HIGH).apply {
                setSound(
                    RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION),
                    android.media.AudioAttributes.Builder()
                        .setUsage(android.media.AudioAttributes.USAGE_NOTIFICATION)
                        .setContentType(android.media.AudioAttributes.CONTENT_TYPE_SONIFICATION)
                        .build()
                )
                enableVibration(true)
            }
            nm.createNotificationChannel(eventChannel)
        }
        startForeground(1, Notification.Builder(this, "btc").setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle("BTC Radar activo").setContentText("Monitoreando BTC/USD").build())
        if (Settings.canDrawOverlays(this) && getSharedPreferences("radar_settings", MODE_PRIVATE).getBoolean("bubble_enabled", false)) show()
        handler.post(task)
    }

    private fun notifyRadarEvent(d: Snapshot) {
        val now = System.currentTimeMillis() / 1000L
        val fresh = d.alertTimestamp > 0L && now >= d.alertTimestamp && now - d.alertTimestamp <= 300L
        val type = d.alertType.orEmpty().uppercase()
        val event = when {
            fresh && type == "CAMBIO_DIRECCION" -> "CAMBIO DE TENDENCIA"
            fresh && type == "ENTRADA" -> "ENTRADA"
            fresh && type == "MANTENER" -> "MANTENER"
            fresh && (type == "SALIDA" || type == "PROTEGER") -> "SALIDA"
            fresh && type == "PREALERTA" -> "PREALERTA"
            else -> ""
        }
        val prefs = getSharedPreferences("radar_notifications", MODE_PRIVATE)
        val expected = if (d.manualSide == "YES") "ALCISTA" else if (d.manualSide == "NO") "BAJISTA" else ""
        val alignment = when {
            d.manualSide.isEmpty() -> "No has marcado YES o NO."
            d.entryDirection == expected || d.direction == expected -> "Tu posición \${d.manualSide} coincide con la señal."
            else -> "ATENCIÓN: la señal va contra tu posición \${d.manualSide}."
        }
        if (event.isNotEmpty()) {
            val key = listOf(event, d.alertKey).joinToString("|")
            if (prefs.getString("last_event", "") == key) return
            val body = listOf(d.alertMessage.ifBlank { "BTC Radar detectó: $event." }, alignment).joinToString("\\n")
            val notification = Notification.Builder(this, "btc_events_v2")
                .setSmallIcon(android.R.drawable.ic_dialog_info)
                .setContentTitle("BTC Radar · $event")
                .setContentText(body)
                .setStyle(Notification.BigTextStyle().bigText(body))
                .setAutoCancel(true)
                .setContentIntent(PendingIntent.getActivity(this, 41, Intent(this, MainActivity::class.java), PendingIntent.FLAG_UPDATE_CURRENT or if (Build.VERSION.SDK_INT >= 23) PendingIntent.FLAG_IMMUTABLE else 0))
                .build()
            getSystemService(NotificationManager::class.java).notify(key.hashCode(), notification)
            prefs.edit().putString("last_event", key).apply()
            return
        }
        if (d.direction == "ALCISTA" || d.direction == "BAJISTA") {
            val previous = prefs.getString("last_direction", "")
            if (previous.orEmpty().isNotEmpty() && previous != d.direction) {
                val key = "DIRECTION|${d.direction}|${d.alertKey}"
                if (prefs.getString("last_event", "") != key) {
                    val notification = Notification.Builder(this, "btc_events_v2")
                        .setSmallIcon(android.R.drawable.ic_dialog_info)
                        .setContentTitle("BTC Radar · CAMBIO DE TENDENCIA")
                        .setContentText("\${d.direction} · $alignment")
                        .setAutoCancel(true)
                        .setContentIntent(PendingIntent.getActivity(this, 42, Intent(this, MainActivity::class.java), PendingIntent.FLAG_UPDATE_CURRENT or if (Build.VERSION.SDK_INT >= 23) PendingIntent.FLAG_IMMUTABLE else 0))
                        .build()
                    getSystemService(NotificationManager::class.java).notify(42, notification)
                    prefs.edit().putString("last_event", key).apply()
                }
            }
            prefs.edit().putString("last_direction", d.direction).apply()
        }
    }

    private fun dp(v: Int) = (v * resources.displayMetrics.density).toInt()

    private fun show() {
        if (panel != null) return
        wm = getSystemService(WINDOW_SERVICE) as WindowManager
        val root = FrameLayout(this).apply {
            background = android.graphics.drawable.GradientDrawable().apply {
                setColor(Color.rgb(8, 13, 23)); cornerRadius = dp(16).toFloat()
                setStroke(dp(1), Color.rgb(67, 105, 139))
            }
            clipToOutline = true
            elevation = dp(18).toFloat()
        }

        // Panel compacto: precios, barra, tendencia y cierre.
        val web = WebView(this).apply {
            setBackgroundColor(Color.rgb(8, 13, 23))
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.cacheMode = android.webkit.WebSettings.LOAD_NO_CACHE
            loadDataWithBaseURL("https://kalshi-bot-jady1.onrender.com/",
                """
                <!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
                <style>
                *{box-sizing:border-box}html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#080d17;color:#eef5ff;font-family:Arial,sans-serif}
                .wrap{height:100%;display:flex;flex-direction:column;justify-content:center;padding:6px 10px 8px}.prices{display:grid;grid-template-columns:1fr 1fr;gap:8px}
                .card{background:#111d2d;border:1px solid #243b55;border-radius:9px;padding:7px 9px;min-width:0}
                .label{font-size:9px;color:#91a9c2;letter-spacing:.4px;white-space:nowrap}
                .value{font-size:17px;font-weight:800;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
                .barline{display:flex;height:13px;border-radius:8px;overflow:hidden;margin-top:10px;background:#273449}
                #bull{width:50%;background:#00d995}#bear{width:50%;background:#ff536b}
                .perc{display:flex;justify-content:space-between;font-size:10px;font-weight:700;margin-top:4px}
                #up{color:#00e6a0}#down{color:#ff7185}
                .bottom{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-top:7px;font-size:10px}
                #trend{font-weight:800;white-space:nowrap}#close{color:#9db2c9;white-space:nowrap}
                .bull #trend{color:#00e6a0}.bear #trend{color:#ff7185}.neutral #trend{color:#e8c66d}
                </style></head><body><div class="wrap neutral" id="panel">
                <div class="prices"><div class="card"><div class="label">PRECIO ACTUAL</div><div class="value" id="price">--</div></div>
                <div class="card"><div class="label">PREDICCIÓN AL CIERRE</div><div class="value" id="prediction">--</div></div></div>
                <div class="barline"><div id="bull"></div><div id="bear"></div></div>
                <div class="perc"><span id="up">ALCISTA --%</span><span id="down">BAJISTA --%</span></div>
                <div class="bottom"><span id="trend">TENDENCIA INDECISA</span><span id="close">CIERRE --:--</span></div>
                </div><script>
                const money=v=>Number.isFinite(Number(v))&&Number(v)>0?'$'+Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'--';
                async function refresh(){
                  try{
                    const r=await fetch('/api?t='+Date.now(),{cache:'no-store'});if(!r.ok)throw Error('api');const d=await r.json();
                    const p=Number(d.kalshi_yes_probability_pct??d.bullish_score??50);const up=Math.max(0,Math.min(100,Number.isFinite(p)?p:50));const down=100-up;
                    document.getElementById('price').textContent=money(d.brti_price??d.coinbase_price);
                    document.getElementById('prediction').textContent=money(d.predicted_price);
                    document.getElementById('bull').style.width=up+'%';document.getElementById('bear').style.width=down+'%';
                    document.getElementById('up').textContent='ALCISTA '+up.toFixed(0)+'%';document.getElementById('down').textContent='BAJISTA '+down.toFixed(0)+'%';
                    let dir=String(d.direction||'').toUpperCase();if(dir!=='ALCISTA'&&dir!=='BAJISTA')dir=up>=55?'ALCISTA':up<=45?'BAJISTA':'INDECISA';
                    const box=document.getElementById('panel');box.className='wrap '+(dir==='ALCISTA'?'bull':dir==='BAJISTA'?'bear':'neutral');
                    document.getElementById('trend').textContent='TENDENCIA '+dir;
                    let secs=Number(d.seconds_to_close??d.countdown);if(!Number.isFinite(secs)||secs<0){const close=Number(d.kalshi_market_close),now=Number(d.server_time);secs=close>0&&now>0?Math.max(0,Math.floor(close-now)):0;}
                    const mm=Math.floor(secs/60),ss=secs%60;document.getElementById('close').textContent='CIERRE '+String(mm).padStart(2,'0')+':'+String(ss).padStart(2,'0');
                  }catch(e){document.getElementById('trend').textContent='CONECTANDO…';}
                }refresh();setInterval(refresh,1000);
                </script></body></html>
                """.trimIndent(), "text/html", "UTF-8", null)
        }
        root.addView(web, FrameLayout.LayoutParams(-1, -1).apply { topMargin = dp(40) })


        val topBar = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            setPadding(dp(12), 0, dp(7), 0)
            setBackgroundColor(Color.rgb(18, 29, 45))
        }
        val title = TextView(this).apply {
            text = "BTC RADAR  ·  EN VIVO"; textSize = 11f
            setTextColor(Color.rgb(111, 207, 255))
            typeface = Typeface.create("sans-serif", Typeface.BOLD)
        }
        topBar.addView(title, LinearLayout.LayoutParams(0, -1, 1f))
        val type = if (Build.VERSION.SDK_INT >= 26) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE
        val lp = WindowManager.LayoutParams(dp(340), dp(190), type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT
        ).apply { gravity = Gravity.TOP or Gravity.CENTER_HORIZONTAL; y = dp(70) }
        fun resizePanel(delta: Int) {
            lp.width = (lp.width + dp(delta)).coerceIn(dp(250), dp(440))
            lp.height = (lp.height + dp((delta * 0.45).toInt())).coerceIn(dp(150), dp(280))
            wm.updateViewLayout(root, lp)
        }
        val close = TextView(this).apply {
            text = "×"; textSize = 25f; gravity = Gravity.CENTER
            setTextColor(Color.WHITE)
            background = android.graphics.drawable.GradientDrawable().apply {
                setColor(Color.rgb(176, 47, 70)); cornerRadius = dp(18).toFloat()
            }
        }
        topBar.addView(close, LinearLayout.LayoutParams(dp(30), dp(30)))
        close.setOnClickListener {
            getSharedPreferences("radar_settings", MODE_PRIVATE).edit().putBoolean("bubble_enabled", false).apply()
            removePanel()
        }
        root.addView(topBar, FrameLayout.LayoutParams(-1, dp(40), Gravity.TOP))

        topBar.setOnTouchListener(object : View.OnTouchListener {
            var sx=0f; var sy=0f; var ox=0; var oy=0
            override fun onTouch(v: View, e: MotionEvent): Boolean {
                when(e.action){
                    MotionEvent.ACTION_DOWN->{sx=e.rawX;sy=e.rawY;ox=lp.x;oy=lp.y;return true}
                    MotionEvent.ACTION_MOVE->{lp.x=(ox+e.rawX-sx).toInt();lp.y=(oy+e.rawY-sy).toInt();wm.updateViewLayout(root,lp);return true}
                    MotionEvent.ACTION_UP->return true
                }
                return false
            }
        })
        val resizeHandle = TextView(this).apply {
            text = "◢"
            textSize = 22f
            gravity = Gravity.CENTER
            setTextColor(Color.rgb(111, 207, 255))
            background = GradientDrawable().apply {
                setColor(Color.argb(220, 20, 43, 66))
                cornerRadius = dp(8).toFloat()
                setStroke(dp(1), Color.rgb(67, 105, 139))
            }
            contentDescription = "Arrastra esta esquina para cambiar el tamaño"
        }
        root.addView(resizeHandle, FrameLayout.LayoutParams(dp(30), dp(30), Gravity.BOTTOM or Gravity.RIGHT).apply {
            rightMargin = dp(4)
            bottomMargin = dp(4)
        })
        resizeHandle.setOnTouchListener(object : View.OnTouchListener {
            var startX = 0f
            var startY = 0f
            var startWidth = 0
            var startHeight = 0
            override fun onTouch(v: View, e: MotionEvent): Boolean {
                when (e.action) {
                    MotionEvent.ACTION_DOWN -> {
                        startX = e.rawX
                        startY = e.rawY
                        startWidth = lp.width
                        startHeight = lp.height
                        return true
                    }
                    MotionEvent.ACTION_MOVE -> {
                        lp.width = (startWidth + (e.rawX - startX).toInt()).coerceIn(dp(250), dp(440))
                        lp.height = (startHeight + (e.rawY - startY).toInt()).coerceIn(dp(150), dp(360))
                        wm.updateViewLayout(root, lp)
                        return true
                    }
                    MotionEvent.ACTION_UP, MotionEvent.ACTION_CANCEL -> return true
                }
                return false
            }
        })

        wm.addView(root, lp)
        panel = root
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
