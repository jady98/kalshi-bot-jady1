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
import android.view.ScaleGestureDetector
import android.widget.*
import android.media.RingtoneManager
import android.media.AudioManager
import android.media.ToneGenerator
import android.webkit.WebView
import android.webkit.JavascriptInterface
import org.json.JSONObject
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
    private var overlayWeb: WebView? = null
    private val handler = Handler(Looper.getMainLooper())
    private var lastSignal = ""
    private var toneGenerator: ToneGenerator? = null
    private var lastCountdownSecond = -1
    private var lastFinalBeepSecond = -1
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
                        overlayWeb?.evaluateJavascript("window.setRadarData && window.setRadarData(" + snapshotJson(d) + ")", null)
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
            val silentEventChannel = NotificationChannel("btc_events_v3", "Alertas BTC Radar", NotificationManager.IMPORTANCE_HIGH).apply {
                setSound(null, null)
                enableVibration(false)
            }
            nm.createNotificationChannel(silentEventChannel)
        }
        toneGenerator = ToneGenerator(AudioManager.STREAM_NOTIFICATION, 85)
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
            fresh && type == "ENTRADA" && d.entryStage == "ENTRADA" -> "ENTRADA"
            fresh && type == "MANTENER" && d.entryStage == "ENTRADA" -> "MANTENER"
            fresh && (type == "SALIDA" || type == "PROTEGER") && d.entryStage == "SALIDA" -> "SALIDA"
            fresh && type == "PREALERTA" && d.entryStage == "PREALERTA" -> "PREALERTA"
            else -> ""
        }
        val prefs = getSharedPreferences("radar_notifications", MODE_PRIVATE)
        if (!prefs.getBoolean("alerts_enabled", true)) {
            prefs.edit().putString("last_direction", d.direction).apply()
            return
        }
        // No emitir pitidos de cuenta regresiva: solo avisos de señales importantes.
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

    private fun playEventTone(event: String) {
        val tone = when (event) {
            "ENTRADA" -> ToneGenerator.TONE_PROP_ACK
            "MANTENER" -> ToneGenerator.TONE_PROP_BEEP2
            "SALIDA" -> ToneGenerator.TONE_PROP_NACK
            else -> ToneGenerator.TONE_PROP_BEEP
        }
        try { toneGenerator?.startTone(tone, 220) } catch (_: Exception) {}
    }

    private fun playContractFinalSeconds(secondsToClose: Int) {
        if (secondsToClose > lastCountdownSecond && lastCountdownSecond >= 0) {
            lastFinalBeepSecond = -1
        }
        lastCountdownSecond = secondsToClose
        if (secondsToClose in 1..4 && secondsToClose != lastFinalBeepSecond) {
            try { toneGenerator?.startTone(ToneGenerator.TONE_PROP_BEEP, 130) } catch (_: Exception) {}
            lastFinalBeepSecond = secondsToClose
        }
        if (secondsToClose <= 0) lastFinalBeepSecond = -1
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
            addJavascriptInterface(object {
                @JavascriptInterface fun getSnapshotJson(): String = try { snapshotJson(RadarData.peek()) } catch (e: Exception) { JSONObject().put("error", e.message ?: "Sin datos").toString() }
            }, "AndroidRadar")
            loadDataWithBaseURL("file:///android_asset/",
                """
                <!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
                <style>
                *{box-sizing:border-box}html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#080d17;color:#eef5ff;font-family:Arial,sans-serif}
                .wrap{height:100%;display:flex;flex-direction:column;justify-content:center;padding:6px 9px 8px;gap:7px}.prices{display:grid;grid-template-columns:1fr 1fr;gap:7px}
                .card{background:#132237;border:1px solid #29415b;border-radius:9px;padding:8px 9px;min-width:0}
                .label{font-size:8px;color:#9db2c9;letter-spacing:.35px;white-space:nowrap}
                .value{font-size:16px;font-weight:800;color:#fff;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
                .mainCard{background:#132237;border:1px solid #29415b;border-radius:10px;padding:9px}
                .trendHead{display:flex;justify-content:space-between;align-items:center;gap:6px}
                .trendTitle{font-size:8px;color:#9db2c9;letter-spacing:.5px}
                #direction{font-size:19px;font-weight:900;color:#36d99a;margin-top:2px}
                .timerBox{background:#202f45;border-radius:7px;padding:5px 8px;text-align:center;min-width:90px}
                .timerLabel{font-size:7px;color:#9db2c9;letter-spacing:.3px}
                #close{font-size:17px;font-weight:800;color:#fff;white-space:nowrap;margin-top:2px}
                .perc{display:flex;justify-content:space-between;font-size:10px;font-weight:800;margin-top:8px}
                #up{color:#36d99a}#down{color:#ff7185}
                .barline{display:flex;height:11px;border-radius:8px;overflow:hidden;margin-top:5px;background:#273449}
                #bull{width:50%;background:#00d995}#bear{width:50%;background:#ff536b}
                .reason{font-size:8px;line-height:1.2;color:#c1d0e2;margin-top:6px;white-space:normal;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;min-height:10px}
                .flow{display:flex;justify-content:space-between;align-items:center;margin-top:8px;padding-top:6px;border-top:1px solid #29415b;font-size:8px;font-weight:700}.flow .buy{color:#36D99A}.flow .sell{color:#FF7185}.flowbar{display:flex;height:7px;overflow:hidden;border-radius:8px;background:#273449;margin-top:5px}.flowbuy{background:#25D987;width:50%}.flowsell{background:#F05269;width:50%}
                
                </style></head><body><div class="wrap neutral" id="panel">
                <div class="prices">
                  <div class="card"><div class="label">PRECIO ACTUAL</div><div class="value" id="price">--</div></div>
                  <div class="card"><div class="label">CIERRE PROYECTADO</div><div class="value" id="prediction">--</div></div>
                </div>
                <div class="mainCard">
                  <div class="trendHead">
                    <div><div class="trendTitle">TENDENCIA</div><div id="direction">ANALIZANDO</div></div>
                    <div class="timerBox"><div class="timerLabel">TIEMPO RESTANTE</div><div id="close">--:--</div></div>
                  </div>
                  <div class="perc"><span id="up">ALCISTA --%</span><span id="down">BAJISTA --%</span></div>
                  <div class="barline"><div id="bull"></div><div id="bear"></div></div>
                  <div class="reason" id="why">Analizando momentum, estructura y fuerza del movimiento…</div>
                  <div class="flow"><span class="buy" id="buyers">COMPRADORES —</span><span class="sell" id="sellers">VENDEDORES —</span></div>
                  <div class="flowbar"><div class="flowbuy" id="flowBuy"></div><div class="flowsell" id="flowSell"></div></div>
                </div>
                
                </div><script>
                const money=v=>Number.isFinite(Number(v))&&Number(v)>0?'$'+Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'--';
                function setRadarData(d){
                  const money=v=>Number.isFinite(Number(v))&&Number(v)>0?'$'+Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'--';
                  if(d.error){document.getElementById('direction').textContent='SIN DATOS';return;}
                  const up=Math.max(0,Math.min(100,Number(d.up)||50));
                  document.getElementById('price').textContent=money(d.price);
                  document.getElementById('prediction').textContent=money(d.prediction);
                  document.getElementById('bull').style.width=up+'%';document.getElementById('bear').style.width=(100-up)+'%';
                  document.getElementById('up').textContent='ALCISTA '+up+'%';document.getElementById('down').textContent='BAJISTA '+(100-up)+'%';
                  document.getElementById('direction').textContent=d.direction||'INDECISO';
                  document.getElementById('direction').style.color=d.direction==='ALCISTA'?'#36d99a':d.direction==='BAJISTA'?'#ff7185':'#f3c969';
                  document.getElementById('why').textContent=(d.entryStage==='ENTRADA'?'SEÑAL CONFIRMADA · ':'VIGILANDO · ')+(d.alertMessage||d.patternDetail||'');
                  document.getElementById('close').textContent='TIEMPO '+(d.time||'--:--');
                  document.getElementById('buyers').textContent='PATRÓN '+(d.pattern||'—');
                  document.getElementById('sellers').textContent='RSI '+Math.round(Number(d.rsi)||50);
                }
                function refresh(){try{setRadarData(JSON.parse(AndroidRadar.getSnapshotJson()));}catch(e){document.getElementById('direction').textContent='CONECTANDO';}}
                refresh();setInterval(refresh,5000);
                </script></body></html>
                """.trimIndent(), "text/html", "UTF-8", null)
        }
        root.addView(web, FrameLayout.LayoutParams(-1, -1).apply { topMargin = dp(40) })
        web.pivotX = 0f
        web.pivotY = 0f


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
        val lp = WindowManager.LayoutParams(dp(340), dp(300), type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT
        ).apply { gravity = Gravity.TOP or Gravity.CENTER_HORIZONTAL; y = dp(70) }
        val display = resources.displayMetrics
        val maxPanelWidth = (display.widthPixels - dp(16)).coerceAtLeast(dp(220))
        val maxPanelHeight = (display.heightPixels - dp(80)).coerceAtLeast(dp(190))
        fun applyPanelSize(width: Int, height: Int) {
            lp.width = width.coerceIn(dp(220), maxPanelWidth)
            lp.height = height.coerceIn(dp(190), maxPanelHeight)
            web.layoutParams = FrameLayout.LayoutParams(-1, lp.height - dp(40)).apply { topMargin = dp(40) }
            topBar.layoutParams = FrameLayout.LayoutParams(lp.width, dp(40), Gravity.TOP)
            wm.updateViewLayout(root, lp)
        }
        val scaleDetector = ScaleGestureDetector(this, object : ScaleGestureDetector.SimpleOnScaleGestureListener() {
            override fun onScale(detector: ScaleGestureDetector): Boolean {
                applyPanelSize((lp.width * detector.scaleFactor).toInt(), (lp.height * detector.scaleFactor).toInt())
                return true
            }
        })
        web.setOnTouchListener { _, event ->
            scaleDetector.onTouchEvent(event)
            false
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
        topBar.pivotX = 0f
        topBar.pivotY = 0f

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
            contentDescription = "Arrastra para reducir o ampliar la ventana"
        }
        root.addView(resizeHandle, FrameLayout.LayoutParams(dp(30), dp(30), Gravity.BOTTOM or Gravity.RIGHT).apply {
            rightMargin = dp(4)
            bottomMargin = dp(4)
        })
        resizeHandle.setOnTouchListener(object : View.OnTouchListener {
            var startX = 0f
            var startY = 0f
            var moved = false
            override fun onTouch(v: View, e: MotionEvent): Boolean {
                when (e.action) {
                    MotionEvent.ACTION_DOWN -> {
                        startX = e.rawX
                        startY = e.rawY
                        moved = false
                        return true
                    }
                    MotionEvent.ACTION_MOVE -> {
                        val dx = e.rawX - startX
                        val dy = e.rawY - startY
                        if (kotlin.math.abs(dx) + kotlin.math.abs(dy) > dp(8)) moved = true
                        if (moved) {
                            applyPanelSize(lp.width + dx.toInt(), lp.height + dy.toInt())
                            startX = e.rawX
                            startY = e.rawY
                        }
                        return true
                    }
                    MotionEvent.ACTION_UP -> {
                        if (!moved) applyPanelSize(if (lp.width > dp(300)) dp(260) else dp(340), if (lp.height > dp(240)) dp(210) else dp(300))
                        return true
                    }
                    MotionEvent.ACTION_CANCEL -> return true
                }
                return false
            }
        })

        wm.addView(root, lp)
        panel = root
        overlayWeb = web
    }


    private fun snapshotJson(d: Snapshot): String {
        return JSONObject()
            .put("price", d.price)
            .put("prediction", d.prediction)
            .put("up", d.up)
            .put("time", d.time)
            .put("direction", d.direction)
            .put("entryStage", d.entryStage)
            .put("alertMessage", d.alertMessage)
            .put("pattern", d.pattern)
            .put("patternDetail", d.patternDetail)
            .put("rsi", d.rsi)
            .toString()
    }

    private fun removePanel() {
        try { panel?.let { wm.removeView(it) } } catch (_: Exception) {}
        panel = null
        overlayWeb = null
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
        try { toneGenerator?.release() } catch (_: Exception) {}
        toneGenerator = null
        super.onDestroy()
    }
    override fun onBind(i: Intent?): IBinder? = null
}
