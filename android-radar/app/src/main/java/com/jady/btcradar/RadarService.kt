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
                        panel?.findViewWithTag<MiniChartView>("mini_chart")?.setCandles(d.candles)
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
            nm.createNotificationChannel(NotificationChannel("btc_events", "Cambios de tendencia y posición", NotificationManager.IMPORTANCE_HIGH))
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
            val notification = Notification.Builder(this, "btc_events")
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
                    val notification = Notification.Builder(this, "btc_events")
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

        // La ventana carga la misma página y el mismo gráfico vivo del radar.
        val web = WebView(this).apply {
            setBackgroundColor(Color.rgb(8, 13, 23))
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.loadsImagesAutomatically = true
            settings.useWideViewPort = true
            settings.loadWithOverviewMode = false
            settings.cacheMode = android.webkit.WebSettings.LOAD_NO_CACHE
            webChromeClient = android.webkit.WebChromeClient()
            webViewClient = object : android.webkit.WebViewClient() {
                override fun onPageFinished(view: WebView?, url: String?) {
                    val js = """
                        (function(){
                          if(document.getElementById('btc-floating-style')) return;
                          const s=document.createElement('style'); s.id='btc-floating-style';
                          s.textContent='html,body{margin:0!important;padding:0!important;background:#080b14!important}body{padding:44px 7px 8px!important}main{max-width:none!important;width:100%!important;margin:0!important}.header{margin:0 0 7px!important}.logo{font-size:15px!important}.status{font-size:7px!important}.grid{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:5px!important;margin-bottom:6px!important}.grid .target-card{display:none!important}.grid .card{padding:7px!important;margin:0!important;border-radius:10px!important}.grid .price{font-size:17px!important;margin:4px 0!important}.grid .label{font-size:8px!important}.grid .small{font-size:7px!important}main>section.card:has(#direction){padding:8px!important;margin-bottom:6px!important}main>section.card:has(#direction) .section-title{font-size:9px!important}main>section.card:has(#direction) .signal{font-size:13px!important;margin:3px 0!important}main>section.card:has(#direction) .price{font-size:16px!important;margin:3px 0!important}.bar{display:flex!important;width:100%!important;height:12px!important;overflow:hidden!important;border-radius:8px!important;gap:0!important}.bar .bull{display:block!important;flex:0 0 auto!important;background:#00d995!important;min-width:0!important}.bar .bear{display:block!important;flex:0 0 auto!important;background:#ff536b!important;min-width:0!important}main>section.card:has(#chart){padding:7px!important;margin:0!important}main>section.card:has(#chart) .section-title{font-size:9px!important}#chart{height:245px!important;min-height:245px!important}.chart-wrap{height:245px!important;min-height:245px!important}main> :not(.header):not(.grid):not(section:has(#direction)):not(section:has(#chart)){display:none!important}.technical-card,.radar-alert-panel{display:none!important}';
                          document.head.appendChild(s);
                          setTimeout(()=>{window.dispatchEvent(new Event('resize'));},500);
                          setTimeout(()=>window.dispatchEvent(new Event('resize')),1500);
                        })();
                    """.trimIndent()
                    evaluateJavascript(js, null)
                }
                override fun shouldOverrideUrlLoading(view: WebView?, request: android.webkit.WebResourceRequest?): Boolean {
                    return !request?.url.toString().startsWith("https://kalshi-bot-jady1.onrender.com/")
                }
            }
        }
        root.addView(web, FrameLayout.LayoutParams(-1, -1))
        web.loadUrl("https://kalshi-bot-jady1.onrender.com/?floating=1&v=2")

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

        val type = if (Build.VERSION.SDK_INT >= 26) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE
        val lp = WindowManager.LayoutParams(dp(360), dp(590), type,
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT
        ).apply { gravity = Gravity.TOP or Gravity.CENTER_HORIZONTAL; y = dp(70) }

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
