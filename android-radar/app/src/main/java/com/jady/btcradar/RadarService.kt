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
        val confidenceDirection = when {
            d.up >= 75 -> "ALCISTA"
            d.up <= 25 -> "BAJISTA"
            else -> ""
        }
        if (confidenceDirection.isEmpty()) {
            prefs.edit().putString("last_confidence_alert", "").apply()
        } else {
            val confidenceKey = "CONFIDENCE|$confidenceDirection"
            if (prefs.getString("last_confidence_alert", "") != confidenceKey) {
                val confidencePercent = if (confidenceDirection == "ALCISTA") d.up else 100 - d.up
                val body = "Probabilidad ${if (confidenceDirection == "ALCISTA") "alcista" else "bajista"}: $confidencePercent%."
                val notification = Notification.Builder(this, "btc_events_v2")
                    .setSmallIcon(android.R.drawable.ic_dialog_info)
                    .setContentTitle("BTC Radar · TENDENCIA $confidenceDirection")
                    .setContentText(body)
                    .setAutoCancel(true)
                    .setContentIntent(PendingIntent.getActivity(this, 43, Intent(this, MainActivity::class.java), PendingIntent.FLAG_UPDATE_CURRENT or if (Build.VERSION.SDK_INT >= 23) PendingIntent.FLAG_IMMUTABLE else 0))
                    .build()
                getSystemService(NotificationManager::class.java).notify(43, notification)
                prefs.edit().putString("last_confidence_alert", confidenceKey).apply()
            }
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
                async function refresh(){
                  try{
                    const r=await fetch('/api?t='+Date.now(),{cache:'no-store'});if(!r.ok)throw Error('api');const d=await r.json();
                    const p=Number(d.bullish_score);const q=Number(d.bearish_score);const valid=d.bullish_score!=null&&d.bearish_score!=null&&Number.isFinite(p)&&Number.isFinite(q);const up=valid?Math.max(0,Math.min(100,p)):50;const down=valid?Math.max(0,Math.min(100,q)):50;
                    document.getElementById('price').textContent=money(d.brti_price??d.coinbase_price);
                    document.getElementById('prediction').textContent=money(d.predicted_price);
                    document.getElementById('bull').style.width=up+'%';document.getElementById('bear').style.width=down+'%';
                    document.getElementById('up').textContent='ALCISTA '+(valid?up.toFixed(0)+'%':'--');document.getElementById('down').textContent='BAJISTA '+(valid?down.toFixed(0)+'%':'--');
                    const buyRaw=d.buyers_pct??d.buyer_pct??d.buy_volume_pct;const sellRaw=d.sellers_pct??d.seller_pct??d.sell_volume_pct;const buy=Number(buyRaw),sell=Number(sellRaw);const flowValid=buyRaw!=null&&sellRaw!=null&&Number.isFinite(buy)&&Number.isFinite(sell)&&buy>=0&&sell>=0&&(buy+sell)>0;document.getElementById('buyers').textContent=flowValid?'COMPRADORES '+buy.toFixed(0)+'%':'COMPRADORES —';document.getElementById('sellers').textContent=flowValid?'VENDEDORES '+sell.toFixed(0)+'%':'VENDEDORES —';document.getElementById('flowBuy').style.width=(flowValid?buy:50)+'%';document.getElementById('flowSell').style.width=(flowValid?sell:50)+'%';
                    let dir=String(d.direction||'').toUpperCase();if(dir!=='ALCISTA'&&dir!=='BAJISTA')dir=up>=55?'ALCISTA':up<=45?'BAJISTA':'INDECISA';
                    const emaUp=d.ema9!=null&&d.ema21!=null&&Number(d.ema9)>Number(d.ema21);const emaDown=d.ema9!=null&&d.ema21!=null&&Number(d.ema9)<Number(d.ema21);const mom=Number(d.momentum_pct);const rsi=d.rsi==null?null:Number(d.rsi);const base=Number(d.brti_price??d.coinbase_price);const delta=Number(d.predicted_price)>0&&base>0?((Number(d.predicted_price)/base-1)*100):null;const factors=[];if(emaUp)factors.push('EMA 9 sobre 21');else if(emaDown)factors.push('EMA 9 bajo 21');if(Number.isFinite(mom))factors.push('impulso '+(mom>=0?'+':'')+mom.toFixed(3)+'%');if(rsi!==null&&Number.isFinite(rsi))factors.push('RSI '+rsi.toFixed(0));if(d.pattern)factors.push(String(d.pattern).replace(/[<>]/g,'').slice(0,30));const outlook=dir;const projection=delta===null?'':'; proyección '+(delta>=0?'+':'')+delta.toFixed(2)+'%';const caution=(emaUp&&mom<0)||(emaDown&&mom>0)?' · señales mixtas':'';const note=factors.slice(0,2).join(' y ')||'analizando señales';document.getElementById('direction').textContent=outlook;document.getElementById('direction').style.color=dir==='ALCISTA'?'#36d99a':dir==='BAJISTA'?'#ff7185':'#f3c969';document.getElementById('why').textContent=note+caution+'.';
                    
                    const box=document.getElementById('panel');box.className='wrap '+(dir==='ALCISTA'?'bull':dir==='BAJISTA'?'bear':'neutral');
                                        let secs=Number(d.seconds_to_close??d.countdown);if(!Number.isFinite(secs)||secs<0){const close=Number(d.kalshi_market_close),now=Number(d.server_time);secs=close>0&&now>0?Math.max(0,Math.floor(close-now)):0;}
                    const mm=Math.floor(secs/60),ss=secs%60;document.getElementById('close').textContent='TIEMPO '+String(mm).padStart(2,'0')+':'+String(ss).padStart(2,'0');
                  }catch(e){document.getElementById('direction').textContent='CONECTANDO';}
                }refresh();setInterval(refresh,1000);
                </script></body></html>
                """.trimIndent(), "text/html", "UTF-8", null)
        }
        root.addView(web, FrameLayout.LayoutParams(dp(340), dp(260)).apply { topMargin = dp(40) })
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
        fun resizePanel(delta: Int) {
            lp.width = (lp.width + dp(delta)).coerceIn(dp(250), dp(440))
            lp.height = (lp.height + dp((delta * 0.45).toInt())).coerceIn(dp(250), dp(380))
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
        root.addView(topBar, FrameLayout.LayoutParams(dp(340), dp(40), Gravity.TOP))
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
        var panelScale = 1f
        fun applyPanelScale(value: Float) {
            panelScale = value.coerceIn(0.55f, 1f)
            lp.width = dp((340 * panelScale).toInt())
            lp.height = dp((300 * panelScale).toInt())
            web.scaleX = panelScale
            web.scaleY = panelScale
            topBar.scaleX = panelScale
            topBar.scaleY = panelScale
            resizeHandle.scaleX = panelScale
            resizeHandle.scaleY = panelScale
            wm.updateViewLayout(root, lp)
        }
        resizeHandle.setOnTouchListener(object : View.OnTouchListener {
            var startX = 0f
            var startY = 0f
            var startScale = 1f
            var moved = false
            override fun onTouch(v: View, e: MotionEvent): Boolean {
                when (e.action) {
                    MotionEvent.ACTION_DOWN -> {
                        startX = e.rawX
                        startY = e.rawY
                        startScale = panelScale
                        moved = false
                        return true
                    }
                    MotionEvent.ACTION_MOVE -> {
                        val dx = e.rawX - startX
                        val dy = e.rawY - startY
                        if (kotlin.math.abs(dx) + kotlin.math.abs(dy) > dp(8)) moved = true
                        if (moved) {
                            val change = (dx + dy) / (2f * dp(340))
                            applyPanelScale(startScale + change)
                        }
                        return true
                    }
                    MotionEvent.ACTION_UP -> {
                        if (!moved) applyPanelScale(if (panelScale > 0.8f) 0.62f else 1f)
                        return true
                    }
                    MotionEvent.ACTION_CANCEL -> return true
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
