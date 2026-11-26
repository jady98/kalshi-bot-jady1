package com.jady.btcradar

import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.webkit.JavascriptInterface
import android.webkit.WebChromeClient
import android.webkit.WebView
import android.widget.Toast
import org.json.JSONArray
import org.json.JSONObject

class MainActivity : Activity() {
    private lateinit var web: WebView
    private val prefs by lazy { getSharedPreferences("radar_settings", MODE_PRIVATE) }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.statusBarColor = android.graphics.Color.rgb(8,13,23)
        window.navigationBarColor = android.graphics.Color.rgb(8,13,23)
        web = WebView(this).apply {
            setBackgroundColor(android.graphics.Color.rgb(8,13,23))
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.allowFileAccess = true
            webChromeClient = WebChromeClient()
            addJavascriptInterface(RadarBridge(), "AndroidRadar")
        }
        setContentView(web)
        enableRadarNotifications()
        if(savedInstanceState==null) web.loadUrl("file:///android_asset/index.html") else web.restoreState(savedInstanceState)
    }

    inner class RadarBridge {
        @JavascriptInterface fun getSnapshotJson():String = try {
            val d=RadarData.peek(); val root=JSONObject()
            root.put("price",d.price); root.put("prediction",d.prediction); root.put("up",d.up)
            root.put("time",d.time); root.put("direction",d.direction); root.put("entryStage",d.entryStage)
            root.put("alertMessage",d.alertMessage); root.put("pattern",d.pattern); root.put("patternDetail",d.patternDetail)
            root.put("momentumPct",d.momentumPct); root.put("rsi",d.rsi)
            val a=JSONArray(); d.candles.forEach{c->a.put(JSONObject().put("time",c.time).put("low",c.low).put("high",c.high).put("open",c.open).put("close",c.close).put("ema9",c.ema9).put("ema21",c.ema21))}
            root.put("candles",a); root.toString()
        } catch(e:Exception){JSONObject().put("error",e.message?:"No se pudo leer Coinbase").toString()}

        @JavascriptInterface fun setAlertsEnabled(enabled:Boolean) {
            getSharedPreferences("radar_notifications",MODE_PRIVATE).edit().putBoolean("alerts_enabled",enabled).apply()
        }
        @JavascriptInterface fun getAlertsEnabled():Boolean =
            getSharedPreferences("radar_notifications",MODE_PRIVATE).getBoolean("alerts_enabled",true)

        @JavascriptInterface fun activateWindow() { runOnUiThread {
            if(!Settings.canDrawOverlays(this@MainActivity)) {
                Toast.makeText(this@MainActivity,"Permite mostrar sobre otras aplicaciones para activar la ventana flotante.",Toast.LENGTH_LONG).show()
                startActivityForResult(Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION,Uri.parse("package:$packageName")),411)
            } else { prefs.edit().putBoolean("bubble_enabled",true).apply(); sendServiceAction(RadarService.ACTION_SHOW) }
        }}
    }

    private fun sendServiceAction(action:String){try{val i=Intent(this,RadarService::class.java).setAction(action);if(android.os.Build.VERSION.SDK_INT>=26)startForegroundService(i)else startService(i)}catch(_:Exception){Toast.makeText(this,"No se pudo iniciar el servicio de alertas.",Toast.LENGTH_LONG).show()}}
    private fun enableRadarNotifications(){
        if(android.os.Build.VERSION.SDK_INT>=33&&checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS)!=android.content.pm.PackageManager.PERMISSION_GRANTED)
            requestPermissions(arrayOf(android.Manifest.permission.POST_NOTIFICATIONS),410)
        else startRadarBackgroundService()
    }
    private fun startRadarBackgroundService(){try{val i=Intent(this,RadarService::class.java);if(android.os.Build.VERSION.SDK_INT>=26)startForegroundService(i)else startService(i)}catch(_:Exception){Toast.makeText(this,"No se pudo iniciar el servicio de alertas. Revisa los permisos.",Toast.LENGTH_LONG).show()}}
    override fun onRequestPermissionsResult(requestCode:Int,permissions:Array<out String>,grantResults:IntArray){super.onRequestPermissionsResult(requestCode,permissions,grantResults);if(requestCode==410)startRadarBackgroundService()}
    override fun onActivityResult(requestCode:Int,resultCode:Int,data:Intent?){super.onActivityResult(requestCode,resultCode,data);if(requestCode==411&&Settings.canDrawOverlays(this)){prefs.edit().putBoolean("bubble_enabled",true).apply();sendServiceAction(RadarService.ACTION_SHOW)}}
    override fun onSaveInstanceState(outState:Bundle){if(::web.isInitialized)web.saveState(outState);super.onSaveInstanceState(outState)}
    @Deprecated("Deprecated in Android") override fun onBackPressed(){if(prefs.getBoolean("bubble_enabled",false)&&Settings.canDrawOverlays(this)){sendServiceAction(RadarService.ACTION_SHOW);moveTaskToBack(true)}else super.onBackPressed()}
    override fun onDestroy(){if(::web.isInitialized){web.stopLoading();web.destroy()};super.onDestroy()}
}