package com.jady.btcradar

import android.app.Activity
import android.content.Intent
import android.graphics.Color
import android.graphics.drawable.GradientDrawable
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.view.Gravity
import android.view.View
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.webkit.JavascriptInterface
import android.widget.Button
import android.widget.FrameLayout
import android.widget.LinearLayout
import android.widget.Switch
import android.widget.ProgressBar
import android.widget.Toast

class MainActivity : Activity() {
    private lateinit var web: WebView
    private val prefs by lazy { getSharedPreferences("radar_settings", MODE_PRIVATE) }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.statusBarColor = Color.rgb(11, 17, 26)
        window.navigationBarColor = Color.rgb(11, 17, 26)

        val root = FrameLayout(this).apply { setBackgroundColor(Color.rgb(11, 17, 26)) }

        web = WebView(this).apply {
            setBackgroundColor(Color.rgb(11, 17, 26))
            settings.javaScriptEnabled = true
            addJavascriptInterface(RadarBridge(), "AndroidRadar")
            settings.domStorageEnabled = true
            settings.databaseEnabled = true
            settings.loadsImagesAutomatically = true
            settings.useWideViewPort = true
            settings.loadWithOverviewMode = true
            settings.javaScriptCanOpenWindowsAutomatically = true
            settings.setSupportMultipleWindows(false)
            webChromeClient = WebChromeClient()
            webViewClient = object : WebViewClient() {
                override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                    val url = request?.url?.toString() ?: return false
                    return !url.startsWith("https://kalshi-bot-jady1.onrender.com/")
                }
            }
        }

        val progress = ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal)
        progress.max = 100
        progress.progressTintList = android.content.res.ColorStateList.valueOf(Color.rgb(0, 214, 138))
        progress.indeterminateTintList = android.content.res.ColorStateList.valueOf(Color.rgb(0, 214, 138))
        web.webChromeClient = object : WebChromeClient() {
            override fun onProgressChanged(view: WebView?, newProgress: Int) {
                progress.progress = newProgress
                progress.visibility = if (newProgress >= 100) View.GONE else View.VISIBLE
            }
        }

        root.addView(web, FrameLayout.LayoutParams(-1, -1))
        root.addView(progress, FrameLayout.LayoutParams(-1, 4, Gravity.TOP))

        setContentView(root)
        enableRadarNotifications()
        if (savedInstanceState == null) web.loadUrl("https://kalshi-bot-jady1.onrender.com/")
        else web.restoreState(savedInstanceState)
    }

    private inner class RadarBridge {
        @JavascriptInterface
        fun setAlertsEnabled(enabled: Boolean) {
            getSharedPreferences("radar_notifications", MODE_PRIVATE)
                .edit().putBoolean("alerts_enabled", enabled).apply()
        }

        @JavascriptInterface
        fun getAlertsEnabled(): Boolean {
            return getSharedPreferences("radar_notifications", MODE_PRIVATE)
                .getBoolean("alerts_enabled", true)
        }

        @JavascriptInterface
        fun activateWindow() {
            runOnUiThread {
                if (!Settings.canDrawOverlays(this@MainActivity)) {
                    Toast.makeText(this@MainActivity, "Permite mostrar sobre otras aplicaciones para usar la ventana.", Toast.LENGTH_LONG).show()
                    startActivityForResult(Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION, Uri.parse("package:" + packageName)), 411)
                } else {
                    prefs.edit().putBoolean("bubble_enabled", true).apply()
                    sendServiceAction(RadarService.ACTION_SHOW)
                }
            }
        }
    }

    private fun dp(value: Int): Int = (value * resources.displayMetrics.density).toInt()

    private fun toggleBubble() {
        if (!Settings.canDrawOverlays(this)) {
            Toast.makeText(this, "Permite mostrar sobre otras aplicaciones para usar la burbuja.", Toast.LENGTH_LONG).show()
            startActivityForResult(Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION, Uri.parse("package:" + packageName)), 411)
            return
        }
        val enabled = prefs.getBoolean("bubble_enabled", false)
        if (enabled) {
            prefs.edit().putBoolean("bubble_enabled", false).apply()
            sendServiceAction(RadarService.ACTION_HIDE)
            Toast.makeText(this, "Burbuja desactivada", Toast.LENGTH_SHORT).show()
        } else {
            prefs.edit().putBoolean("bubble_enabled", true).apply()
            sendServiceAction(RadarService.ACTION_SHOW)
            Toast.makeText(this, "Burbuja activada", Toast.LENGTH_SHORT).show()
        }
    }

    private fun sendServiceAction(action: String) {
        try {
            val intent = Intent(this, RadarService::class.java).setAction(action)
            if (android.os.Build.VERSION.SDK_INT >= 26) startForegroundService(intent) else startService(intent)
        } catch (_: Exception) {
            Toast.makeText(this, "No se pudo cambiar la burbuja. Revisa el permiso de ventana flotante.", Toast.LENGTH_LONG).show()
        }
    }

    override fun onResume() {
        super.onResume()
        refreshBubbleButton()
    }

    private fun enableRadarNotifications() {
        if (android.os.Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != android.content.pm.PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(android.Manifest.permission.POST_NOTIFICATIONS), 410)
        } else {
            startRadarBackgroundService()
        }
    }

    private fun startRadarBackgroundService() {
        try {
            val intent = Intent(this, RadarService::class.java)
            if (android.os.Build.VERSION.SDK_INT >= 26) startForegroundService(intent) else startService(intent)
        } catch (_: Exception) {
            Toast.makeText(this, "No se pudo iniciar el servicio de alertas. Revisa los permisos.", Toast.LENGTH_LONG).show()
        }
    }

    override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<out String>, grantResults: IntArray) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == 410) startRadarBackgroundService()
    }

    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == 411 && Settings.canDrawOverlays(this)) {
            prefs.edit().putBoolean("bubble_enabled", true).apply()
            sendServiceAction(RadarService.ACTION_SHOW)
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        web.saveState(outState)
        super.onSaveInstanceState(outState)
    }

    @Deprecated("Deprecated in Android")
    override fun onBackPressed() {
        if (prefs.getBoolean("bubble_enabled", false) && Settings.canDrawOverlays(this)) {
            sendServiceAction(RadarService.ACTION_SHOW)
            moveTaskToBack(true)
        } else {
            super.onBackPressed()
        }
    }

    override fun onDestroy() {
        if (::web.isInitialized) {
            web.stopLoading()
            web.destroy()
        }
        super.onDestroy()
    }
}
