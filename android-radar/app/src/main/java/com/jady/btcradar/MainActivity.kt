package com.jady.btcradar

import android.app.Activity
import android.content.Intent
import android.graphics.Color
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.view.Gravity
import android.view.View
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.FrameLayout
import android.widget.ProgressBar
import android.widget.Toast

class MainActivity : Activity() {
    private lateinit var web: WebView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.statusBarColor = Color.rgb(11, 17, 26)
        window.navigationBarColor = Color.rgb(11, 17, 26)

        val root = FrameLayout(this).apply {
            setBackgroundColor(Color.rgb(11, 17, 26))
        }

        web = WebView(this).apply {
            setBackgroundColor(Color.rgb(11, 17, 26))
            settings.javaScriptEnabled = true
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

        val bubbleButton = Button(this).apply {
            text = "BURBUJA"
            textSize = 11f
            setTextColor(Color.WHITE)
            setBackgroundColor(Color.rgb(20, 125, 100))
            setOnClickListener {
                if (!Settings.canDrawOverlays(this@MainActivity)) {
                    Toast.makeText(this@MainActivity, "Activa Permitir mostrar sobre otras apps y vuelve a tocar BURBUJA.", Toast.LENGTH_LONG).show()
                    startActivity(Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION, Uri.parse("package:" + packageName)))
                } else {
                    startForegroundService(Intent(this@MainActivity, RadarService::class.java))
                    Toast.makeText(this@MainActivity, "Burbuja flotante activada", Toast.LENGTH_SHORT).show()
                }
            }
        }
        val buttonParams = FrameLayout.LayoutParams(
            (104 * resources.displayMetrics.density).toInt(),
            (46 * resources.displayMetrics.density).toInt(),
            Gravity.BOTTOM or Gravity.END
        ).apply {
            val margin = (16 * resources.displayMetrics.density).toInt()
            setMargins(margin, margin, margin, margin + (18 * resources.displayMetrics.density).toInt())
        }
        root.addView(bubbleButton, buttonParams)

        setContentView(root)
        if (savedInstanceState == null) {
            web.loadUrl("https://kalshi-bot-jady1.onrender.com/")
        } else {
            web.restoreState(savedInstanceState)
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        web.saveState(outState)
        super.onSaveInstanceState(outState)
    }

    @Deprecated("Deprecated in Android")
    override fun onBackPressed() {
        if (::web.isInitialized && web.canGoBack()) web.goBack() else super.onBackPressed()
    }

    override fun onDestroy() {
        if (::web.isInitialized) {
            web.stopLoading()
            web.destroy()
        }
        super.onDestroy()
    }
}
