package com.mqx.aibot

import android.annotation.SuppressLint
import android.graphics.Bitmap
import android.os.Bundle
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.WebChromeClient
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AppCompatActivity
import androidx.webkit.WebViewCompat
import androidx.webkit.WebViewFeature

class MainActivity : AppCompatActivity() {
    private lateinit var web: WebView
    private var docStartOk = false

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        web = WebView(this)
        setContentView(web, ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT))

        val s = web.settings
        s.javaScriptEnabled = true
        s.domStorageEnabled = true
        s.databaseEnabled = true
        s.mediaPlaybackRequiresUserGesture = false
        s.useWideViewPort = true
        s.loadWithOverviewMode = true
        s.userAgentString = s.userAgentString.replace("; wv", "").replace("Version/4.0 ", "")

        CookieManager.getInstance().setAcceptCookie(true)
        CookieManager.getInstance().setAcceptThirdPartyCookies(web, true)

        val script = assets.open("bot.js").bufferedReader().use { it.readText() }

        // Run the bot before any page script so the WebSocket hook sees every price frame.
        if (WebViewFeature.isFeatureSupported(WebViewFeature.DOCUMENT_START_SCRIPT)) {
            try {
                val origins = setOf(
                    "https://market-qx.info", "https://*.market-qx.info",
                    "https://market-qx.pro", "https://*.market-qx.pro",
                    "https://qxbroker.com", "https://*.qxbroker.com",
                    "https://quotex.io", "https://*.quotex.io",
                    "https://quotex.com", "https://*.quotex.com"
                )
                WebViewCompat.addDocumentStartJavaScript(web, script, origins)
                docStartOk = true
            } catch (e: Exception) {
                docStartOk = false
            }
        }

        web.webViewClient = object : WebViewClient() {
            override fun onPageStarted(view: WebView, url: String?, favicon: Bitmap?) {
                super.onPageStarted(view, url, favicon)
                if (!docStartOk) view.evaluateJavascript(script, null) // fallback (guarded against double load)
            }
        }
        web.webChromeClient = WebChromeClient()
        web.loadUrl("https://market-qx.info/en/demo-trade")
    }

    @Deprecated("Deprecated in Java")
    override fun onBackPressed() {
        if (web.canGoBack()) web.goBack() else super.onBackPressed()
    }

    override fun onDestroy() {
        web.destroy()
        super.onDestroy()
    }
}
