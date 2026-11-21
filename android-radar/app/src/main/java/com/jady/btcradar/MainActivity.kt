package com.jady.btcradar

import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.graphics.Color
import android.view.ViewGroup
import android.widget.*

class MainActivity:Activity(){
 override fun onCreate(savedInstanceState:Bundle?){
  super.onCreate(savedInstanceState)
  window.statusBarColor=Color.rgb(11,17,26);window.navigationBarColor=Color.rgb(11,17,26)
  val root=LinearLayout(this).apply{orientation=1;setPadding(18,22,18,18);setBackgroundColor(Color.rgb(11,17,26))}
  fun txt(s:String,n:Float=17f)=TextView(this).apply{text=s;textSize=n;setTextColor(Color.WHITE);setPadding(6,9,6,9)}
  root.addView(txt("BTC RADAR  •  COINBASE LIVE",20f))
  val price=txt("Conectando…",26f);root.addView(price)
  val prediction=txt("PREDICCIÓN AL CIERRE 15 MIN: —");root.addView(prediction)
  val timer=txt("CIERRE EN —");root.addView(timer)
  val trend=txt("ALCISTA —%  |  BAJISTA —%",20f);root.addView(trend)
  val trendbar=ProgressBar(this,null,android.R.attr.progressBarStyleHorizontal).apply{max=100;progress=50}
  root.addView(trendbar,LinearLayout.LayoutParams(-1,18))
  val pressure=txt("COMPRADORES —%  |  VENDEDORES —%");root.addView(pressure)
  val buybar=ProgressBar(this,null,android.R.attr.progressBarStyleHorizontal).apply{max=100;progress=50}
  root.addView(buybar,LinearLayout.LayoutParams(-1,18))
  val chart=CandleChartView(this)
  root.addView(chart,LinearLayout.LayoutParams(-1,(220*resources.displayMetrics.density).toInt()))
  root.addView(txt("Velas de 1 minuto  •  EMA 9 / EMA 21",12f))
  root.addView(txt("Porcentajes estimados; no garantizan el movimiento.",12f))
  root.addView(Button(this).apply{text="ACTIVAR RADAR FLOTANTE";setOnClickListener{
   if(!Settings.canDrawOverlays(this@MainActivity))startActivity(Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION,Uri.parse("package:"+packageName)))
   else {startForegroundService(Intent(this@MainActivity,RadarService::class.java));Toast.makeText(this@MainActivity,"Radar activado",Toast.LENGTH_SHORT).show()}
  }})
  root.addView(Button(this).apply{text="DETENER RADAR";setOnClickListener{stopService(Intent(this@MainActivity,RadarService::class.java))}})
  setContentView(ScrollView(this).apply{addView(root)})
  val h=android.os.Handler(mainLooper)
  val job=object:Runnable{override fun run(){
   Thread{try{val d=RadarData.load();runOnUiThread{
    price.text="BTC/USD  $"+ "%,.2f".format(d.price)
    prediction.text="PREDICCIÓN AL CIERRE: $"+ "%,.2f".format(d.prediction)
    timer.text="CIERRE EN "+d.time
    trend.text="ALCISTA "+d.up+"%  |  BAJISTA "+(100-d.up)+"%"
    trendbar.progress=d.up
    pressure.text="COMPRADORES "+d.buyers+"%  |  VENDEDORES "+(100-d.buyers)+"%"
    buybar.progress=d.buyers
    chart.setCandles(d.candles)
   }}catch(_:Exception){} }.start();h.postDelayed(this,4000)
  }}
  h.post(job)
 }
}
