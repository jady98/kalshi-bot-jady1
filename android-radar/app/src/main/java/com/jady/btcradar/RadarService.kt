package com.jady.btcradar
import android.app.*
import android.content.Intent
import android.graphics.Color
import android.graphics.PixelFormat
import android.os.*
import android.provider.Settings
import android.view.*
import android.widget.*
class RadarService:Service(){
 private lateinit var wm:WindowManager
 private var panel:LinearLayout?=null
 private val handler=Handler(Looper.getMainLooper())
 private var lastSignal=""
 private val task=object:Runnable{override fun run(){
  Thread{try{val d=RadarData.load();handler.post{
   panel?.findViewWithTag<TextView>("p")?.text="BTC $"+ "%,.0f".format(d.price)
   panel?.findViewWithTag<TextView>("t")?.text="ALCISTA "+d.up+"%  |  BAJISTA "+(100-d.up)+"%"
   panel?.findViewWithTag<ProgressBar>("tb")?.progress=d.up
   panel?.findViewWithTag<TextView>("b")?.text="COMPRADORES "+d.buyers+"%  |  VENDEDORES "+(100-d.buyers)+"%"
   panel?.findViewWithTag<ProgressBar>("bb")?.progress=d.buyers
   panel?.findViewWithTag<TextView>("c")?.text="CIERRE 15M  $"+ "%,.2f".format(d.prediction)+"  •  "+d.time
   val signal=if(d.up>=75)"ALCISTA" else if(d.up<=25)"BAJISTA" else ""
   if(signal.isNotEmpty()&&signal!=lastSignal){val n=Notification.Builder(this@RadarService,"btc").setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle("BTC: "+signal).setContentText("Probabilidad estimada "+(if(signal=="ALCISTA")d.up else 100-d.up)+"%").build();getSystemService(NotificationManager::class.java).notify(9,n);lastSignal=signal}
   if(signal.isEmpty())lastSignal=""
  }}catch(_:Exception){} }.start();handler.postDelayed(this,5000)
 }}
 override fun onCreate(){super.onCreate()
 val nm=getSystemService(NotificationManager::class.java)
 if(Build.VERSION.SDK_INT>=26)nm.createNotificationChannel(NotificationChannel("btc","Alertas BTC",NotificationManager.IMPORTANCE_DEFAULT))
 startForeground(1,Notification.Builder(this,"btc").setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle("BTC Radar activo").setContentText("Monitoreando BTC/USD").build())
 if(Settings.canDrawOverlays(this))show()
 handler.post(task)
 }
 private fun show(){wm=getSystemService(WINDOW_SERVICE) as WindowManager
 val box=LinearLayout(this).apply{orientation=1;setPadding(14,10,14,10);setBackgroundColor(Color.rgb(11,17,26))}
 fun line(s:String,tag:String,size:Float)=TextView(this).apply{text=s;this.tag=tag;textSize=size;setTextColor(Color.WHITE);setPadding(3,4,3,4)}
 box.addView(line("BTC RADAR  ⠿","h",15f))
 box.addView(line("BTC —","p",19f));box.addView(line("ALCISTA — | BAJISTA —","t",14f))
 box.addView(ProgressBar(this,null,android.R.attr.progressBarStyleHorizontal).apply{max=100;progress=50;tag="tb"})
 box.addView(line("COMPRADORES — | VENDEDORES —","b",12f))
 box.addView(ProgressBar(this,null,android.R.attr.progressBarStyleHorizontal).apply{max=100;progress=50;tag="bb"})
 box.addView(line("CIERRE 15M —","c",12f))
 val type=if(Build.VERSION.SDK_INT>=26)WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE
 val lp=WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT,WindowManager.LayoutParams.WRAP_CONTENT,type,WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE,PixelFormat.TRANSLUCENT).apply{gravity=Gravity.TOP or Gravity.END;x=15;y=180}
 box.setOnTouchListener(object:View.OnTouchListener{var sx=0f;var sy=0f;var ox=0;var oy=0
 override fun onTouch(v:View,e:MotionEvent):Boolean{when(e.action){MotionEvent.ACTION_DOWN->{sx=e.rawX;sy=e.rawY;ox=lp.x;oy=lp.y;return true};MotionEvent.ACTION_MOVE->{lp.x=(ox+sx-e.rawX).toInt();lp.y=(oy+e.rawY-sy).toInt();wm.updateViewLayout(box,lp);return true}};return true}})
 wm.addView(box,lp);panel=box
 }
 override fun onStartCommand(i:Intent?,f:Int,id:Int)=START_STICKY
 override fun onDestroy(){handler.removeCallbacks(task);if(panel!=null)wm.removeView(panel);super.onDestroy()}
 override fun onBind(i:Intent?):IBinder?=null
}
