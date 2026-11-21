package com.jady.btcradar

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.view.View

class CandleChartView(context:Context):View(context) {
 private var data:List<Candle> = emptyList()
 private val grid=Paint( Paint.ANTI_ALIAS_FLAG).apply{color=Color.rgb(42,53,67);strokeWidth=1f}
 private val up=Paint(Paint.ANTI_ALIAS_FLAG).apply{color=Color.rgb(35,205,132);strokeWidth=2f}
 private val down=Paint(Paint.ANTI_ALIAS_FLAG).apply{color=Color.rgb(245,82,102);strokeWidth=2f}
 private val emaFast=Paint(Paint.ANTI_ALIAS_FLAG).apply{color=Color.rgb(190,105,255);strokeWidth=2.5f;style=Paint.Style.STROKE}
 private val emaSlow=Paint(Paint.ANTI_ALIAS_FLAG).apply{color=Color.rgb(70,160,255);strokeWidth=2.5f;style=Paint.Style.STROKE}
 private val label=Paint(Paint.ANTI_ALIAS_FLAG).apply{color=Color.LTGRAY;textSize=11f}
 fun setCandles(value:List<Candle>){data=value;invalidate()}
 override fun onDraw(canvas:Canvas){
  super.onDraw(canvas)
  canvas.drawColor(Color.rgb(11,17,26))
  if(data.isEmpty()){canvas.drawText("Esperando velas de 1 minuto…",16f,height/2f,label);return}
  val left=12f;val right=width-12f;val top=18f;val bottom=height-22f
  val hi=data.maxOf{it.high};val lo=data.minOf{it.low};val range=(hi-lo).coerceAtLeast(1f)
  fun y(v:Float)=bottom-(v-lo)/range*(bottom-top)
  for(i in 0..3){val yy=top+(bottom-top)*i/3f;canvas.drawLine(left,yy,right,yy,grid)}
  val slot=(right-left)/data.size;val body=(slot*0.58f).coerceAtLeast(3f)
  val fast=android.graphics.Path();val slow=android.graphics.Path()
  data.forEachIndexed{index,c->
   val x=left+slot*(index+0.5f);val p=if(c.close>=c.open)up else down
   canvas.drawLine(x,y(c.high),x,y(c.low),p)
   val t=y(maxOf(c.open,c.close));val b=y(minOf(c.open,c.close))
   canvas.drawRect(x-body/2,t,x+body/2,maxOf(t+2f,b),p)
   if(index==0){fast.moveTo(x,y(c.ema9));slow.moveTo(x,y(c.ema21))}
   else {fast.lineTo(x,y(c.ema9));slow.lineTo(x,y(c.ema21))}
  }
  canvas.drawPath(fast,emaFast);canvas.drawPath(slow,emaSlow)
  canvas.drawText("EMA 9",left,12f,emaFast);canvas.drawText("EMA 21",left+64f,12f,emaSlow)
 }
}
