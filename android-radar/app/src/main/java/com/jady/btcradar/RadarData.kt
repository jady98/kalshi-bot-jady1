package com.jady.btcradar
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

data class Candle(val low:Float,val high:Float,val open:Float,val close:Float,val ema9:Float,val ema21:Float)
data class Snapshot(val price:Double,val prediction:Double,val up:Int,val buyers:Int,val time:String,val candles:List<Candle>)

object RadarData {
 private fun read(path:String):String {
  val c=URL("https://api.exchange.coinbase.com"+path).openConnection() as HttpURLConnection
  c.connectTimeout=5000; c.readTimeout=5000; c.setRequestProperty("User-Agent","BTC-Radar")
  return try { c.inputStream.bufferedReader().use{it.readText()} } finally { c.disconnect() }
 }
 fun load():Snapshot {
  val ticker=JSONObject(read("/products/BTC-USD/ticker"))
  val price=ticker.getString("price").toDouble()
  val book=JSONObject(read("/products/BTC-USD/book?level=2"))
  fun depth(k:String):Double { val a=book.getJSONArray(k); var s=0.0; for(i in 0 until minOf(20,a.length())) s+=a.getJSONArray(i).getString(1).toDouble(); return s }
  val bid=depth("bids"); val ask=depth("asks")
  val buyers=if(bid+ask>0) (100*bid/(bid+ask)).toInt().coerceIn(0,100) else 50
  val raw=org.json.JSONArray(read("/products/BTC-USD/candles?granularity=60"))
  val rows=ArrayList<DoubleArray>()
  for(i in 0 until raw.length()) { val a=raw.getJSONArray(i); rows.add(doubleArrayOf(a.getDouble(0),a.getDouble(1),a.getDouble(2),a.getDouble(3),a.getDouble(4))) }
  rows.reverse()
  val closes=rows.map{it[4]}
  fun ema(period:Int):List<Double> {
   if(closes.isEmpty()) return emptyList()
   val alpha=2.0/(period+1); val out=ArrayList<Double>(); var v=closes.first()
   for(c in closes){v=alpha*c+(1-alpha)*v;out.add(v)}
   return out
  }
  val e9=ema(9); val e21=ema(21)
  val candles=rows.takeLast(45).mapIndexed { i,r ->
   val ix=rows.size-45.coerceAtMost(rows.size)+i
   Candle(r[1].toFloat(),r[2].toFloat(),r[3].toFloat(),r[4].toFloat(),e9[ix].toFloat(),e21[ix].toFloat())
  }
  val now=closes.lastOrNull() ?: price
  val past=if(closes.size>14) closes[closes.size-15] else now
  val momentum=if(past>0) (now-past)/past else 0.0
  val up=(50+momentum*9000+(buyers-50)*0.35).toInt().coerceIn(1,99)
  val predicted=price*(1+momentum*0.25)
  val seconds=900-(System.currentTimeMillis()/1000%900)
  return Snapshot(price,predicted,up,buyers,"%02d:%02d".format(seconds/60,seconds%60),candles)
 }
}
