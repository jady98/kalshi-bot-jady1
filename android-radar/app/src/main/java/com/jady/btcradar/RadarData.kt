package com.jady.btcradar
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
data class Snapshot(val price:Double,val prediction:Double,val up:Int,val buyers:Int,val time:String)
object RadarData {
 private fun read(path:String):String { val c=URL("https://api.exchange.coinbase.com"+path).openConnection() as HttpURLConnection
 c.connectTimeout=5000; c.readTimeout=5000; c.setRequestProperty("User-Agent","BTC-Radar"); return try { c.inputStream.bufferedReader().use{it.readText()} } finally { c.disconnect() } }
 fun load():Snapshot {
  val ticker=JSONObject(read("/products/BTC-USD/ticker")); val price=ticker.getString("price").toDouble()
  val book=JSONObject(read("/products/BTC-USD/book?level=2"))
  fun depth(k:String):Double { val a=book.getJSONArray(k); var s=0.0; for(i in 0 until minOf(20,a.length())) s+=a.getJSONArray(i).getString(1).toDouble(); return s }
  val bid=depth("bids"); val ask=depth("asks"); val buyers=if(bid+ask>0) (100*bid/(bid+ask)).toInt().coerceIn(0,100) else 50
  val candles=org.json.JSONArray(read("/products/BTC-USD/candles?granularity=60"))
  val now=if(candles.length()>0) candles.getJSONArray(0).getDouble(4) else price
  val past=if(candles.length()>14) candles.getJSONArray(14).getDouble(4) else now
  val momentum=if(past>0) (now-past)/past else 0.0
  val up=(50+momentum*9000+(buyers-50)*0.35).toInt().coerceIn(1,99)
  val predicted=price*(1+momentum*0.25)
  val seconds=900-(System.currentTimeMillis()/1000%900)
  return Snapshot(price,predicted,up,buyers,"%02d:%02d".format(seconds/60,seconds%60))
 }
}
