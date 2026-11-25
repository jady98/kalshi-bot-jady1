package com.jady.btcradar

import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.Locale
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min

data class Candle(val time: Long, val low: Float, val high: Float, val open: Float, val close: Float, val ema9: Float, val ema21: Float)
data class Snapshot(
    val price: Double, val prediction: Double, val up: Int, val time: String,
    val candles: List<Candle>, val direction: String, val alertType: String,
    val alertMessage: String, val alertKey: String, val alertTimestamp: Long,
    val entryStage: String, val entryDirection: String, val manualSide: String,
    val secondsToClose: Int, val pattern: String, val patternDetail: String,
    val momentumPct: Double, val rsi: Double
)

object RadarData {
    private const val BASE = "https://api.exchange.coinbase.com"
    @Volatile private var cached: Snapshot? = null
    @Volatile private var cachedAt = 0L

    @Synchronized fun load(): Snapshot {
        val nowMs = System.currentTimeMillis()
        cached?.let { if (nowMs - cachedAt < 5000L) return it }
        val ticker = getObject("/products/BTC-USD/ticker")
        val raw = getArray("/products/BTC-USD/candles?granularity=60")
        val rows = mutableListOf<Raw>()
        for (i in 0 until raw.length()) {
            val a = raw.optJSONArray(i) ?: continue
            if (a.length() < 6) continue
            val t=a.optLong(0); val low=a.optDouble(1); val high=a.optDouble(2)
            val open=a.optDouble(3); val close=a.optDouble(4)
            if (t>0 && listOf(low,high,open,close).all { it.isFinite() && it>0 } && high>=low)
                rows.add(Raw(t,low,high,open,close))
        }
        rows.sortBy { it.time }
        require(rows.size >= 30) { "Coinbase todavía no entregó suficientes velas de un minuto." }
        val last=rows.last()
        val price=ticker.optString("price","").toDoubleOrNull()?.takeIf { it.isFinite() && it>0 } ?: last.close
        val closes=rows.map { it.close }
        val e9=ema(closes,9); val e21=ema(closes,21)
        val candles=rows.indices.map { i -> val c=rows[i]; Candle(c.time,c.low.toFloat(),c.high.toFloat(),c.open.toFloat(),c.close.toFloat(),e9[i].toFloat(),e21[i].toFloat()) }
        val base=closes[max(0,closes.lastIndex-5)]
        val momentum=if(base>0) (price/base-1.0)*100.0 else 0.0
        val rsi=rsi(closes,14)
        val emaUp=e9.last()>e21.last(); val emaDown=e9.last()<e21.last()
        val bullCandle=bullEngulf(rows)||hammer(rows); val bearCandle=bearEngulf(rows)||star(rows)
        val previous=rows.dropLast(1).takeLast(20)
        val breakUp=previous.isNotEmpty() && last.close>previous.maxOf{it.high}
        val breakDown=previous.isNotEmpty() && last.close<previous.minOf{it.low}
        val upCount=listOf(emaUp,momentum>=0.035,bullCandle||breakUp,rsi in 50.0..72.0).count{it}
        val downCount=listOf(emaDown,momentum<=-0.035,bearCandle||breakDown,rsi in 28.0..50.0).count{it}
        val direction=when { upCount>=3&&upCount>downCount->"ALCISTA"; downCount>=3&&downCount>upCount->"BAJISTA"; else->"INDECISO" }
        val pattern=when {
            breakUp->"RUPTURA ALCISTA"; breakDown->"RUPTURA BAJISTA"
            bullEngulf(rows)->"ENVOLVENTE ALCISTA"; bearEngulf(rows)->"ENVOLVENTE BAJISTA"
            hammer(rows)->"MARTILLO"; star(rows)->"ESTRELLA FUGAZ"
            doubleBottom(rows)->"POSIBLE DOBLE SUELO"; doubleTop(rows)->"POSIBLE DOBLE TECHO"
            else->"SIN PATRÓN DESTACADO"
        }
        val confirmed=direction!="INDECISO"
        val alertMessage=if(confirmed) {
            val facts=listOf(if(emaUp&&direction=="ALCISTA"||emaDown&&direction=="BAJISTA")"EMA 9/21 alineadas" else "",
                if(direction=="ALCISTA"&&momentum>0||direction=="BAJISTA"&&momentum<0)"impulso 5m compatible" else "",
                if(breakUp||breakDown)"ruptura de rango" else if(bullCandle||bearCandle)"vela compatible" else "",
                "RSI \${rsi.toInt()}").filter{it.isNotBlank()}
            "Señal $direction confirmada por \${facts.joinToString(", ")}. Patrón: $pattern."
        } else "Sin señal importante confirmada. El radar continúa vigilando."
        val strength=min(28.0,abs(momentum)*230.0)+if(emaUp&&momentum>0||emaDown&&momentum<0)9.0 else 0.0
        val upScore=(50.0+(if(emaUp)1 else if(emaDown)-1 else 0)*strength).toInt().coerceIn(5,95)
        val projection=(momentum*3.0/100.0).coerceIn(-0.005,0.005)
        val epoch=System.currentTimeMillis()/1000L
        val seconds=(900L-epoch%900L).toInt()
        val result=Snapshot(price,price*(1+projection),upScore,"%02d:%02d".format(Locale.US,seconds/60,seconds%60),
            candles.takeLast(90),direction,if(confirmed)"ENTRADA" else "",
            alertMessage,if(confirmed)"$direction|\${last.time}" else "",if(confirmed)epoch else 0L,
            if(confirmed)"ENTRADA" else "SIN_SEÑAL",direction,"",seconds,pattern,
            "EMA9 \${if(emaUp)"sobre" else "bajo"} EMA21 · momentum 5m \${String.format(Locale.US,"%+.3f",momentum)}% · RSI \${rsi.toInt()}",
            momentum,rsi)
        cached=result; cachedAt=nowMs
        return result
    }

    private data class Raw(val time:Long,val low:Double,val high:Double,val open:Double,val close:Double)
    private fun getObject(path:String):JSONObject { val c=connection(path); return try{JSONObject(c.inputStream.bufferedReader().use{it.readText()})}finally{c.disconnect()} }
    private fun getArray(path:String):JSONArray { val c=connection(path); return try{JSONArray(c.inputStream.bufferedReader().use{it.readText()})}finally{c.disconnect()} }
    private fun connection(path:String)= (URL(BASE+path).openConnection() as HttpURLConnection).apply {
        connectTimeout=7000; readTimeout=7000; setRequestProperty("User-Agent","BTC-Radar-Android/2.0"); setRequestProperty("Accept","application/json")
    }
    private fun ema(v:List<Double>,p:Int):List<Double>{val a=2.0/(p+1.0);var x=v.first();return v.mapIndexed{i,n->x=if(i==0)n else a*n+(1-a)*x;x}}
    private fun rsi(v:List<Double>,p:Int):Double{if(v.size<=p)return 50.0;var g=0.0;var l=0.0;for(i in v.size-p until v.size){val d=v[i]-v[i-1];if(d>=0)g+=d else l-=d};if(l==0.0)return 100.0;val rs=(g/p)/(l/p);return 100.0-100.0/(1.0+rs)}
    private fun bullEngulf(r:List<Raw>):Boolean{if(r.size<2)return false;val a=r[r.lastIndex-1];val b=r.last();return a.close<a.open&&b.close>b.open&&b.open<=a.close&&b.close>=a.open}
    private fun bearEngulf(r:List<Raw>):Boolean{if(r.size<2)return false;val a=r[r.lastIndex-1];val b=r.last();return a.close>a.open&&b.close<b.open&&b.open>=a.close&&b.close<=a.open}
    private fun hammer(r:List<Raw>):Boolean{val c=r.last();val b=abs(c.close-c.open).coerceAtLeast(c.close*0.00001);return min(c.open,c.close)-c.low>=b*2&&(c.high-max(c.open,c.close))<=b*.7&&c.close>=c.open}
    private fun star(r:List<Raw>):Boolean{val c=r.last();val b=abs(c.close-c.open).coerceAtLeast(c.close*0.00001);return c.high-max(c.open,c.close)>=b*2&&(min(c.open,c.close)-c.low)<=b*.7&&c.close<=c.open}
    private fun doubleTop(r:List<Raw>):Boolean{if(r.size<12)return false;val x=r.takeLast(24);val p=x.indices.filter{i->i in 1 until x.lastIndex&&x[i].high>=x[i-1].high&&x[i].high>=x[i+1].high};if(p.size<2)return false;val a=x[p[p.lastIndex-1]].high;val b=x[p.last()].high;return abs(a-b)/max(a,b)<.0015&&p.last()-p[p.lastIndex-1]>=4}
    private fun doubleBottom(r:List<Raw>):Boolean{if(r.size<12)return false;val x=r.takeLast(24);val p=x.indices.filter{i->i in 1 until x.lastIndex&&x[i].low<=x[i-1].low&&x[i].low<=x[i+1].low};if(p.size<2)return false;val a=x[p[p.lastIndex-1]].low;val b=x[p.last()].low;return abs(a-b)/max(a,b)<.0015&&p.last()-p[p.lastIndex-1]>=4}
}