
import os, time, logging, threading
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("btc-radar")
LOCK = threading.RLock()
CANDLES = deque(maxlen=1800)
PRICE, UPDATED, SOURCE, ERROR = 0.0, 0.0, "SIN CONEXIÓN", ""
STARTED = False
START_LOCK = threading.Lock()

def make_session():
    s = requests.Session()
    retry = Retry(total=2, connect=2, read=1, backoff_factor=.4,
                  status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent":"BTC-Radar-PRO/6.0","Accept":"application/json"})
    return s

SESSION = make_session()

def num(x, default=0.0):
    try: return float(x)
    except (TypeError, ValueError): return default

def add_price(price, volume=0, timestamp=None):
    global PRICE, UPDATED
    if price <= 0: return
    stamp = int((time.time() if timestamp is None else timestamp)//60)*60
    with LOCK:
        PRICE, UPDATED = price, time.time()
        if not CANDLES:
            CANDLES.append(dict(time=stamp,open=price,high=price,low=price,close=price,volume=max(0,volume)))
        elif stamp == CANDLES[-1]["time"]:
            c=CANDLES[-1]; c["high"]=max(c["high"],price); c["low"]=min(c["low"],price)
            c["close"]=price; c["volume"]+=max(0,volume)
        elif stamp > CANDLES[-1]["time"]:
            while CANDLES[-1]["time"]+60 < stamp:
                prev=CANDLES[-1]["close"]; t=CANDLES[-1]["time"]+60
                CANDLES.append(dict(time=t,open=prev,high=prev,low=prev,close=prev,volume=0))
            CANDLES.append(dict(time=stamp,open=price,high=price,low=price,close=price,volume=max(0,volume)))

def coinbase():
    r=SESSION.get("https://api.exchange.coinbase.com/products/BTC-USD/ticker",timeout=6); r.raise_for_status()
    p=num(r.json().get("price"))
    if p<=0: raise ValueError("Coinbase: precio inválido")
    return p

def binance():
    r=SESSION.get("https://api.binance.us/api/v3/ticker/price",params={"symbol":"BTCUSDT"},timeout=6); r.raise_for_status()
    p=num(r.json().get("price"))
    if p<=0: raise ValueError("Binance.US: precio inválido")
    return p

def kraken():
    r=SESSION.get("https://api.kraken.com/0/public/Ticker",params={"pair":"XBTUSD"},timeout=6); r.raise_for_status()
    data=r.json().get("result",{})
    if not data: raise ValueError("Kraken: respuesta vacía")
    p=num(next(iter(data.values())).get("c",[0])[0])
    if p<=0: raise ValueError("Kraken: precio inválido")
    return p

def coingecko():
    r=SESSION.get("https://api.coingecko.com/api/v3/simple/price",
                  params={"ids":"bitcoin","vs_currencies":"usd"},timeout=8); r.raise_for_status()
    p=num(r.json().get("bitcoin",{}).get("usd"))
    if p<=0: raise ValueError("CoinGecko: precio inválido")
    return p

def live_price():
    global SOURCE, ERROR
    for name, fn in [("Coinbase",coinbase),("Binance.US",binance),("Kraken",kraken),("CoinGecko",coingecko)]:
        try:
            p=fn()
            with LOCK: SOURCE, ERROR = name, ""
            return p
        except Exception as e:
            log.warning("%s no disponible: %s",name,e)
            ERROR=f"{name}: {str(e)[:100]}"
    raise RuntimeError("Todas las fuentes de precio fallaron. "+ERROR)

def load_history():
    global ERROR
    # Binance.US, luego Coinbase, luego Kraken.
    try:
        r=SESSION.get("https://api.binance.us/api/v3/klines",
          params={"symbol":"BTCUSDT","interval":"1m","limit":500},timeout=10); r.raise_for_status()
        rows=r.json()
        parsed=[dict(time=int(x[0]//1000),open=num(x[1]),high=num(x[2]),low=num(x[3]),close=num(x[4]),volume=num(x[5])) for x in rows]
        if parsed:
            with LOCK: CANDLES.clear(); CANDLES.extend(parsed)
            log.info("Historial Binance.US: %d velas",len(parsed)); return
    except Exception as e: log.warning("Historial Binance.US falló: %s",e)
    try:
        end=datetime.now(timezone.utc); start=end-timedelta(minutes=280)
        r=SESSION.get("https://api.exchange.coinbase.com/products/BTC-USD/candles",
          params={"granularity":60,"start":start.isoformat(),"end":end.isoformat()},timeout=10); r.raise_for_status()
        rows=sorted(r.json(),key=lambda x:x[0])
        parsed=[dict(time=int(x[0]),low=num(x[1]),high=num(x[2]),open=num(x[3]),close=num(x[4]),volume=num(x[5])) for x in rows if len(x)>=6]
        if parsed:
            with LOCK: CANDLES.clear(); CANDLES.extend(parsed)
            log.info("Historial Coinbase: %d velas",len(parsed)); return
    except Exception as e: log.warning("Historial Coinbase falló: %s",e)
    try:
        r=SESSION.get("https://api.kraken.com/0/public/OHLC",
          params={"pair":"XBTUSD","interval":1,"since":int(time.time())-280*60},timeout=10); r.raise_for_status()
        result=r.json().get("result",{}); key=next((k for k in result if k!="last"),None)
        rows=result.get(key,[]) if key else []
        parsed=[dict(time=int(x[0]),open=num(x[1]),high=num(x[2]),low=num(x[3]),close=num(x[4]),volume=num(x[6])) for x in rows if len(x)>=7]
        if parsed:
            with LOCK: CANDLES.clear(); CANDLES.extend(parsed[-500:])
            log.info("Historial Kraken: %d velas",len(parsed)); return
    except Exception as e: log.warning("Historial Kraken falló: %s",e)
    ERROR="Historial no disponible; acumulando velas en vivo."

def price_worker():
    failures=0
    while True:
        try:
            p=live_price(); add_price(p); failures=0; time.sleep(3)
        except Exception as e:
            log.error("%s",e); failures+=1
            time.sleep(min(15,3+failures*2))

def ema(values,period):
    if not values:return []
    k=2/(period+1); out=[values[0]]
    for v in values[1:]: out.append(v*k+out[-1]*(1-k))
    return out

def rsi(values,period=14):
    if len(values)<period+1:return 50.0
    changes=[values[i]-values[i-1] for i in range(1,len(values))]
    gains=[max(x,0) for x in changes]; losses=[max(-x,0) for x in changes]
    ag=sum(gains[:period])/period; al=sum(losses[:period])/period
    for i in range(period,len(changes)):
        ag=(ag*(period-1)+gains[i])/period; al=(al*(period-1)+losses[i])/period
    if al==0:return 100.0 if ag else 50.0
    return 100-100/(1+ag/al)

def aggregate(data,minutes):
    if minutes==1:return list(data)
    groups={}
    for c in data:
        t=(c["time"]//(minutes*60))*(minutes*60)
        if t not in groups: groups[t]=dict(time=t,open=c["open"],high=c["high"],low=c["low"],close=c["close"],volume=c["volume"])
        else:
            g=groups[t]; g["high"]=max(g["high"],c["high"]); g["low"]=min(g["low"],c["low"])
            g["close"]=c["close"]; g["volume"]+=c["volume"]
    return sorted(groups.values(),key=lambda x:x["time"])

def analyze(data):
    closes=[c["close"] for c in data]
    if len(closes)<30:
        return dict(bull=50,bear=50,neutral=0,direction="REUNIENDO DATOS",score=0,rsi=50,ema9=None,ema21=None,vwap=None,histogram=0)
    fast,slow=ema(closes,9),ema(closes,21)
    totalv=totalval=0; vw=[]
    for c in data:
        typical=(c["high"]+c["low"]+c["close"])/3; totalv+=max(0,c["volume"]); totalval+=typical*max(0,c["volume"])
        vw.append(totalval/totalv if totalv else c["close"])
    rv=rsi(closes); macd=[a-b for a,b in zip(ema(closes,12),ema(closes,26))]
    hist=macd[-1]-ema(macd,9)[-1]; momentum=(closes[-1]/closes[-6]-1)*100 if closes[-6] else 0
    score=(2 if fast[-1]>slow[-1] else -2)+(1.5 if closes[-1]>vw[-1] else -1.5)+(1.5 if hist>0 else -1.5)+max(-2,min(2,momentum*8))
    if 50<rv<70:score+=1
    elif 30<rv<50:score-=1
    score=max(-8,min(8,score)); bull=50+score*5; bear=50-score*5; neutral=max(5,100-abs(score)/8*100)
    total=bull+bear+neutral; bull,bear,neutral=bull/total*100,bear/total*100,neutral/total*100
    direction="ALCISTA" if bull>bear and bull>neutral else ("BAJISTA" if bear>bull and bear>neutral else "INDECISO")
    return dict(bull=round(bull,1),bear=round(bear,1),neutral=round(neutral,1),direction=direction,score=round(score,2),rsi=round(rv,2),ema9=round(fast[-1],2),ema21=round(slow[-1],2),vwap=round(vw[-1],2),histogram=round(hist,4))

def forecast(data,price,pred):
    closes=[c["close"] for c in data if c["close"]>0]
    if len(closes)<30:return dict(target=None,change_pct=None,direction="ESPERANDO DATOS",range_pct=None)
    recent=closes[-30:]; rets=sorted([recent[i]/recent[i-1]-1 for i in range(1,len(recent)) if recent[i-1]>0])
    if len(rets)>4:rets=rets[2:-2]
    avg=sum(rets)/max(1,len(rets)); var=sum((x-avg)**2 for x in rets)/max(1,len(rets)); vol=var**.5
    raw=avg*15*100*.45+num(pred.get("score"))/8*vol*15*100*.55
    cap=max(.04,min(1.20,vol*(15**.5)*100*1.5)); pct=max(-cap,min(cap,raw))
    return dict(target=round(price*(1+pct/100),2),change_pct=round(pct,3),
                direction="SUBE" if pct>.015 else ("BAJA" if pct<-.015 else "LATERAL"),range_pct=round(cap,3))

HTML = r'''<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC Radar PRO</title>
<script src="https://unpkg.com/lightweight-charts@4.2.2/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{box-sizing:border-box}body{margin:0;background:#050610;color:#f5f4ff;font-family:Arial}header{padding:16px;border-bottom:1px solid #29263f;display:flex;justify-content:space-between}h1{font-size:20px;margin:0;color:#b98aff}main{max-width:1100px;margin:auto;padding:14px}.grid,.indicators,.bottom{display:grid;gap:10px}.grid{grid-template-columns:repeat(3,1fr)}.indicators{grid-template-columns:repeat(3,1fr);margin-top:12px}.bottom{grid-template-columns:repeat(4,1fr);margin-top:12px}.card,.tile,.status{background:#101225;border:1px solid #292b49;border-radius:12px;padding:13px;min-width:0}.label,small{display:block;color:#999bb9;font-size:12px;margin-bottom:8px}.pct{font-size:25px;font-weight:bold}.green{color:#00e39a}.red{color:#ff5574}.purple{color:#bb8cff}.direction{font-size:19px;font-weight:bold}.bar{height:6px;background:#292b49;border-radius:9px;overflow:hidden;margin-top:10px}.bar span{display:block;height:100%;background:#00e39a}.bar.red span{background:#ff5574}.bar.purple span{background:#bb8cff}.tabs{display:flex;gap:8px;margin:14px 0}button{background:#171a30;color:#ddd;border:1px solid #383b5d;border-radius:8px;padding:9px 16px}button.active{background:#39245c;border-color:#bb8cff}.chart{height:340px;background:#080a16;border:1px solid #292b49;border-radius:12px;overflow:hidden}.tile strong{font-size:19px;display:block;overflow-wrap:anywhere}.status{margin-top:14px;font-size:12px;color:#b9bad0;overflow-wrap:anywhere}.note{font-size:11px;line-height:1.5;color:#898ba6}@media(max-width:650px){.grid{grid-template-columns:1fr 1fr}.indicators{grid-template-columns:1fr 1fr}.bottom{grid-template-columns:1fr 1fr}.chart{height:300px}}
</style></head><body><header><h1>₿ BTC RADAR PRO</h1><div id="live">● CONECTANDO</div></header><main>
<section class="grid"><div class="card"><div class="label">Señal técnica alcista</div><div class="pct green" id="bull">--%</div><div class="bar"><span id="bullbar"></span></div></div><div class="card"><div class="label">Dirección técnica</div><div class="direction purple" id="direction">ESPERANDO</div><small id="score">Reuniendo datos</small></div><div class="card"><div class="label">Señal técnica bajista</div><div class="pct red" id="bear">--%</div><div class="bar red"><span id="bearbar"></span></div></div></section>
<div class="tabs"><button class="tf active" data-tf="1">1m</button><button class="tf" data-tf="3">3m</button><button class="tf" data-tf="5">5m</button><button class="tf" data-tf="15">15m</button></div><div class="chart" id="chart"></div>
<section class="indicators"><div class="tile"><small>RSI (14)</small><strong id="rsi">--</strong></div><div class="tile"><small>MACD</small><strong id="macd">--</strong></div><div class="tile"><small>EMA 9 / EMA 21</small><strong id="emas">--</strong></div></section>
<section class="bottom"><div class="tile"><small>Tiempo restante del bloque 15m</small><strong class="purple" id="countdown">15:00</strong><div class="bar purple"><span id="timebar" style="width:100%"></span></div></div><div class="tile"><small>Precio BTC/USD</small><strong class="green" id="price">$--</strong><small id="updated">Esperando precio</small></div><div class="tile"><small>Estimación técnica a 15m</small><strong class="purple" id="target">$--</strong><small id="targetchange">Esperando datos</small></div><div class="tile"><small>Escenario estimado</small><strong id="forecastdir">--</strong><small>No es una garantía</small></div></section>
<div class="status" id="status">Conectando con fuentes...</div><p class="note">Fuentes: Coinbase, Binance.US, Kraken y CoinGecko. Cambio automático de proveedor. Las señales son cálculos técnicos, no probabilidades verificadas ni garantías. El precio spot puede diferir del índice BRTI de Kalshi. Este radar no ejecuta operaciones.</p></main>
<script>
const $=id=>document.getElementById(id);let tf=1,state=null,chart,cs,e9,e21,vw,vs;
const fmt=x=>Number(x).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2});
function init(){if(!window.LightweightCharts){$("status").textContent="No se pudo cargar la librería de gráficas.";return}
chart=LightweightCharts.createChart($("chart"),{width:$("chart").clientWidth,height:$("chart").clientHeight,layout:{background:{color:"#080a16"},textColor:"#c7c4dc"},grid:{vertLines:{color:"#202238"},horzLines:{color:"#202238"}},timeScale:{timeVisible:true},rightPriceScale:{}});
cs=chart.addCandlestickSeries({upColor:"#00d98a",downColor:"#ff4166",borderUpColor:"#00d98a",borderDownColor:"#ff4166",wickUpColor:"#00d98a",wickDownColor:"#ff4166"});e9=chart.addLineSeries({color:"#00aaff",lineWidth:2});e21=chart.addLineSeries({color:"#ff9d2e",lineWidth:2});vw=chart.addLineSeries({color:"#b34cff",lineWidth:2});vs=chart.addHistogramSeries({priceScaleId:"",priceFormat:{type:"volume"},scaleMargins:{top:.82,bottom:0}})}
function ema(a,n){if(!a.length)return [];let k=2/(n+1),o=[a[0]];for(let i=1;i<a.length;i++)o.push(a[i]*k+o[i-1]*(1-k));return o}
function draw(){if(!state||!chart)return;let d=state.timeframes[String(tf)]||[];if(!d.length)return;cs.setData(d.map(x=>({time:x.time,open:x.open,high:x.high,low:x.low,close:x.close})));let c=d.map(x=>x.close),a=ema(c,9),b=ema(c,21),v=0,val=0,w=[];d.forEach(x=>{let t=(x.high+x.low+x.close)/3;v+=x.volume;val+=t*x.volume;w.push(v?val/v:x.close)});e9.setData(a.map((x,i)=>({time:d[i].time,value:x})));e21.setData(b.map((x,i)=>({time:d[i].time,value:x})));vw.setData(w.map((x,i)=>({time:d[i].time,value:x})));vs.setData(d.map(x=>({time:x.time,value:x.volume,color:x.close>=x.open?"#00d98a66":"#ff416666"})));chart.timeScale().scrollToRealTime()}
function render(s){state=s;let p=s.prediction,f=s.forecast;$("bull").textContent=p.bull+"%";$("bear").textContent=p.bear+"%";$("bullbar").style.width=p.bull+"%";$("bearbar").style.width=p.bear+"%";$("direction").textContent=p.direction;$("direction").className="direction "+(p.direction==="ALCISTA"?"green":p.direction==="BAJISTA"?"red":"purple");$("score").textContent="Puntuación técnica: "+p.score;$("rsi").textContent=p.rsi;$("macd").textContent=p.histogram;$("emas").textContent=p.ema9!==null?fmt(p.ema9)+" / "+fmt(p.ema21):"Reuniendo datos";$("price").textContent="$"+fmt(s.price);$("target").textContent=f.target!==null?"$"+fmt(f.target):"Esperando datos";$("targetchange").textContent=f.change_pct!==null?(f.change_pct>0?"+":"")+f.change_pct+"% estimado":"Sin estimación";$("forecastdir").textContent=f.direction;$("forecastdir").className=f.direction==="SUBE"?"green":f.direction==="BAJA"?"red":"purple";$("live").textContent=s.connected?"● EN VIVO":"● SIN CONEXIÓN";$("live").style.color=s.connected?"#00e39a":"#ff657e";$("status").textContent="Fuente: "+s.source+" · Actualizado hace "+s.update_age+" s · "+(s.connected?"conectado":"sin conexión")+(s.last_error?" · "+s.last_error:"");$("updated").textContent="Fuente: "+s.source;draw()}
async function refresh(){try{let r=await fetch("/api/state?t="+Date.now(),{cache:"no-store"});if(!r.ok)throw Error("HTTP "+r.status);render(await r.json())}catch(e){$("live").textContent="● REINTENTANDO";$("live").style.color="#ffb347";$("status").textContent="Sin datos temporalmente. Reconexión automática activa."}}
function clock(){let n=900-Math.floor(Date.now()/1000)%900;$("countdown").textContent=String(Math.floor(n/60)).padStart(2,"0")+":"+String(n%60).padStart(2,"0");$("timebar").style.width=(n/900*100)+"%"}
document.querySelectorAll(".tf").forEach(b=>b.onclick=()=>{document.querySelectorAll(".tf").forEach(x=>x.classList.remove("active"));b.classList.add("active");tf=+b.dataset.tf;draw()});init();window.addEventListener("resize",()=>{if(chart)chart.applyOptions({width:$("chart").clientWidth,height:$("chart").clientHeight})});refresh();clock();setInterval(refresh,3000);setInterval(clock,250);
</script></body></html>'''

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/health")
def health():
    with LOCK:
        age=round(time.time()-UPDATED,1) if UPDATED else None
        return jsonify(ok=True,connected=bool(PRICE and age is not None and age<20),
                       source=SOURCE,candles=len(CANDLES),last_update_age=age,last_error=ERROR)

@app.route("/api/state")
def api_state():
    with LOCK:
        raw=list(CANDLES); price=PRICE; updated=UPDATED; provider=SOURCE; error=ERROR
    age=time.time()-updated if updated else 999999
    online=price>0 and age<20
    if not raw or price<=0:
        return jsonify(error="Esperando datos de las fuentes",connected=False,source=provider,last_error=error),503
    frames={str(n):aggregate(raw,n)[-240:] for n in (1,3,5,15)}
    pred=analyze(raw); fc=forecast(raw,price,pred)
    base=raw[0]["open"] or price
    return jsonify(price=round(price,2),change=round((price-base)/base*100,3) if base else 0,
                   connected=online,source=provider,update_age=round(age,1),last_error=error,
                   prediction=pred,forecast=fc,timeframes=frames)

def start_background():
    global STARTED
    with START_LOCK:
        if STARTED:return
        STARTED=True
    load_history()
    threading.Thread(target=price_worker,daemon=True,name="price-worker").start()

start_background()
if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),threaded=True)
