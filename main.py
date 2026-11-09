<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>BTC Radar PRO - Live WebSocket</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <!-- Librería oficial TradingView Lightweight Charts para velas profesionales perfectas -->
  <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    body { background-color: #0b0e14; color: #ffffff; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    .card-bg { background-color: #121824; border: 1px solid #1e293b; }
  </style>
</head>
<body class="p-3 max-w-md mx-auto select-none pb-8">

  <!-- Header -->
  <div class="flex items-center justify-between mb-3">
    <div class="flex items-center gap-2">
      <div class="w-8 h-8 rounded-full bg-amber-500/20 text-amber-500 flex items-center justify-center font-bold text-lg">₿</div>
      <div>
        <div class="flex items-center gap-1.5">
          <span class="font-extrabold text-base tracking-wide">BTC Radar</span>
          <span class="bg-fuchsia-600/30 text-fuchsia-400 text-[10px] font-bold px-1.5 py-0.5 rounded border border-fuchsia-500/40">PRO</span>
        </div>
        <p class="text-[10px] text-gray-400">Análisis en Tiempo Real • Coinbase</p>
      </div>
    </div>
    <div class="flex items-center gap-2">
      <div class="flex items-center gap-1.5 bg-emerald-950/60 text-emerald-400 text-xs px-2.5 py-1 rounded-full border border-emerald-500/30">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
        <span class="font-semibold text-[11px]">En Vivo</span>
      </div>
      <button class="w-8 h-8 rounded-full card-bg flex items-center justify-center text-gray-400 hover:text-white">
        <i class="fa-solid fa-gear text-xs"></i>
      </button>
    </div>
  </div>

  <!-- Prediction Probability Bars -->
  <div class="grid grid-cols-3 gap-2 mb-3">
    <div class="card-bg rounded-xl p-2.5 flex flex-col justify-between border-emerald-500/30 shadow-lg shadow-emerald-950/20">
      <div class="flex items-center gap-1 text-[11px] font-bold text-emerald-400">
        <i class="fa-solid fa-arrow-trend-up text-xs"></i> Alcista
      </div>
      <div class="text-xl font-black text-emerald-400 mt-1" id="prob-alcista">67.8%</div>
      <div class="w-full bg-slate-800 h-1 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-emerald-400 h-full rounded-full" style="width: 67.8%"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-2 text-center flex flex-col justify-center border-slate-800">
      <span class="text-[9px] text-gray-400 font-medium">Predicción 15 min</span>
      <div class="text-xs font-bold text-emerald-400 mt-0.5" id="pred-price">↑ 76,842.50</div>
      <span class="text-[10px] text-emerald-400 font-semibold" id="pred-change">(+0.73%)</span>
    </div>
    <div class="card-bg rounded-xl p-2.5 flex flex-col justify-between items-end border-rose-500/30 shadow-lg shadow-rose-950/20">
      <div class="flex items-center gap-1 text-[11px] font-bold text-rose-400">
        Bajista <i class="fa-solid fa-arrow-trend-down text-xs"></i>
      </div>
      <div class="text-xl font-black text-rose-400 mt-1" id="prob-bajista">32.2%</div>
      <div class="w-full bg-slate-800 h-1 rounded-full mt-1.5 overflow-hidden flex justify-end">
        <div class="bg-rose-500 h-full rounded-full" style="width: 32.2%"></div>
      </div>
    </div>
  </div>

  <!-- Timeframe Selector & Indicators Legend -->
  <div class="flex items-center justify-between text-xs mb-2">
    <div class="flex bg-slate-900 rounded-lg p-0.5 border border-slate-800">
      <button class="px-2.5 py-1 text-[11px] rounded bg-purple-600 text-white font-semibold">1m</button>
      <button class="px-2.5 py-1 text-[11px] text-gray-400 hover:text-white">3m</button>
      <button class="px-2.5 py-1 text-[11px] text-gray-400 hover:text-white">5m</button>
      <button class="px-2.5 py-1 text-[11px] text-gray-400 hover:text-white">15m</button>
    </div>
    <div class="flex items-center gap-2 text-[10px] text-gray-400">
      <span class="flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-cyan-400"></span>EMA 9</span>
      <span class="flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-amber-400"></span>EMA 21</span>
      <span class="flex items-center gap-1"><span class="w-1.5 h-1.5 rounded-full bg-fuchsia-400"></span>VWAP</span>
    </div>
  </div>

  <!-- Chart Card -->
  <div class="card-bg rounded-xl p-3 mb-3 relative overflow-hidden">
    <div class="flex justify-between items-center mb-1 text-xs">
      <span class="text-gray-400 font-medium">BTC/USD</span>
      <span class="font-mono text-emerald-400 font-bold bg-emerald-950/40 px-2 py-0.5 rounded border border-emerald-500/30" id="badge-price">76,734.32</span>
    </div>
    <div class="h-64 w-full" id="tradingview-chart"></div>
  </div>
    <!-- Technical Indicators Row -->
  <div class="grid grid-cols-3 gap-2 mb-3">
    <div class="card-bg rounded-xl p-2.5">
      <div class="text-[10px] text-gray-400 font-medium">RSI (14)</div>
      <div class="text-lg font-black text-cyan-400" id="rsi-val">72.4</div>
      <div class="h-1.5 w-full bg-slate-800 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-cyan-400 h-full rounded-full" style="width: 72.4%"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-2.5">
      <div class="text-[10px] text-gray-400 font-medium">MACD</div>
      <div class="text-lg font-black text-emerald-400" id="macd-val">+124.6</div>
      <div class="flex gap-0.5 h-2 items-end mt-1">
        <div class="w-1/4 h-1 bg-emerald-500/40 rounded-sm"></div>
        <div class="w-1/4 h-2 bg-emerald-500 rounded-sm"></div>
        <div class="w-1/4 h-1.5 bg-emerald-500 rounded-sm"></div>
        <div class="w-1/4 h-0.5 bg-emerald-500/30 rounded-sm"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-2.5">
      <div class="text-[10px] text-gray-400 font-medium">Volumen</div>
      <div class="text-lg font-black text-purple-400" id="vol-val">2.8K</div>
      <div class="h-1.5 w-full bg-slate-800 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-purple-400 h-full rounded-full" style="width: 58%"></div>
      </div>
    </div>
  </div>

  <!-- Timer & Current Price -->
  <div class="grid grid-cols-2 gap-2 mb-3">
    <div class="card-bg rounded-xl p-3 flex flex-col justify-center">
      <div class="flex items-center gap-1.5 text-xs text-gray-400 mb-0.5">
        <i class="fa-regular fa-clock"></i>
        <span>Tiempo Restante</span>
      </div>
      <div class="font-mono text-2xl font-black tracking-wider text-slate-100" id="timer">00:07:18</div>
      <div class="w-full bg-slate-800 h-1 rounded-full mt-1.5 overflow-hidden">
        <div class="bg-fuchsia-500 h-full rounded-full" style="width: 45%"></div>
      </div>
    </div>
    <div class="card-bg rounded-xl p-3 flex flex-col justify-center">
      <div class="flex items-center gap-1.5 text-xs text-
