BTC Radar PRO - versión corregida

Archivos:
- app.py
- requirements.txt

Render:
- Runtime: Python
- Build Command: pip install -r requirements.txt
- Start Command: gunicorn app:app

No requiere variables de entorno obligatorias.
La aplicación usa Coinbase BTC-USD:
- WebSocket para operaciones en tiempo real.
- REST ticker como respaldo si el WebSocket falla.
- /health para comprobar el estado.
- /api/state para los datos del radar.

La estimación a 15 minutos es un escenario técnico heurístico, no una probabilidad calibrada ni una garantía de precio.
