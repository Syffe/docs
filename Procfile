web: gunicorn -k uvicorn.workers.UvicornH11Worker --statsd-host localhost:8125 --log-level warning --pythonpath=mergify-engine mergify_engine.web.asgi
worker: mergify-engine-worker
