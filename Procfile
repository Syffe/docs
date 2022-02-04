web: ddtrace-run gunicorn -k uvicorn.workers.UvicornH11Worker --statsd-host localhost:8125 --log-level warning mergify_engine.web.asgi
worker-shared: ddtrace-run mergify-engine-worker --enabled-services=shared-stream
worker-dedicated: ddtrace-run mergify-engine-worker --enabled-services=dedicated-stream,stream-monitoring,delayed-refresh
