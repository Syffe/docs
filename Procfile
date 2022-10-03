web: gunicorn -k uvicorn.workers.UvicornH11Worker --log-level warning mergify_engine.web.asgi
worker-shared: ddtrace-run mergify-engine-worker --enabled-services=shared-stream
worker-dedicated: ddtrace-run mergify-engine-worker --enabled-services=dedicated-stream
worker-monitoring: ddtrace-run mergify-engine-worker --enabled-services=stream-monitoring,delayed-refresh
bridge: python -u mergify_engine/tests/bridge.py --clean
