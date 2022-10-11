web: uvicorn --log-level warning mergify_engine.web.asgi_testing:application --port 8802 --host localhost --reload
worker-shared: ddtrace-run mergify-engine-worker --enabled-services=shared-stream
worker-dedicated: ddtrace-run mergify-engine-worker --enabled-services=dedicated-stream
worker-monitoring: ddtrace-run mergify-engine-worker --enabled-services=stream-monitoring,delayed-refresh
bridge: python -u mergify_engine/tests/bridge.py --clean
