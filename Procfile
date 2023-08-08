web: uvicorn --log-level warning mergify_engine.web.asgi_testing:application --port 8802 --host localhost --reload
worker-shared: ddtrace-run mergify-engine-worker --enabled-services=shared-workers-spawner,gitter
worker-dedicated: ddtrace-run mergify-engine-worker --enabled-services=dedicated-workers-spawner,gitter
worker-monitoring: ddtrace-run mergify-engine-worker --enabled-services=stream-monitoring,delayed-refresh,event-forwarder
worker-ci: ddtrace-run mergify-engine-worker --enabled-services=ci-event-processing,log-embedder
bridge: python -u mergify_engine/tests/bridge.py --clean
