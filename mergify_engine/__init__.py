import sys

from mergify_engine import config
import mergify_engine.asyncio_workaround  # noqa
import mergify_engine.jinja2_workaround  # noqa


settings = config.EngineSettings()
if not settings.SAAS_MODE and settings.SUBSCRIPTION_TOKEN is None:
    print("SUBSCRIPTION_TOKEN is missing. Mergify can't start.")
    sys.exit(1)
