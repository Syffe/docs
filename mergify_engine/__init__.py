import sys

from mergify_engine import config
import mergify_engine.alembic_utils_workaround  # noqa: RUF100
import mergify_engine.jinja2_workaround  # noqa: RUF100 F401


settings = config.EngineSettings()
if not settings.SAAS_MODE and settings.SUBSCRIPTION_TOKEN is None:
    print("SUBSCRIPTION_TOKEN is missing. Mergify can't start.")  # type: ignore[unreachable] # noqa: T201
    sys.exit(1)
