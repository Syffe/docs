# NOTE(sileht): This should mimic the worker and asgi app as much as possible.


def import_check_worker() -> int:
    from mergify_engine import config  # noqa isort:skip
    from mergify_engine import worker  # noqa isort:skip

    worker.service.setup("import-check-worker")
    worker.signals.register()

    return 0


def import_check_web() -> int:
    from mergify_engine import config  # noqa isort:skip
    from mergify_engine.web import asgi

    asgi.service.setup("import-check-web")

    return 0
