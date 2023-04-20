# NOTE(sileht): This should mimic the worker and asgi app as much as possible.


def import_check_worker() -> int:
    from mergify_engine import config  # noqa isort:skip
    from mergify_engine.worker import manager  # isort:skip

    manager.service.setup("import-check-worker")  # type: ignore[attr-defined]
    manager.signals.register()  # type: ignore[attr-defined]

    return 0


def import_check_web() -> int:
    from mergify_engine import config  # noqa isort:skip
    from mergify_engine.web import asgi  # noqa isort:skip

    return 0


def import_check_db_update() -> int:
    from mergify_engine.models import manage  # noqa isort:skip

    return 0
