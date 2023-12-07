from collections import abc
import importlib
from unittest import mock

import pydantic
import pytest

from mergify_engine import crypto
from mergify_engine import settings


def test_encrypt() -> None:
    x = "this is an amazing string, right? 🙄".encode()
    assert x == crypto.decrypt(crypto.encrypt(x))
    assert x == crypto.decrypt(crypto.encrypt(x))
    assert x == crypto.decrypt(crypto.encrypt(crypto.decrypt(crypto.encrypt(x))))


@pytest.fixture()
def _cleanup_secrets() -> abc.Generator[None, None, None]:
    current_secret = settings.REDIS_CRYPTO_SECRET_CURRENT.get_secret_value()
    try:
        yield
    finally:
        # ensure pytest monkeypatch has revert values and we reload the module
        assert settings.REDIS_CRYPTO_SECRET_CURRENT.get_secret_value() == current_secret
        assert settings.REDIS_CRYPTO_SECRET_OLD is None
        # regen digest with default secrets
        importlib.reload(crypto)  # type: ignore[unreachable]


def test_key_rotation(_cleanup_secrets: None) -> None:
    x = "this is an amazing string, right? 🙄".encode()

    with mock.patch.object(
        settings,
        "REDIS_CRYPTO_SECRET_CURRENT",
        pydantic.SecretStr("old password"),
    ):
        importlib.reload(crypto)  # regen digest with new secret
        encryped_old = crypto.encrypt(x)

    with mock.patch.object(
        settings,
        "REDIS_CRYPTO_SECRET_CURRENT",
        pydantic.SecretStr("new password"),
    ), mock.patch.object(
        settings,
        "REDIS_CRYPTO_SECRET_OLD",
        pydantic.SecretStr("old password"),
    ):
        importlib.reload(crypto)  # regen digest with new secrets
        encryped_new = crypto.encrypt(x)
        assert encryped_new != encryped_old

        assert x == crypto.decrypt(encryped_new)
        assert x == crypto.decrypt(encryped_old)
