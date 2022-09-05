import importlib
import typing

import pytest

from mergify_engine import config
from mergify_engine import crypto


def test_encrypt() -> None:
    x = "this is an amazing string, right? 🙄".encode()
    assert x == crypto.decrypt(crypto.encrypt(x))
    assert x == crypto.decrypt(crypto.encrypt(x))
    assert x == crypto.decrypt(crypto.encrypt(crypto.decrypt(crypto.encrypt(x))))


@pytest.fixture
def cleanup_secrets() -> typing.Generator[None, None, None]:
    current_secret = config.CACHE_TOKEN_SECRET
    try:
        yield
    finally:
        # ensure pytest monkeypatch has revert values and we reload the module
        assert config.CACHE_TOKEN_SECRET == current_secret
        assert config.CACHE_TOKEN_SECRET_OLD is None
        importlib.reload(crypto)  # regen digest with default secrets


def test_key_rotation(
    cleanup_secrets: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    x = "this is an amazing string, right? 🙄".encode()

    monkeypatch.setattr(config, "CACHE_TOKEN_SECRET", "old password")
    importlib.reload(crypto)  # regen digest with new secret
    encryped_old = crypto.encrypt(x)

    monkeypatch.setattr(config, "CACHE_TOKEN_SECRET", "new password")
    monkeypatch.setattr(config, "CACHE_TOKEN_SECRET_OLD", "old password")
    importlib.reload(crypto)  # regen digest with new secrets
    encryped_new = crypto.encrypt(x)
    assert encryped_new != encryped_old

    assert x == crypto.decrypt(encryped_new)
    assert x == crypto.decrypt(encryped_old)
