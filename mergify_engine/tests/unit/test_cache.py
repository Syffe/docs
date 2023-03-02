from mergify_engine import cache


def test_cache_method() -> None:
    c: cache.Cache[str, bool] = cache.Cache()
    assert c.get("foo") is cache.Unset
    c.set("foo", False)
    assert not c.get("foo")
    c.set("bar", True)
    assert c.get("bar")
    c.delete("foo")
    assert c.get("foo") is cache.Unset
    assert c.get("bar")

    c.clear()
    assert c.get("foo") is cache.Unset
    assert c.get("bar") is cache.Unset


def test_cache_dict_interface() -> None:
    c: cache.Cache[str, bool] = cache.Cache()
    assert c.get("foo") is cache.Unset
    assert c["foo"] is cache.Unset
    c["foo"] = False
    assert not c["foo"]
    # https://github.com/python/mypy/issues/11969
    c["foo"] = True  # type: ignore[unreachable]
    assert c["foo"]
    del c["foo"]
    assert c.get("foo") is cache.Unset
    assert c["foo"] is cache.Unset
    del c["notexist"]


def test_single_cache() -> None:
    c: cache.SingleCache[str] = cache.SingleCache()
    assert c.get() is cache.Unset
    c.set("foo")
    assert c.get() == "foo"
    c.delete()
    assert c.get() is cache.Unset
