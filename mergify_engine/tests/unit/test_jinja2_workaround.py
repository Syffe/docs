import jinja2.environment


def test_unique_with_async_gen() -> None:
    env_async = jinja2.environment.Environment(enable_async=True)
    items = ["a", "b", "c", "c", "a", "d", "z"]
    tmpl = env_async.from_string("{{ items|reject('==', 'z')|unique|list }}")
    out = tmpl.render(items=items)
    assert out == "['a', 'b', 'c', 'd']"
