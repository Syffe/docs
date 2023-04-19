import pytest
import voluptuous

from mergify_engine.rules import types


@pytest.mark.parametrize(
    "s",
    (
        "hello",
        "{{author}}",
        "{{ assignee[0] }}",
        "{{ assignee[99] }}",
    ),
)
def test_jinja2_valid(s: str) -> None:
    assert types.Jinja2(s) == s

    assert types.Jinja2WithNone(s) == s


def test_jinja2_invalid() -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2("{{foo")
    assert str(x.value) == "Template syntax error @ data[line 1]"
    assert (
        str(x.value.error_message)
        == "unexpected end of template, expected 'end of print statement'."
    )

    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2WithNone("{{foo")
    assert str(x.value) == "Template syntax error @ data[line 1]"
    assert (
        str(x.value.error_message)
        == "unexpected end of template, expected 'end of print statement'."
    )


def test_jinja2_None() -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2(None)
    assert str(x.value) == "Template cannot be null"

    assert types.Jinja2WithNone(None) is None


def test_jinja2_not_str() -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2({"title": None})
    assert str(x.value) == "Template must be a string"

    assert types.Jinja2WithNone(None) is None


def test_jinja2_unknown_attr() -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2("{{foo}}")
    assert str(x.value) == "Template syntax error"
    assert str(x.value.error_message) == "Unknown pull request attribute: foo"

    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2WithNone("{{foo}}")
    assert str(x.value) == "Template syntax error"
    assert str(x.value.error_message) == "Unknown pull request attribute: foo"


def test_jinja2_custom_attr() -> None:
    s = "{{ role_status }}"

    assert types.Jinja2(s, {"role_status": "passed"}) == s

    assert types.Jinja2WithNone(s, {"role_status": "passed"}) == s


@pytest.mark.parametrize(
    "login",
    ("foobar", "foobaz", "foo-baz", "f123", "123foo", "foouser_barorgname"),
)
def test_github_login_ok(login: str) -> None:
    assert voluptuous.Schema(types.GitHubLogin)(login) == login


@pytest.mark.parametrize(
    "login,error",
    (
        ("-foobar", "GitHub login contains invalid characters: -foobar"),
        ("foobaz-", "GitHub login contains invalid characters: foobaz-"),
        ("foo-béaz", "GitHub login contains invalid characters: foo-béaz"),
        ("🤣", "GitHub login contains invalid characters: 🤣"),
        ("", "A GitHub login cannot be an empty string"),
    ),
)
def test_github_login_nok(login: str, error: str) -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        voluptuous.Schema(types.GitHubLogin)(login)
    assert str(x.value) == error


@pytest.mark.parametrize(
    "login,org,slug",
    (
        ("foobar", None, "foobar"),
        ("foobaz", None, "foobaz"),
        ("foo-baz", None, "foo-baz"),
        ("f123", None, "f123"),
        ("foo/bar", "foo", "bar"),
        ("@foo/bar", "foo", "bar"),
        ("@fo-o/bar", "fo-o", "bar"),
        ("@fo-o/ba-r", "fo-o", "ba-r"),
        ("@foo/ba-r", "foo", "ba-r"),
        ("under_score", None, "under_score"),
    ),
)
def test_github_team_ok(login: str, org: str, slug: str) -> None:
    team = voluptuous.Schema(types.GitHubTeam)(login)
    assert team.team == slug
    assert team.organization == org


@pytest.mark.parametrize(
    "login,error",
    (
        ("-foobar", "GitHub team contains invalid characters"),
        ("/-foobar", "A GitHub organization cannot be an empty string"),
        ("foo/-foobar", "GitHub team contains invalid characters"),
        ("foo/-", "GitHub team contains invalid characters"),
        ("foo/foo/bar", "GitHub team contains invalid characters"),
        ("foo//-", "GitHub team contains invalid characters"),
        ("/foo//-", "A GitHub organization cannot be an empty string"),
        ("@/foo//-", "A GitHub organization cannot be an empty string"),
        ("@arf/", "A GitHub team cannot be an empty string"),
        ("", "A GitHub team cannot be an empty string"),
    ),
)
def test_github_team_nok(login: str, error: str) -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        voluptuous.Schema(types.GitHubTeam)(login)
    assert str(x.value) == error
