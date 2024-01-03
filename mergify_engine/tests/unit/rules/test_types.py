import pytest
import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine.rules import types
from mergify_engine.rules import types_dummy_context


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


def test_jinja2_invalid_format_string() -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        types.Jinja2('foo  {{ "{foo}".format(0) }}')
    assert str(x.value) == "Template syntax error"


@pytest.mark.parametrize(
    "login",
    ("foobar", "foobaz", "foo-baz", "f123", "123foo", "foouser_barorgname"),
)
def test_github_login_ok(login: str) -> None:
    assert voluptuous.Schema(types.GitHubLogin)(login) == login


@pytest.mark.parametrize(
    ("login", "error"),
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
    ("login", "org", "slug"),
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
    ("login", "error"),
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


@pytest.mark.parametrize(
    ("tmpl", "expectation", "context_attributes_var"),
    [
        ("{{{{ {attr} }}}}", "False", "BOOLEAN_ATTRIBUTES"),
        ("{{{{ {attr}|replace('foo', 'bar') }}}}", "", "STRING_ATTRIBUTES"),
        ("{{{{ {attr} * 123 }}}}", "0", "NUMBER_ATTRIBUTES"),
        ("{{{{ {attr}|join(', ') }}}}", "", "LIST_ATTRIBUTES"),
    ],
)
def test_jinja2_template_with_all_attributes(
    tmpl: str,
    expectation: str,
    context_attributes_var: str,
) -> None:
    for attr in getattr(condition_value_querier.PullRequest, context_attributes_var):
        tmpl_attr = tmpl.format(attr=attr.replace("-", "_"))

        value = types_dummy_context.DUMMY_PR.render_template(tmpl_attr, None)
        assert value == expectation


@pytest.mark.parametrize(
    "name",
    ("foo", "foo-bar", "fop.bar"),
)
def test_repository_name_format_valid(name: str) -> None:
    assert name == voluptuous.Schema(types.GitHubRepositoryName)(name)


@pytest.mark.parametrize(
    "name",
    ("../../foobar", "foo bar", "fop%20bar"),
)
def test_repository_name_format_invalid(name: str) -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        voluptuous.Schema(types.GitHubRepositoryName)(name)

    assert x.value.error_message == r"does not match regular expression ^[\w\-.]+$"


@pytest.mark.parametrize(
    "name",
    ("..", ".", ".git"),
)
def test_repository_name_format_forbidden_name(name: str) -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        voluptuous.Schema(types.GitHubRepositoryName)(name)

    assert x.value.error_message == f"Repository name '{name}' is forbidden"


@pytest.mark.parametrize(
    "name",
    (
        "foobar",
        "foo/bar",
        "foo/bar/bar",
        "foo.bar",
    ),
)
def test_branch_name_format_valid_name(name: str) -> None:
    assert name == voluptuous.Schema(types.BranchName)(name)


@pytest.mark.parametrize(
    "name",
    (
        "foo..bar",
        "foo bar",
        ".yo",
        ".lock",
        "\x00sowrong\x00",
        "hidden\x1fhidden",
        "foo~bar",
        "foo^bar",
        "@",
        "foo@{bar",
        "foo?bar",
        "/foobar",
        "foo//bar",
        "/foo/bar",
        "foobar/",
        "foo/bar/",
        "\\escapedslack",
    ),
)
def test_branch_name_format_invalid_name(name: str) -> None:
    with pytest.raises(voluptuous.Invalid) as x:
        voluptuous.Schema(types.BranchName)(name)

    assert x.value.error_message == "Branch name is invalid"
