from mergify_engine.models.github import account
from mergify_engine.models.github import repository
from mergify_engine.models.github import user


def test_repr() -> None:
    u = user.GitHubUser(id=1, login="foobar")
    acc = account.GitHubAccount(id=1, login="foobar", type="User")
    repo = repository.GitHubRepository(id=1, owner=acc, name="repo-name")

    u_ref = hex(id(u))
    acc_ref = hex(id(acc))
    repo_ref = hex(id(repo))

    assert (
        repr(u)
        == f"<mergify_engine.models.github.user.GitHubUser object at {u_ref} id=1 login='foobar'>"
    )
    assert (
        repr(acc)
        == f"<mergify_engine.models.github.account.GitHubAccount object at {acc_ref} id=1 login='foobar' type='User'>"
    )
    assert (
        repr(repo)
        == f"<mergify_engine.models.github.repository.GitHubRepository object at {repo_ref} id=1 full_name='foobar/repo-name'>"
    )
