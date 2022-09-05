from mergify_engine import check_api


def test_conclusion_str() -> None:
    assert str(check_api.Conclusion(None)) == "🟠 pending"
    assert str(check_api.Conclusion("success")) == "✅ success"
