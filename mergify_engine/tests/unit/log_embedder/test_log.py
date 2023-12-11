import pytest

from mergify_engine.log_embedder import log as logm


def test_log_from_content() -> None:
    content = "hello\nworld"
    log = logm.Log.from_content(content)
    assert log.content == content
    assert log.lines == ["hello", "world"]
    assert log.b == content.encode()


def test_log_from_bytes() -> None:
    content = b"hello\nworld"
    log = logm.Log.from_bytes(content)
    assert log.b == content
    assert log.lines == ["hello", "world"]
    assert log.content == content.decode()


def test_log_from_lines() -> None:
    lines = ["hello", "world"]
    log = logm.Log.from_lines(lines)
    assert log.b == b"hello\nworld"
    assert log.lines == lines
    assert log.content == "hello\nworld"


def test_encode_decode_log() -> None:
    log = logm.Log.from_bytes(b"hello\nworld")
    assert logm.Log.decode(log.encode()) == log


def test_iter_gpt_cleaned_log_lines_reverse() -> None:
    content = "hello\nworld"
    log = logm.Log.from_content(content)
    for i, line in enumerate(log.iter_gpt_cleaned_log_lines_reverse()):
        if i == 1:
            assert line == "hello"
        if i == 0:
            assert line == "world"
        if i > 1:
            raise ValueError("Should only have 2 lines")


def test_extract() -> None:
    content = """ hello
world
   happy


 to😇
be
here """
    log = logm.Log.from_content(content)

    with pytest.raises(ValueError, match="positive"):
        assert log.extract(-1) == ""

    assert log.extract(0) == ""
    assert log.extract(1) == "e"
    assert log.extract(2) == "re"
    assert log.extract(3) == "ere"
    assert log.extract(4) == "here"
    assert log.extract(5) == "here"
    assert log.extract(6) == "here"
    assert log.extract(7) == "be\nhere"
    assert log.extract(8) == "be\nhere"
    assert log.extract(9) == "be\nhere"
    assert log.extract(10) == "be\nhere"
    assert log.extract(11) == "be\nhere"
    assert log.extract(12) == "to😇\nbe\nhere"
    assert log.extract(13) == "to😇\nbe\nhere"
    assert log.extract(14) == "to😇\nbe\nhere"
    assert log.extract(15) == "to😇\nbe\nhere"
    assert log.extract(16) == "to😇\nbe\nhere"
    assert log.extract(17) == "to😇\nbe\nhere"
    assert log.extract(18) == "to😇\nbe\nhere"
    assert log.extract(19) == "to😇\nbe\nhere"
    assert log.extract(20) == "to😇\nbe\nhere"
    assert log.extract(21) == "to😇\nbe\nhere"
    assert log.extract(22) == "to😇\nbe\nhere"
    assert log.extract(23) == "happy\n\n\n to😇\nbe\nhere"
    assert log.extract(24) == "happy\n\n\n to😇\nbe\nhere"
    assert log.extract(25) == "happy\n\n\n to😇\nbe\nhere"
    assert log.extract(26) == "happy\n\n\n to😇\nbe\nhere"
    assert log.extract(27) == "happy\n\n\n to😇\nbe\nhere"
    assert log.extract(28) == "happy\n\n\n to😇\nbe\nhere"
    assert log.extract(29) == "world\n   happy\n\n\n to😇\nbe\nhere"
    assert log.extract(30) == "world\n   happy\n\n\n to😇\nbe\nhere"
    assert log.extract(31) == "world\n   happy\n\n\n to😇\nbe\nhere"
    assert log.extract(32) == "world\n   happy\n\n\n to😇\nbe\nhere"
    assert log.extract(33) == "world\n   happy\n\n\n to😇\nbe\nhere"
    assert log.extract(34) == "world\n   happy\n\n\n to😇\nbe\nhere"
    assert log.extract(35) == content.strip()
    assert log.extract(36) == content.strip()

    assert log.extract(1000) == content.strip()
