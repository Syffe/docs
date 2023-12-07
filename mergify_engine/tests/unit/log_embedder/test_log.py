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
