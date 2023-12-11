import pytest

from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import log as logm
from mergify_engine.log_embedder import openai_api


MAX_TOKENS_EMBEDDED_LOG = github_action.MAX_CHAT_COMPLETION_TOKENS


@pytest.mark.parametrize(
    ("raw_log", "expected_length", "expected_cleaned_log"),
    [
        ("hello\n", 1, "hello"),
        (
            "hello\n" * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            "hello" * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
        ),
        (
            ("hello\n" * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN)
            # NOTE(Kontrolix): When this part is cleaned, it uses 4 tokens
            + "before the end\nextra token at the end",
            openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            ("hello" * (openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN - 4))
            + "endextra token end",
        ),
        (
            # more is removed by LogCleaner, this ensures we remove end of files
            # from emebedded_log and log_embedding
            "hello\nmore\nmore\nmore\n",
            1,
            "hello",
        ),
    ],
    ids=[
        "one_token_string",
        "max_input_token_string",
        "too_long_input_token_string",
        "ending_lines_removed",
    ],
)
async def test_get_tokenized_cleaned_log(
    raw_log: str,
    expected_length: int,
    expected_cleaned_log: str,
) -> None:
    tokens = await github_action.get_tokenized_cleaned_log(
        logm.Log.from_content(raw_log),
    )

    assert len(tokens) == expected_length

    assert openai_api.TIKTOKEN_ENCODING.decode(tokens) == expected_cleaned_log


@pytest.mark.parametrize(
    ("raw_log_lines", "expected_cleaned_log", "cleaned_log_token_size"),
    [
        (["hello\n"], "hello", 1),
        (
            ["hello\n"] * github_action.MAX_CHAT_COMPLETION_TOKENS,
            # NOTE(Kontrolix): Divide by 2 because 'hello\n' is 2 token
            "\n".join(["hello"] * int(MAX_TOKENS_EMBEDDED_LOG / 2)),
            # NOTE(Kontrolix): minus 1 because we haven't an '\n' at the end of the log
            github_action.MAX_CHAT_COMPLETION_TOKENS - 1,
        ),
        (
            ["Tokens that will be removed at the beginning"]
            + (["hello\n"] * github_action.MAX_CHAT_COMPLETION_TOKENS),
            # NOTE(Kontrolix): Divide by 2 because 'hello\n' is 2 token
            "\n".join(["hello"] * int(MAX_TOKENS_EMBEDDED_LOG / 2)),
            # NOTE(Kontrolix): minus 1 because we haven't an '\n' at the end of the log
            github_action.MAX_CHAT_COMPLETION_TOKENS - 1,
        ),
    ],
    ids=["one_token_string", "max_input_token_string", "too_long_input_token_string"],
)
async def test_get_cleaned_log(
    raw_log_lines: list[str],
    expected_cleaned_log: str,
    cleaned_log_token_size: int,
) -> None:
    cleaned_log = github_action.get_cleaned_log(logm.Log.from_lines(raw_log_lines))

    assert expected_cleaned_log == cleaned_log
    assert (
        len(openai_api.TIKTOKEN_ENCODING.encode(cleaned_log)) == cleaned_log_token_size
    )
