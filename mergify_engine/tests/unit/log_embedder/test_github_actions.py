import pytest

from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import openai_api


MAX_TOKENS_EMBEDDED_LOG = (
    openai_api.OPENAI_CHAT_COMPLETION_MODELS[-1]["max_tokens"]
    - github_action.ERROR_TITLE_QUERY_TEMPLATE.get_tokens_size()
)


@pytest.mark.parametrize(
    "raw_log,expected_length,expected_cleaned_log,expected_embedded_log",
    [
        (["hello\n"], 1, "hello", "hello\n"),
        (
            ["hello\n"] * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            "hello" * (openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN),
            # NOTE(sileht): 'hello\n' it's two token and we can't go over the bigger chat model
            "hello\n" * int(MAX_TOKENS_EMBEDDED_LOG / 2),
        ),
        (
            (["hello\n"] * openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN)
            + [
                "before the end\n",
                "extra token at the end",
            ],
            openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN,
            # NOTE(Kontrolix): When this part is cleaned, it leaves 4 tokens
            ("hello" * (openai_api.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN - 4))
            + "endextra token end",
            (
                # NOTE(sileht): 'hello\n' it's two token and we can't go over the bigger chat model
                "hello\n" * (int((MAX_TOKENS_EMBEDDED_LOG - 9) / 2))
                # 9 tokens
                + "before the end\nextra token at the end"
            ),
        ),
        (
            # more is removed by LogCleaner, this ensures we remove end of files
            # from emebedded_log and log_embedding
            ["hello\n", "more\n", "more\n", "more\n"],
            1,
            "hello",
            "hello\n",
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
    raw_log: list[str],
    expected_length: int,
    expected_cleaned_log: str,
    expected_embedded_log: str,
) -> None:
    tokens, truncated_log = await github_action.get_tokenized_cleaned_log(raw_log)

    assert len(tokens) == expected_length

    assert openai_api.TIKTOKEN_ENCODING.decode(tokens) == expected_cleaned_log
    assert len(truncated_log) == len(expected_embedded_log)
    assert truncated_log == expected_embedded_log
