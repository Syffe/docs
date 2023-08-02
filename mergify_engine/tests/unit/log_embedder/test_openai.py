import numpy as np
import pytest
import respx

from mergify_engine.log_embedder import openai_embedding
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)


async def test_openai_get_embedding(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(
        openai_embedding.OPENAI_EMBEDDINGS_END_POINT,
        json={
            "input": "toto",
            "model": openai_embedding.OPENAI_EMBEDDINGS_MODEL,
        },
    ).respond(
        200,
        json={
            "object": "list",
            "data": [
                {
                    "object": "embedding",
                    "index": 0,
                    "embedding": OPENAI_EMBEDDING_DATASET["toto"],
                }
            ],
            "model": openai_embedding.OPENAI_EMBEDDINGS_MODEL,
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        },
    )

    embedding = await openai_embedding.get_embedding("toto")

    assert np.array_equal(embedding, OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"])


@pytest.mark.timeout(60)
async def test_truncate_to_max_openai_size() -> None:
    one_token_string = "hello"
    max_input_token_string = (
        one_token_string * openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    )
    too_long_input_token_string = max_input_token_string + "the end"

    assert len(openai_embedding.TIKTOKEN_ENCODING.encode(one_token_string)) == 1
    assert (
        len(openai_embedding.TIKTOKEN_ENCODING.encode(max_input_token_string))
        == openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    )
    assert (
        len(openai_embedding.TIKTOKEN_ENCODING.encode(too_long_input_token_string))
        > openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    )

    truncated_one_token_string = openai_embedding.truncate_to_max_openai_size(
        one_token_string
    )
    truncated_max_input_token_string = openai_embedding.truncate_to_max_openai_size(
        max_input_token_string
    )
    truncated_too_long_input_token_string = (
        openai_embedding.truncate_to_max_openai_size(too_long_input_token_string)
    )

    assert truncated_one_token_string == openai_embedding.TIKTOKEN_ENCODING.encode(
        one_token_string
    )
    assert (
        truncated_max_input_token_string
        == openai_embedding.TIKTOKEN_ENCODING.encode(max_input_token_string)
    )
    assert (
        truncated_too_long_input_token_string
        != openai_embedding.TIKTOKEN_ENCODING.encode(too_long_input_token_string)
    )

    assert (
        len(truncated_too_long_input_token_string)
        == openai_embedding.OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN
    )
    assert (
        openai_embedding.TIKTOKEN_ENCODING.decode(
            truncated_too_long_input_token_string
        )[-7:]
        == "the end"
    )
