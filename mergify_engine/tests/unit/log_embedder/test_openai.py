import numpy as np
import pytest
import respx

from mergify_engine.log_embedder import openai_api
from mergify_engine.tests.openai_embedding_dataset import OPENAI_EMBEDDING_DATASET
from mergify_engine.tests.openai_embedding_dataset import (
    OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT,
)


async def test_openai_get_embedding(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/embeddings",
        json={
            "input": "toto",
            "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
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
            "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        },
    )

    async with openai_api.OpenAIClient() as client:
        embedding = await client.get_embedding("toto")

    assert np.array_equal(embedding, OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"])


async def test_openai_chat_completion_models_order() -> None:
    previous_max_tokens = 0
    for model in openai_api.OPENAI_CHAT_COMPLETION_MODELS:
        if model["max_tokens"] <= previous_max_tokens:
            pytest.fail(
                "OPENAI_CHAT_COMPLETION_MODELS list must be ascending sorted according to max_tokens values"
            )
        previous_max_tokens = model["max_tokens"]


async def test_get_chat_completion(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
        json={
            "model": "gpt-3.5-turbo",
            "messages": [{"role": "user", "content": "hello"}],
        },
    ).respond(
        200,
        json={
            "id": "chatcmpl-7tAj9cUlsxgQOtD6ojRq24bPrljzs",
            "object": "chat.completion",
            "created": 1693383631,
            "model": "gpt-3.5-turbo-0613",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello! How can I assist you today?",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 8, "completion_tokens": 9, "total_tokens": 17},
        },
    )

    async with openai_api.OpenAIClient() as client:
        chat_completion = await client.get_chat_completion(
            openai_api.ChatCompletionQuery("user", "hello", 0)
        )

    assert (
        chat_completion["choices"][0]["message"]["content"]
        == "Hello! How can I assist you today?"
    )


async def test_get_chat_completion_model() -> None:
    query = openai_api.ChatCompletionQuery(
        "user",
        "hello",
        0,
    )
    model = query.get_chat_completion_model()
    assert model["name"] == "gpt-3.5-turbo"

    query = openai_api.ChatCompletionQuery(
        "user",
        "hello"
        * (model["max_tokens"] - 1 - openai_api.OPENAI_CHAT_COMPLETION_FEW_EXTRA_TOKEN),
        0,
    )
    # NOTE(Kontrolix): - 1 is for the token used by the role
    model = query.get_chat_completion_model()
    assert model["name"] == "gpt-3.5-turbo"

    query = openai_api.ChatCompletionQuery(
        "user",
        "hello"
        * (
            model["max_tokens"]
            - 1
            - openai_api.OPENAI_CHAT_COMPLETION_FEW_EXTRA_TOKEN
            - 100
        ),
        100,
    )

    model = query.get_chat_completion_model()
    assert model["name"] == "gpt-3.5-turbo"

    query = openai_api.ChatCompletionQuery(
        "user",
        "hello"
        * (
            model["max_tokens"]
            - 1
            - openai_api.OPENAI_CHAT_COMPLETION_FEW_EXTRA_TOKEN
            - 100
        ),
        101,
    )

    model = query.get_chat_completion_model()
    assert model["name"] == "gpt-3.5-turbo-16k"

    query = openai_api.ChatCompletionQuery(
        "user",
        "hello" * (model["max_tokens"] * 2),
        101,
    )
    with pytest.raises(
        openai_api.OpenAiException, match="No model found to handle 32876 tokens"
    ):
        model = query.get_chat_completion_model()
