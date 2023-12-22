import numpy as np
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
                },
            ],
            "model": openai_api.OPENAI_EMBEDDINGS_MODEL,
            "usage": {"prompt_tokens": 2, "total_tokens": 2},
        },
    )

    async with openai_api.OpenAIClient() as client:
        embedding = await client.get_embedding("toto")

    assert np.array_equal(embedding, OPENAI_EMBEDDING_DATASET_NUMPY_FORMAT["toto"])


async def test_get_chat_completion(
    respx_mock: respx.MockRouter,
) -> None:
    json_response = {
        "id": "chatcmpl-123",
        "object": "chat.completion",
        "created": 1677652288,
        "model": "gpt-3.5-turbo-0613",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello! How can I assist you today?",
                },
                "finish_reason": "stop",
            },
        ],
        "usage": {"prompt_tokens": 8, "completion_tokens": 9, "total_tokens": 17},
    }
    respx_mock.post(
        f"{openai_api.OPENAI_API_BASE_URL}/chat/completions",
    ).respond(
        200,
        json=json_response,
    )

    async with openai_api.OpenAIClient() as client:
        chat_completion = await client.get_chat_completion(
            openai_api.ChatCompletion(
                model="gpt-4-1106-preview",
                messages=[
                    {
                        "role": "user",
                        "content": "hello",
                    },
                ],
                response_format={"type": "text"},
                seed=0,
                temperature=0,
            ),
        )

    assert (
        chat_completion["choices"][0]["message"]["content"]
        == "Hello! How can I assist you today?"
    )


def test_get_chat_completion_token_size() -> None:
    assert (
        openai_api.get_chat_completion_token_size(
            openai_api.ChatCompletion(
                model="gpt-4-1106-preview",
                messages=[
                    {
                        "role": "user",
                        "content": "hello",
                    },
                ],
                response_format={"type": "text"},
                seed=0,
                temperature=0,
            ),
        )
        == 8
    )
    assert (
        openai_api.get_chat_completion_token_size(
            openai_api.ChatCompletion(
                model="gpt-4-1106-preview",
                messages=[
                    {
                        "role": "system",
                        "content": "you are a bot",
                    },
                    {
                        "role": "user",
                        "content": "hello",
                    },
                ],
                response_format={"type": "text"},
                seed=0,
                temperature=0,
            ),
        )
        == 13
    )
