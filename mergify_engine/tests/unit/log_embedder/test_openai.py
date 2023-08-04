import numpy as np
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
