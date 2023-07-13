import numpy as np
import numpy.typing as npt

from mergify_engine import settings
from mergify_engine.clients import http


OPENAI_API_BASE_URL: str = "https://api.openai.com/v1"

OPENAI_EMBEDDINGS_END_POINT: str = OPENAI_API_BASE_URL + "/embeddings"
OPENAI_EMBEDDINGS_MODEL: str = "text-embedding-ada-002"


async def get_embedding(input_string: str) -> npt.NDArray[np.float32]:
    async with http.AsyncClient(
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_TOKEN}",
            "Accept": "application/json",
        },
    ) as session:
        response = await session.post(
            OPENAI_EMBEDDINGS_END_POINT,
            json={
                "input": input_string,
                "model": OPENAI_EMBEDDINGS_MODEL,
            },
        )
        embedding = response.json()["data"][0]["embedding"]

    return np.array(list(map(np.float32, embedding)))
