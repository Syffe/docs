import base64
import importlib.resources  # nosemgrep: python.lang.compatibility.python37.python37-compatibility-importlib2

import numpy as np
import numpy.typing as npt
import tiktoken

from mergify_engine import settings
from mergify_engine.clients import http


OPENAI_API_BASE_URL: str = "https://api.openai.com/v1"

OPENAI_EMBEDDINGS_END_POINT: str = OPENAI_API_BASE_URL + "/embeddings"
OPENAI_EMBEDDINGS_MODEL: str = "text-embedding-ada-002"
OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN: int = 8191

# NOTE: https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken
mergeable_ranks_file = str(
    importlib.resources.files(__package__).joinpath("cl100k_base.tiktoken")
)

with open(mergeable_ranks_file) as f:
    mergeable_ranks = {
        base64.b64decode(token): int(rank)
        for token, rank in (line.split() for line in f.readlines() if line)
    }

# NOTE: Values from https://github.com/openai/tiktoken/blob/main/tiktoken_ext/openai_public.py#L63
TIKTOKEN_ENCODING = tiktoken.Encoding(
    name="cl100k_base_static",
    pat_str=r"""(?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+""",
    mergeable_ranks=mergeable_ranks,
    special_tokens={
        "<|endoftext|>": 100257,
        "<|fim_prefix|>": 100258,
        "<|fim_middle|>": 100259,
        "<|fim_suffix|>": 100260,
        "<|endofprompt|>": 100276,
    },
)


async def get_embedding(input_data: str | list[int]) -> npt.NDArray[np.float32]:
    async with http.AsyncClient(
        headers={
            "Authorization": f"Bearer {settings.OPENAI_API_TOKEN}",
            "Accept": "application/json",
        },
    ) as session:
        response = await session.post(
            OPENAI_EMBEDDINGS_END_POINT,
            json={
                "input": input_data,
                "model": OPENAI_EMBEDDINGS_MODEL,
            },
        )
        embedding = response.json()["data"][0]["embedding"]

    return np.array(list(map(np.float32, embedding)))
