import base64
import importlib.resources  # nosemgrep: python.lang.compatibility.python37.python37-compatibility-importlib2
import typing

import httpx
import numpy as np
import numpy.typing as npt
import tiktoken

from mergify_engine import settings
from mergify_engine.clients import http
from mergify_engine.config import types


OPENAI_API_BASE_URL: str = "https://api.openai.com/v1"

OPENAI_EMBEDDINGS_MODEL: str = "text-embedding-ada-002"
OPENAI_EMBEDDINGS_MAX_INPUT_TOKEN: int = 8191

# This is a rule-of-thumb number of how many bytes are stored in a token.
# This is not accurate but gives a good estimation when needed.
BYTES_PER_TOKEN_APPROX = 4

# NOTE: https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken
mergeable_ranks_file = str(
    importlib.resources.files(__package__).joinpath("cl100k_base.tiktoken"),
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


class OpenAiException(Exception):
    pass


class OpenAIModelNotFound(Exception):
    pass


OPENAI_CHAT_COMPLETION_MODELS: list[types.OpenAIModel] = types.OpenAIModel.__args__  # type: ignore[attr-defined]
# All models above must at least support this
OPENAI_CHAT_COMPLETION_MODEL_MAX_TOKENS = 16384

# NOTE(Kontolix): "Why 6 ? O_o" according to openai doc "Each message passed to the API
# consumes the number of tokens in the content, role, and other fields, plus a few
# extra for behind-the-scenes formatting. This may change slightly in the future."
# and according to my tests (29/08/2023) 6 is the few extra
OPENAI_CHAT_COMPLETION_FEW_EXTRA_TOKEN = 6


ChatCompletionRole = typing.Literal["system", "user", "assistant"]


class ChatCompletionMessage(typing.TypedDict):
    role: ChatCompletionRole
    content: str | None


class ChatCompletionResponseFormat(typing.TypedDict):
    type: typing.Literal["text", "json_object"]


class ChatCompletion(typing.TypedDict):
    model: types.OpenAIModel
    messages: list[ChatCompletionMessage]
    response_format: ChatCompletionResponseFormat
    seed: int
    temperature: float


# https://platform.openai.com/docs/api-reference/chat/object
class ChatCompletionChoice(typing.TypedDict):
    index: int
    message: ChatCompletionMessage
    finish_reason: typing.Literal[
        "stop",
        "length",
        "content_filter",
        "tool_calls",
        "function_call",
    ]


class ChatCompletionResponse(typing.TypedDict):
    model: str
    choices: list[ChatCompletionChoice]


def get_chat_completion_token_size(completion: ChatCompletion) -> int:
    return (
        sum(
            (
                (
                    len(TIKTOKEN_ENCODING.encode(msg["content"]))
                    if msg["content"] is not None
                    else 0
                )
                + (
                    len(TIKTOKEN_ENCODING.encode(msg["role"]))
                    if msg["role"] is not None
                    else 0
                )
            )
            for msg in completion["messages"]
        )
        + OPENAI_CHAT_COMPLETION_FEW_EXTRA_TOKEN
    )


class OpenAIClient(http.AsyncClient):
    TIMEOUT = httpx.Timeout(5.0, read=120.0)

    def __init__(self) -> None:
        super().__init__(
            base_url=OPENAI_API_BASE_URL,
            headers={
                "Authorization": f"Bearer {settings.OPENAI_API_TOKEN.get_secret_value()}",
                "Accept": "application/json",
            },
            timeout=self.TIMEOUT,
        )

    async def get_chat_completion(
        self,
        query: ChatCompletion,
    ) -> ChatCompletionResponse:
        response = await self.post("chat/completions", json=query)
        return typing.cast(ChatCompletionResponse, response.json())

    async def get_embedding(
        self,
        input_data: str | list[int],
    ) -> npt.NDArray[np.float32]:
        response = await self.post(
            "embeddings",
            json={
                "input": input_data,
                "model": OPENAI_EMBEDDINGS_MODEL,
            },
            extensions={
                "retry": lambda response: response.json()["data"][0]["embedding"]
                is None,
            },
        )
        embedding = response.json()["data"][0]["embedding"]

        if embedding is None:
            raise OpenAiException(f"OpenAI return None for embedding of {input_data}")

        return np.array(list(map(np.float32, embedding)))
