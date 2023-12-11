import codecs
from collections import abc
import dataclasses
import re
import typing

from mergify_engine.log_embedder import log_cleaner
from mergify_engine.log_embedder import log_tag


LOG_CLEANER = log_cleaner.LogCleaner()


@dataclasses.dataclass
class Log:
    _content: str | None = dataclasses.field(default=None, compare=False, repr=False)
    _lines: list[str] | None = dataclasses.field(
        default=None,
        compare=False,
        repr=False,
    )
    _bytes: bytes | None = dataclasses.field(default=None, compare=False, repr=False)
    _tags: list[log_tag.LogTag] | None = None

    @property
    def tags(self) -> list[log_tag.LogTag]:
        if self._tags is None:
            self._tags = []

            if re.search("npm run build", self.content):
                self._tags.append(log_tag.LogTag.NPM)

            if re.search(r"cypress:|\(run starting\)", self.content):
                self._tags.append(log_tag.LogTag.CYPRESS)

        return self._tags

    def __eq__(self, other: typing.Any) -> bool:
        return isinstance(other, Log) and self.content == other.content

    def __repr__(self) -> str:
        return f"self.__class__.__name__({len(self)} bytes)"

    def __str__(self) -> str:
        return self.content

    def __len__(self) -> int:
        return len(self.content)

    @property
    def b(self) -> bytes:
        """Bytes."""
        if self._bytes is None:
            self._bytes = self.content.encode()
        return self._bytes

    @property
    def content(self) -> str:
        if self._content is None:
            if self._lines is not None:
                self._content = "\n".join(self.lines)
            elif self._bytes is not None:
                self._content = self._bytes.decode()
            else:
                raise ValueError("Nor _lines nor _content were provided")
        return self._content

    @property
    def lines(self) -> list[str]:
        if self._lines is None:
            self._lines = self.content.splitlines()
        return self._lines

    @classmethod
    def from_content(cls, content: str) -> "Log":
        return cls(_content=content)

    @classmethod
    def from_bytes(cls, content: bytes) -> "Log":
        return cls(_bytes=content)

    @classmethod
    def from_lines(cls, lines: list[str]) -> "Log":
        return cls(_lines=lines)

    def encode(self) -> bytes:
        return codecs.encode(self.b, encoding="zlib")

    @classmethod
    def decode(cls, content: bytes) -> "Log":
        return cls.from_bytes(codecs.decode(content, encoding="zlib"))

    def iter_gpt_cleaned_log_lines_reverse(self) -> abc.Generator[str, None, None]:
        for line in reversed(self.lines):
            yield LOG_CLEANER.gpt_clean_line(line)

    def extract(self, max_bytes: int) -> str:
        """Return an extract of the log that is bytes long at max, starting from the end."""
        if max_bytes < 0:
            raise ValueError("max_bytes must be positive")

        if max_bytes == 0:
            return ""

        content = self.content.strip()

        # Do we need to truncate?
        if len(content) <= max_bytes:
            return content

        ex = content[-max_bytes:]

        # now try to find if the first line we have in the extract is complete or not
        previous_char = content[-max_bytes - 1]

        # We split just before or on a \n, great
        if previous_char == "\n" or ex[0] == "\n":
            return ex.lstrip()

        # Otherwise remove the first line as it's incomplete
        try:
            newline = ex.index("\n")
        except ValueError:
            return ex

        return ex[newline + 1 :].lstrip()
