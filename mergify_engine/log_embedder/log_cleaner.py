import dataclasses
import re
import typing

import unidecode

from mergify_engine.log_embedder import utils


class RegexToolbox:
    # INDIVIDUAL REGEXES
    MANIPULATING_OBJECT_PATTERN: str = r"\s*(remote:)?\s*(Counting|Compressing|Receiving)\s+objects\s*:\s+\d{1,3}%\s*\(\d+/\d+\)(,\sdone.)?"
    RESOLVING_DELTAS_PATTERN: str = (
        r"\s*(remote:)?\s*Resolving\sdeltas\s*:\s+\d{1,3}%\s*\(\d+/\d+\)(,\sdone.)?"
    )
    TIMESTAMP_PATTERN: str = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(-\d{2}:\d{2}|\.\d*Z)"
    COLOR_PATTERN: str = r"\[[0-9;](.*)[mGK]"

    # REGEX LISTS
    SEARCH_REGEXES: tuple[str, str] = (
        MANIPULATING_OBJECT_PATTERN,
        RESOLVING_DELTAS_PATTERN,
    )
    SUB_REGEXES: tuple[str, str] = (TIMESTAMP_PATTERN, COLOR_PATTERN)

    # COMPILED REGEX LISTS
    COMPILED_SEARCH_REGEXES: typing.Pattern[str] = re.compile(
        r"|".join(SEARCH_REGEXES),
        re.IGNORECASE | re.VERBOSE,
    )

    COMPILED_SUB_REGEXES: typing.Pattern[str] = re.compile(
        r"|".join(SUB_REGEXES),
        re.IGNORECASE | re.VERBOSE,
    )

    @classmethod
    def apply_regex(cls, raw_log_line: str) -> str:
        if cls.COMPILED_SEARCH_REGEXES.search(raw_log_line):
            return ""

        return cls.COMPILED_SUB_REGEXES.sub("", raw_log_line)


class GeneralCleaningToolbox:
    @staticmethod
    def lower_case(log_line: str) -> str:
        return log_line.lower().strip()

    @staticmethod
    def clean_stopwords(log_line: str) -> str:
        return " ".join(w for w in log_line.split() if w not in utils.ENGLISH_STOPWORDS)

    @staticmethod
    def replace_special_characters(log_line: str) -> str:
        return unidecode.unidecode(log_line)

    @staticmethod
    def clean_one_letter_words(log_line: str) -> str:
        return " ".join(w for w in log_line.split() if len(w) > 1)

    @staticmethod
    def clean_non_alphanumeric_characters(log_line: str) -> str:
        return re.sub(r"[^\w\s]", " ", log_line).strip()

    @staticmethod
    def ensure_spacing(log_line: str) -> str:
        return re.sub(r"\s{2,}", " ", log_line)

    clean_functions = (
        lower_case,
        clean_stopwords,
        replace_special_characters,
        clean_one_letter_words,
    )

    @classmethod
    def apply_general_cleaning(cls, log_line: str, clean_non_alphanumeric: bool) -> str:
        for clean_fn in cls.clean_functions:
            log_line = clean_fn(log_line)

        if clean_non_alphanumeric:
            log_line = cls.clean_non_alphanumeric_characters(log_line)

        return cls.ensure_spacing(log_line)


@dataclasses.dataclass
class LogCleaner:
    regex_toolbox: RegexToolbox = dataclasses.field(
        default_factory=RegexToolbox, repr=False
    )
    general_cleaning_toolbox: GeneralCleaningToolbox = dataclasses.field(
        default_factory=GeneralCleaningToolbox, repr=False
    )

    def clean_line(
        self,
        raw_log_line: str,
        clean_non_alphanumeric: bool = False,
    ) -> str:
        # REGEX CLEANING
        line = self.regex_toolbox.apply_regex(raw_log_line)
        if not line:
            return line
        # GENERAL CLEANING
        return self.general_cleaning_toolbox.apply_general_cleaning(
            line, clean_non_alphanumeric
        )
