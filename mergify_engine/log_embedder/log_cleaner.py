import dataclasses
import enum
import re
import typing

import unidecode

from mergify_engine.log_embedder import utils


CYPRESS_PATTERNS = [
    "http ",
    "+--------",
    "-----",
    "============",
    "(attempt",
    "cypress:",
    "browser:",
    "passing:",
    "failing:",
    "pending:",
    "skipped:",
    "(screenshots)",
    "(results)",
    "passing (",
    "video:",
    "duration:",
    "(run finished)",
    "screenshots:",
    "spec ran: ",
    "show ",
    "timers tick",
    "redirect ",
    "display ",
    "handle ",
    "group ",
    "group ",
    "env:",
    "github_token:",
    "wait-on",
    "start:",
    "spec:",
    "info accepting",
    "devtools listening",
    "seconds ago",
    "start server command",
    "running: ",
    "node version:",
    "(run starting)",
    "specs: ",
    "running: ",
]
NPM_PATTERNS = [
    "npm run build",
    "run `npm fund`",
    "publish-summary:",
    "-----+---",
    "task without title",
    "version last used",
    "runtests:",
    "record:",
    "publish-summary:",
    "build app command",
    "with:",
    "component:",
    "packages, audited",
    "packages looking",
    "days ago",
    "cache size:",
    "cache saved successfully",
    "##[endgroup]",
    "##[group]run",
    "[command]/",
]


class LogTags(enum.Enum):
    NPM = "npm"
    CYPRESS = "cypress"


class RegexToolbox:
    # INDIVIDUAL REGEXES
    MANIPULATING_OBJECT_PATTERN: str = r"\s*(remote:)?\s*(Counting|Compressing|Receiving)\s+objects\s*:\s+\d{1,3}%\s*\(\d+/\d+\)(,\sdone.)?"
    RESOLVING_DELTAS_PATTERN: str = (
        r"\s*(remote:)?\s*Resolving\sdeltas\s*:\s+\d{1,3}%\s*\(\d+/\d+\)(,\sdone.)?"
    )
    TIMESTAMP_PATTERN: str = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(-\d{2}:\d{2}|\.\d*Z))|(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d*\s)"
    COLOR_PATTERN: str = r"(\[\d+[;mGK])(([\d+\s])([mGK]))*|\x1B"
    SPECIAL_CHARACTERS_PATTERN: str = r"━{1,}|•\s|={3,}|--{2,}|_{3,}|\*{2,}"
    # NOTE(Syffe): In this regex, the order of each pattern is important.
    # Because the regex anchors at the beginning of the string, if we detect a single space (\s)
    # it will be subbed directly, and thus because of the OR clause, the regex will not go check for
    # the special lines characters. This is why the special line characters are checked before the
    # single space pattern.
    START_OF_LINE_PATTERN: str = r"^(\s*[^└|^┌|^├]──*|\s)"

    # REGEX LISTS
    SEARCH_REGEXES: tuple[str, str] = (
        MANIPULATING_OBJECT_PATTERN,
        RESOLVING_DELTAS_PATTERN,
    )
    SUB_REGEXES: tuple[str, str] = (TIMESTAMP_PATTERN, COLOR_PATTERN)
    GPT_SUB_REGEXES: tuple[str, str, str] = (
        TIMESTAMP_PATTERN,
        COLOR_PATTERN,
        SPECIAL_CHARACTERS_PATTERN,
    )

    # COMPILED REGEX LISTS
    COMPILED_SEARCH_REGEXES: typing.Pattern[str] = re.compile(
        r"|".join(SEARCH_REGEXES),
        re.IGNORECASE | re.VERBOSE,
    )

    COMPILED_SUB_REGEXES: typing.Pattern[str] = re.compile(
        r"|".join(SUB_REGEXES),
        re.IGNORECASE | re.VERBOSE,
    )

    COMPILED_GPT_SUB_REGEXES: typing.Pattern[str] = re.compile(
        r"|".join(GPT_SUB_REGEXES),
        re.IGNORECASE | re.VERBOSE,
    )

    # NOTE(Syffe): Because regexes that checks beginning of string are anchored at
    # the beginning of the string, we can't execute them with the regular sequence
    # matching regexes. This is why they are executed separately.
    COMPILED_START_OF_LINE_REGEX: typing.Pattern[str] = re.compile(
        START_OF_LINE_PATTERN,
        re.IGNORECASE | re.VERBOSE,
    )

    @classmethod
    def apply_regex(cls, raw_log_line: str) -> str:
        if cls.COMPILED_SEARCH_REGEXES.search(raw_log_line):
            return ""

        return cls.COMPILED_SUB_REGEXES.sub("", raw_log_line)

    @classmethod
    def apply_gpt_regex(cls, raw_log_line: str) -> str:
        if cls.COMPILED_SEARCH_REGEXES.search(raw_log_line):
            return ""

        line = cls.COMPILED_GPT_SUB_REGEXES.sub("", raw_log_line)
        return cls.COMPILED_START_OF_LINE_REGEX.sub("", line)


class GeneralCleaningToolbox:
    @staticmethod
    def lower_case(log_line: str) -> str:
        return log_line.lower()

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

    @staticmethod
    def clean_number_inside_brackets(log_line: str) -> str:
        # NOTE(Syffe): We clear group of caracters such as [657] or [1234]
        return re.sub(r"\[\d*]", " ", log_line)

    @staticmethod
    def clean_cypress_verbosity(log_line: str) -> str:
        # NOTE(Syffe): we clear lines containing patterns such as (123ms) or (68685ms)
        ms_steps = re.search(r"(\(\d*ms\))", log_line)
        if ms_steps is not None:
            return ""

        if any(cypress_pattern in log_line for cypress_pattern in CYPRESS_PATTERNS):
            return ""

        return log_line

    @staticmethod
    def clean_npm_verbosity(log_line: str) -> str:
        if any(npm_pattern in log_line for npm_pattern in NPM_PATTERNS):
            return ""

        return log_line

    clean_functions = (
        lower_case,
        clean_stopwords,
        replace_special_characters,
        clean_one_letter_words,
        clean_number_inside_brackets,
    )

    @classmethod
    def apply_general_cleaning(
        cls,
        log_line: str,
        log_tags: list[LogTags],
        clean_non_alphanumeric: bool,
    ) -> str:
        for clean_fn in cls.clean_functions:
            log_line = clean_fn(log_line)

        if LogTags.NPM in log_tags:
            log_line = cls.clean_npm_verbosity(log_line)

        if LogTags.CYPRESS in log_tags:
            log_line = cls.clean_cypress_verbosity(log_line)

        if clean_non_alphanumeric:
            log_line = cls.clean_non_alphanumeric_characters(log_line)

        return cls.ensure_spacing(log_line)


@dataclasses.dataclass
class LogCleaner:
    regex_toolbox: RegexToolbox = dataclasses.field(
        default_factory=RegexToolbox,
        repr=False,
    )
    general_cleaning_toolbox: GeneralCleaningToolbox = dataclasses.field(
        default_factory=GeneralCleaningToolbox,
        repr=False,
    )
    log_tags: list[LogTags] = dataclasses.field(default_factory=list)

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
            line,
            self.log_tags,
            clean_non_alphanumeric,
        )

    def gpt_clean_line(
        self,
        raw_log_line: str,
    ) -> str:
        line = self.regex_toolbox.apply_gpt_regex(raw_log_line)
        if not line:
            return line
        return self.general_cleaning_toolbox.clean_number_inside_brackets(line.rstrip())

    def apply_log_tags(self, raw_log: str) -> None:
        if re.search("npm run build", raw_log):
            self.log_tags.append(LogTags.NPM)

        if re.search(r"cypress:|\(run starting\)", raw_log):
            self.log_tags.append(LogTags.CYPRESS)
