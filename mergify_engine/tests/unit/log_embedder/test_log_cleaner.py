import os
import re
from re import Match
import typing

import pytest

from mergify_engine.log_embedder import log_cleaner


CleanerModeT = typing.Literal["basic", "gpt"]

LOG_INPUT_LIBRARY_PATH = "zfixtures/unit/log_embedder/log_cleaner_input"
LOG_OUTPUT_LIBRARY_PATH = "zfixtures/unit/log_embedder/log_cleaner_output"
GPT_LOG_INPUT_LIBRARY_PATH = "zfixtures/unit/log_embedder/gpt_log_cleaner_input"
GPT_LOG_OUTPUT_LIBRARY_PATH = "zfixtures/unit/log_embedder/gpt_log_cleaner_output"

TEST_LOGS_FILEPATH_DICT = {
    filename: (
        os.path.join(LOG_INPUT_LIBRARY_PATH, filename),
        os.path.join(LOG_OUTPUT_LIBRARY_PATH, filename),
    )
    for filename in os.listdir(LOG_INPUT_LIBRARY_PATH)
}

TEST_GPT_LOGS_FILEPATH_DICT = {
    filename: (
        os.path.join(GPT_LOG_INPUT_LIBRARY_PATH, filename),
        os.path.join(GPT_LOG_OUTPUT_LIBRARY_PATH, filename),
    )
    for filename in os.listdir(GPT_LOG_INPUT_LIBRARY_PATH)
}


def get_cleaned_log(input_file: typing.TextIO, mode: CleanerModeT = "basic") -> str:
    cleaner = log_cleaner.LogCleaner()
    cleaned_lines = []
    for line in input_file:
        if mode == "gpt":
            cleaned_line = cleaner.gpt_clean_line(line)
        else:
            cleaned_line = cleaner.clean_line(line)
        if cleaned_line:
            cleaned_lines.append(cleaned_line)

    return "\n".join(cleaned_lines)


def record_log_cleaner_output() -> None:
    for log_filename in os.listdir(LOG_INPUT_LIBRARY_PATH):
        with open(
            f"{LOG_INPUT_LIBRARY_PATH}/{log_filename}",
            encoding="utf-8",
        ) as input_file, open(
            f"{LOG_OUTPUT_LIBRARY_PATH}/{log_filename}",
            "w",
        ) as output_file:
            log_output = get_cleaned_log(input_file)
            output_file.write(log_output)


def record_gpt_log_cleaner_output() -> None:
    for log_filename in os.listdir(GPT_LOG_INPUT_LIBRARY_PATH):
        with open(
            f"{GPT_LOG_INPUT_LIBRARY_PATH}/{log_filename}",
            encoding="utf-8",
        ) as input_file, open(
            f"{GPT_LOG_OUTPUT_LIBRARY_PATH}/{log_filename}",
            "w",
        ) as output_file:
            log_output = get_cleaned_log(input_file, mode="gpt")
            output_file.write(log_output)


@pytest.mark.parametrize(
    ("input_log_filepath", "output_log_filepath"),
    list(TEST_GPT_LOGS_FILEPATH_DICT.values()),
)
def test_gpt_log_cleaner_output(
    input_log_filepath: str,
    output_log_filepath: str,
) -> None:
    with open(input_log_filepath, encoding="utf-8") as log_input_file, open(
        output_log_filepath,
    ) as log_output_file:
        expected_log_output = log_output_file.read()
        log_output = get_cleaned_log(log_input_file, mode="gpt")
        assert log_output == expected_log_output


@pytest.mark.parametrize(
    ("input_log_filepath", "output_log_filepath"),
    list(TEST_LOGS_FILEPATH_DICT.values()),
)
def test_log_cleaner_output(input_log_filepath: str, output_log_filepath: str) -> None:
    with open(input_log_filepath, encoding="utf-8") as log_input_file, open(
        output_log_filepath,
    ) as log_output_file:
        expected_log_output = log_output_file.read()
        log_output = get_cleaned_log(log_input_file)
        assert log_output == expected_log_output


def is_regex_found(search: Match[str] | None) -> bool:
    return search is not None


@pytest.mark.parametrize(
    ("raw_log", "expected_result"),
    [
        # drop deltas resolving lines
        (
            "2023-04-25T07:49:02.4739906Z Resolving deltas:  35% (29628/84650) toto tutu",
            True,
        ),
        ("toto tutu Resolving deltas:  35% (29628/84650) fef zefzfzefzef", True),
        (
            "adazdzd 2023-04-25T07:48:33.5986060Z remote: Resolving deltas: 100% (127/127), done. toto tutu",
            True,
        ),
        ("Resolvingdeltas:  35% (29628/84650)", False),
        ("Resolving toto deltas:  35% (29628/84650)", False),
        ("remote: Compressing objects:  73% (93/127)", False),
        (
            "2023-04-25T07:49:14.5441296Z [command]/usr/bin/git checkout --progress --force refs/remotes/pull/5770/merge",
            False,
        ),
    ],
)
def test_resolving_deltas_lines(raw_log: str, expected_result: bool) -> None:
    cleaner = log_cleaner.LogCleaner()
    regex = re.compile(cleaner.regex_toolbox.RESOLVING_DELTAS_PATTERN, re.IGNORECASE)
    assert is_regex_found(regex.search(raw_log)) == expected_result


@pytest.mark.parametrize(
    ("raw_log", "expected_result"),
    [
        # drop objects handling lines
        ("remote: Compressing objects:  73% (93/127) toto tutu", True),
        ("toto tutu remote: Compressing objects:  73% (93/127)", True),
        (
            "a adzdazdd 2023-04-25T07:48:36.1979444Z Receiving objects:  27% (32083/118825), 103.12 MiB | 41.26 MiB/s adazazd",
            True,
        ),
        (
            "2023-04-25T07:48:33.4596377Z remote: Counting objects: 100% (214/214), done.   toto tutu",
            True,
        ),
        (
            " efzef af 2023-04-25T07:48:33.4611041Z tutu edzd remote: Compressing objects:   6% (8/127) ezf",
            True,
        ),
        ("remote: Compressingobjects:  73% (93/127) toto tutu", False),
        (
            "2023-04-25T07:48:33.4596377Z remote: Counting objects:100% (214/214), done.",
            False,
        ),
        ("2023-04-25T07:49:01.5394728Z Resolving deltas:  31% (26790/84650)", False),
        (
            "2023-04-25T07:49:14.5441296Z [command]/usr/bin/git checkout --progress --force refs/remotes/pull/5770/merge",
            False,
        ),
    ],
)
def test_clean_manipulating_objects_lines(raw_log: str, expected_result: bool) -> None:
    cleaner = log_cleaner.LogCleaner()
    regex = re.compile(cleaner.regex_toolbox.MANIPULATING_OBJECT_PATTERN, re.IGNORECASE)
    assert is_regex_found(regex.search(raw_log)) == expected_result


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # clean timestamps
        ("2021-05-11T14:16:48.7222537Z toto", " toto"),
        ("2022-05-11T02:00:03.01Z tutu", " tutu"),
        ("1990-12-31T15:59:60-08:00 tutu", " tutu"),
        ("tutu 1990-12-31T15:59:60-08:00", "tutu "),
        ("toto 2021-05-11T14:16:48.7222537Z", "toto "),
        ("2021-05-11 T14:16:48.7222537Z", "2021-05-11 T14:16:48.7222537Z"),
        ("2022-05-11T02: 00:03.01Z", "2022-05-11T02: 00:03.01Z"),
        (
            "2023-04-25T07:49:14.5441296Z [command]/usr/bin/git checkout --progress --force refs/remotes/pull/5770/merge",
            " [command]/usr/bin/git checkout --progress --force refs/remotes/pull/5770/merge",
        ),
    ],
)
def test_clean_timestamps(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    regex = re.compile(cleaner.regex_toolbox.TIMESTAMP_PATTERN, re.IGNORECASE)
    assert regex.sub("", raw_log) == cleaned_log


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # remove colors
        ("tutu [32msuccess[0m toto", "tutu success toto"),
        ("another tutu [31mfailed[0m toto", "another tutu failed toto"),
        ("[31mfailed [0m", "failed "),
        (
            "2023-04-25T07:49:14.5441296Z [command]/usr/bin/git checkout --progress --force refs/remotes/pull/5770/merge",
            "2023-04-25T07:49:14.5441296Z [command]/usr/bin/git checkout --progress --force refs/remotes/pull/5770/merge",
        ),
    ],
)
def test_clean_colors(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    regex = re.compile(cleaner.regex_toolbox.COLOR_PATTERN, re.IGNORECASE)
    assert regex.sub("", raw_log) == cleaned_log


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # lower casing
        ("ToToTuTu", "tototutu"),
        ("tatutito", "tatutito"),
        (
            "INFO: pip is looking at multiple versions",
            "info: pip is looking at multiple versions",
        ),
        ("Collecting virtualenv!=20.4.5", "collecting virtualenv!=20.4.5"),
        (
            "Temporarily overriding HOME='/home/runner/work/_temp/414c33dd-9bbf-4f69-b3b9-0720a54f8909'",
            "temporarily overriding home='/home/runner/work/_temp/414c33dd-9bbf-4f69-b3b9-0720a54f8909'",
        ),
    ],
)
def test_lower_case(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert cleaner.general_cleaning_toolbox.lower_case(raw_log) == cleaned_log


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # stripping
        ("  toto          ", "toto"),
        ("toto tata tutu", "toto tata tutu"),
        (
            "2023-04-25T07:49:14.5401980Z  * [new ref]             89ba3e87c5c2a94a332c78f22c23d476f6c63f2b -> pull/5770/merge",
            "[new ref] 89ba3e87c5c2a94a332c78f22c23d476f6c63f2b -> pull/5770/merge",
        ),
        (
            "2023-04-25T07:49:49.9502619Z   • Installing tryceratops (1.1.0)",
            "installing tryceratops (1.1.0)",
        ),
    ],
)
def test_stripping(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert cleaner.clean_line(raw_log) == cleaned_log


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # replace accents
        ("éèàùôî", "eeauoi"),
        ("hebete", "hebete"),
        ("hêbété", "hebete"),
        ("kožušček", "kozuscek"),
        ("#### toto", "#### toto"),
        ("\u5317\u4EB0", "Bei Jing "),
    ],
)
def test_special_characters(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert (
        cleaner.general_cleaning_toolbox.replace_special_characters(raw_log)
        == cleaned_log
    )


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # space reduction
        ("hello       world", "hello world"),
        ("hello world", "hello world"),
        (
            "2023-04-25T07:49:49.5137532Z   • Installing pytest-github-actions-annotate-failures (0.1.8)",
            "2023-04-25T07:49:49.5137532Z • Installing pytest-github-actions-annotate-failures (0.1.8)",
        ),
        (
            "2023-04-25T07:49:15.4934082Z   python-version: 3.11.3",
            "2023-04-25T07:49:15.4934082Z python-version: 3.11.3",
        ),
    ],
)
def test_space_reduction(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert cleaner.general_cleaning_toolbox.ensure_spacing(raw_log) == cleaned_log


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # drop one-letter words
        ("hello ab ty a 1 world 1 2 3", "hello ab ty world"),
        ("bonjour a comment i tu kzq q vas ?", "bonjour comment tu kzq vas"),
        (
            "aucun mot de moins de une lettre dans cette phrase",
            "aucun mot de moins de une lettre dans cette phrase",
        ),
        (
            "2023-04-25T07:48:27.6252507Z Job is waiting for a hosted runner to come online.",
            "2023-04-25T07:48:27.6252507Z Job is waiting for hosted runner to come online.",
        ),
    ],
)
def test_clean_one_letter_words(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert (
        cleaner.general_cleaning_toolbox.clean_one_letter_words(raw_log) == cleaned_log
    )


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # drop one-letter words
        (
            "2023-04-25T07:48:28.4788290Z Job is about to start running on the hosted runner: GitHub Actions 36 (hosted)",
            "2023-04-25T07:48:28.4788290Z Job start running hosted runner: GitHub Actions 36 (hosted)",
        ),
        (
            "2023-04-25T07:48:32.8319666Z Adding repository directory to the temporary git global config as a safe directory",
            "2023-04-25T07:48:32.8319666Z Adding repository directory temporary git global config safe directory",
        ),
        (
            "2023-04-25T07:49:15.4323922Z Turn off this advice by setting config variable advice.detachedHead to false",
            "2023-04-25T07:49:15.4323922Z Turn advice setting config variable advice.detachedHead false",
        ),
        (
            "2023-04-25T07:49:15.4321821Z If you want to create a new branch to retain commits you create, you may",
            "2023-04-25T07:49:15.4321821Z If want create new branch retain commits create, may",
        ),
        (
            "test sentence merge queue praise mergify random words following hazard sun motor mechanic ball obi wan",
            "test sentence merge queue praise mergify random words following hazard sun motor mechanic ball obi wan",
        ),
    ],
)
def test_clean_stopwords(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert cleaner.general_cleaning_toolbox.clean_stopwords(raw_log) == cleaned_log


@pytest.mark.parametrize(
    ("raw_log", "cleaned_log"),
    [
        # drop non-alphanumeric characters
        ("toto ? tutu, tata; for frodo!!!", "toto   tutu  tata  for frodo"),
        (
            "toto $$ tutu *** tata £££££ @@@ for the &&& gondor",
            "toto    tutu     tata           for the     gondor",
        ),
        (
            "2023-04-25T07:48:30.8041282Z ##[endgroup]",
            "2023 04 25T07 48 30 8041282Z    endgroup",
        ),
        (
            "2023-04-25T07:48:30.8003957Z Current runner version: '2.303.0'",
            "2023 04 25T07 48 30 8003957Z Current runner version   2 303 0",
        ),
        (
            "2023-04-25T07:48:33.4553952Z remote: Enumerating objects: 118825, done.",
            "2023 04 25T07 48 33 4553952Z remote  Enumerating objects  118825  done",
        ),
        (
            "there is no alphanumeric characters in this phrase please dont do nothing",
            "there is no alphanumeric characters in this phrase please dont do nothing",
        ),
    ],
)
def test_clean_non_alphanumeric_characters(raw_log: str, cleaned_log: str) -> None:
    cleaner = log_cleaner.LogCleaner()
    assert (
        cleaner.general_cleaning_toolbox.clean_non_alphanumeric_characters(raw_log)
        == cleaned_log
    )
