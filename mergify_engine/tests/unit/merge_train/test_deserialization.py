"""
###############
The cassettes must be in YAML (.yaml) format.
The cassette file name will be used as a test id to better identify which test is failing.

###############
If you want to display the `LOG.info` while running the tests to make sure it tested everything
correctly, you need to pass the following arguments to pytest:
-s --log-cli-level=INFO

###############
If you want to test a specific cassette, you can use `-k` like this (with only one backslash per bracket):
-k test_deserialize_train\\[your_cassette_name.yaml\\]

###############
If you want to test the value of a dictionnary in an array at a specific index without
having to write all of the dictionnary before the one you want to test, you can put
the key `__array_idx` in your dict. The value of this key will be used to compare the
element of the dict with the dict at the index `__array_idx` in the real object.
Example:
train_properties_asserts:
  waiting_pulls:
    - __array_idx: 1
      user_pull_request_number: 123

This will make sure that the index 1 of the attribute `waiting_pulls` of the real Train
object has a `user_pull_request_number` of `123`.

###############
The format of each cassette is as follow:

test_infos: free space to put whatever you like that might help
understand what the test is doing

train_dumped: the json dumped representation of the train, and each of its
sub/attributes,that will be passed to the `load_from_bytes` function of the Train.

train_properties_asserts: a yaml dictionnary with, the keys representing the name
of an attribute of the Train object, and, the values being the expected values
of the key in the deserialized object.
"""

import os
import pathlib
import typing

import daiquiri

from mergify_engine import yaml
from mergify_engine.queue import merge_train


LOG = daiquiri.getLogger(__name__)

AUTHORIZED_YAML_KEY_IN_CASSETTES = (
    "train_dumped",
    "train_properties_asserts",
    "test_infos",
)


def pytest_generate_tests(metafunc: typing.Any) -> None:
    if (
        "cassette_json_data" not in metafunc.fixturenames
        or "train_properties_asserts" not in metafunc.fixturenames
    ):
        return

    cassettes_dir = (
        pathlib.Path(os.path.dirname(__file__)) / "deserialization_cassettes"
    )
    test_data = []
    cassette_names = []
    for cassette_file in cassettes_dir.glob("*.yaml"):
        with open(cassette_file) as f:
            cassette_names.append(cassette_file.name)

            yaml_data = yaml.safe_load(f.read())

            for key in yaml_data.keys():
                if key not in AUTHORIZED_YAML_KEY_IN_CASSETTES:
                    raise RuntimeError(
                        f"Found unauthorized yaml key `{key}` in cassette `{cassette_file.name}`",
                    )

            # Encode the train dict for it to be in bytes like when reading from redis
            cassette_json_data = yaml_data["train_dumped"].encode()
            train_properties_asserts = yaml_data.get("train_properties_asserts", {})

            test_data.append(
                (
                    cassette_json_data,
                    train_properties_asserts,
                ),
            )

    metafunc.parametrize(
        (
            "cassette_json_data",
            "train_properties_asserts",
        ),
        test_data,
        ids=cassette_names,
    )


def _test_nested_variable(
    real_nested_variable: typing.Any,
    expected_nested_variable: typing.Any,
    property_path_name: str,
) -> None:
    LOG.info("Testing train property `%s`", property_path_name)

    if not isinstance(real_nested_variable, list | dict):
        assert real_nested_variable == expected_nested_variable
        return

    if isinstance(real_nested_variable, list):
        assert isinstance(expected_nested_variable, list)

        if not real_nested_variable:
            assert expected_nested_variable == []
            return

        for idx, elem in enumerate(expected_nested_variable):
            if isinstance(elem, dict) and "__array_idx" in elem:
                idx = elem.pop("__array_idx")

            _test_nested_variable(
                real_nested_variable[idx],
                elem,
                f"{property_path_name}[{idx}]",
            )

        return

    if isinstance(real_nested_variable, dict):
        assert isinstance(expected_nested_variable, dict)

        if not real_nested_variable:
            assert expected_nested_variable == {}
            return

        for key, value in expected_nested_variable.items():
            _test_nested_variable(
                real_nested_variable[key],
                value,
                f"{property_path_name}['{key}']",
            )

        return


async def test_deserialize_train(
    cassette_json_data: bytes,
    train_properties_asserts: dict[str, typing.Any],
    convoy: merge_train.Convoy,
) -> None:
    train = merge_train.Train(convoy)
    await train.load_from_bytes(cassette_json_data)

    train_reserialized = train.to_serialized(serialize_if_empty=True)
    assert train_reserialized is not None

    for property_name, expected_property_value in train_properties_asserts.items():
        assert property_name in train_reserialized

        if isinstance(expected_property_value, list | dict):
            _test_nested_variable(
                train_reserialized[property_name],  # type: ignore[literal-required]
                expected_property_value,
                property_name,
            )
        else:
            LOG.info("Testing train property `%s`", property_name)
            assert train_reserialized[property_name] == expected_property_value  # type: ignore[literal-required]
