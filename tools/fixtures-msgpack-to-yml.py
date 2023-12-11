# ruff: noqa: T201
import pathlib
import sys

import msgpack
import yaml


if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} test-name")
    sys.exit(1)


for file in pathlib.Path("zfixtures/cassettes").glob(f"**/{sys.argv[1]}/http.msgpack"):
    print(f"Found file at {file}")
    yaml_file_path = file.parent / "http.yaml"
    with open(file, "rb") as msgpack_file:
        data = msgpack.unpackb(msgpack_file.read())

    with open(yaml_file_path, "w") as yaml_file:
        yaml_file.write(yaml.dump(data))

    print(f"Wrote yaml file at {yaml_file_path}")
