# ruff: noqa: T201
import pathlib

import msgpack
import yaml


for subdir, _, filenames in pathlib.Path("zfixtures").walk():
    for filename in filenames:
        if filename == "http.yaml":
            yaml_file_path = subdir / filename
            msgpack_file_path = subdir / "http.msgpack"

            with yaml_file_path.open() as yaml_file:
                data = yaml.safe_load(yaml_file)

            with msgpack_file_path.open("wb") as msgpack_file:
                msgpack.pack(data, msgpack_file)

            yaml_file_path.unlink()

            print(f"Converted '{yaml_file_path}' to '{msgpack_file_path}'")
