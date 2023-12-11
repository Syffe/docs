# ruff: noqa: T201
import os

import msgpack
import yaml


for subdir, _, files in os.walk("zfixtures"):
    for file in files:
        if file == "http.yaml":
            yaml_file_path = os.path.join(subdir, file)
            msgpack_file_path = os.path.join(subdir, "http.msgpack")

            with open(yaml_file_path) as yaml_file:
                data = yaml.safe_load(yaml_file)

            with open(msgpack_file_path, "wb") as msgpack_file:
                msgpack.pack(data, msgpack_file)

            os.system("git rm " + yaml_file_path)

            print(f"Converted '{yaml_file_path}' to '{msgpack_file_path}'")
