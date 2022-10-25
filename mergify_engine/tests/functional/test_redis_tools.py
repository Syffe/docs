import base64
import json
import os
import shutil
import tempfile

from mergify_engine import redis_tools
from mergify_engine import yaml
from mergify_engine.tests.functional import base


class TestRedisTools(base.FunctionalTestBase):
    async def test_download_cached_config_files(self) -> None:
        rules = {
            "queue_rules": [
                {
                    "name": "urgent",
                    "conditions": [
                        "status-success=continuous-integration/fast-ci",
                    ],
                },
                {
                    "name": "default",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
                {
                    "name": "low-priority",
                    "conditions": [
                        "status-success=continuous-integration/slow-ci",
                    ],
                },
            ],
            "pull_request_rules": [
                {
                    "name": "Merge priority high",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-urgent",
                    ],
                    "actions": {"queue": {"name": "urgent"}},
                },
                {
                    "name": "Merge default",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue",
                    ],
                    "actions": {"queue": {"name": "default"}},
                },
                {
                    "name": "Merge low",
                    "conditions": [
                        f"base={self.main_branch_name}",
                        "label=queue-low",
                    ],
                    "actions": {"queue": {"name": "low-priority"}},
                },
            ],
        }
        await self.setup_repo(yaml.dump(rules))
        await self.repository_ctxt.get_mergify_config_file()
        temporary_folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temporary_folder)
        await redis_tools.download_redis_cached_keys(
            ["--path", temporary_folder, "--key", "non_existent_key"]
        )
        assert not os.path.exists(
            f"{temporary_folder}/config_file-{self.repository_ctxt.repo['id']}.txt"
        )

        await redis_tools.download_redis_cached_keys(["--path", temporary_folder])
        assert os.path.exists(
            f"{temporary_folder}/config_file-{self.repository_ctxt.repo['id']}.txt"
        )
        with open(
            f"{temporary_folder}/config_file-{self.repository_ctxt.repo['id']}.txt"
        ) as file:
            jsoned_content = json.loads(file.read())

        decoded_content_rules = (
            base64.b64decode(bytearray(jsoned_content["content"], "utf-8")).decode(),
        )
        jsoned_rules = json.loads(json.dumps(yaml.safe_load(decoded_content_rules[0])))
        assert jsoned_rules == rules
