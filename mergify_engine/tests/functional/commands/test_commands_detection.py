# -*- encoding: utf-8 -*-
#
# Copyright © 2022 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import asyncio

from mergify_engine.tests.functional import base


class TestCommandsDetection(base.FunctionalTestBase):
    async def test_hidden_comment_not_detected_twice(self) -> None:
        await self.setup_repo()
        p1 = await self.create_pr()
        comment_id = await self.create_comment_as_admin(
            p1["number"], "@mergifyio update"
        )
        await self.run_engine()

        await self.wait_for("issue_comment", {"action": "created"})
        assert await self.hide_comment(p1["number"], comment_id)

        await self.wait_for("issue_comment", {"action": "edited"})
        await self.run_full_engine()

        # NOTE(greesb): We could also just wait_for "issue_comment/created", and check
        # that it times out (with the raised exceptions).
        # It works in RECORD mode but not in non-RECORD mode, because, for some unknown reason,
        # the exception raised in non-RECORD mode is an exception telling it cannot override
        # the cassettes.
        if base.RECORD:
            await asyncio.sleep(15)

        resp = await self.app.post(
            f"/refresh/{p1['base']['repo']['full_name']}/pull/{p1['number']}",
            headers={"X-Hub-Signature": "sha1=" + base.FAKE_HMAC},
        )
        assert resp.status_code == 202, resp.text
        await self.run_full_engine()

        comments = await self.get_issue_comments(p1["number"])
        assert len(comments) == 2
