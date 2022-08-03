# -*- encoding: utf-8 -*-
#
# Copyright © 2020—2022 Mergify SAS
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

import dataclasses
import re
import typing

from mergify_engine import utils


@dataclasses.dataclass
class BaseAbortReason:
    message: typing.ClassVar[str]

    @property
    def code(self) -> str:
        # eg: BaseAbortReason -> BASE_ABORT_REASON
        return (
            re.sub(r"([A-Z][a-z]+)", r"\1_", self.__class__.__name__)
            .rstrip("_")
            .upper()
        )

    def __str__(self) -> str:
        format_vars = {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in ("code", "message")
        }
        return self.message.format(**format_vars)


@dataclasses.dataclass
class PrAheadDequeued(BaseAbortReason):
    message = "Pull request #{pr_number} which was ahead in the queue has been dequeued"  # noqa: FS003
    pr_number: int


@dataclasses.dataclass
class PrAheadFailedToMerge(BaseAbortReason):
    message = "Pull request ahead in queue failed to get merged"


@dataclasses.dataclass
class PrWithHigherPriorityQueued(BaseAbortReason):
    message = (
        "Pull request #{pr_number} with higher priority has been queued"  # noqa: FS003
    )
    pr_number: int


@dataclasses.dataclass
class PrQueuedTwice(BaseAbortReason):
    message = "The pull request has been queued twice"


@dataclasses.dataclass
class SpeculativeCheckNumberReduced(BaseAbortReason):
    message = "The number of speculative checks has been reduced"


@dataclasses.dataclass
class ChecksTimeout(BaseAbortReason):
    message = "Checks have timed out"


@dataclasses.dataclass
class ChecksFailed(BaseAbortReason):
    message = "Checks did not succeed"


@dataclasses.dataclass
class QueueRuleMissing(BaseAbortReason):
    message = "The associated queue rule does not exist anymore"


@dataclasses.dataclass
class UnexpectedQueueChange(BaseAbortReason):
    message = "Unexpected queue change: {change}"  # noqa: FS003
    change: str


def is_pr_body_a_merge_queue_pr(pull_request_body: typing.Optional[str]) -> bool:
    if pull_request_body is None:
        return False

    payload = utils.get_hidden_payload_from_comment_body(pull_request_body)
    if payload is None:
        return False

    return payload.get("merge-queue-pr", False)
