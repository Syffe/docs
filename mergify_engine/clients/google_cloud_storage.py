import asyncio
import json
import warnings


# NOTE(sileht): google still use pkg_resources...
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from google.cloud import storage
    from google.oauth2 import service_account

from mergify_engine.config import types


class GoogleCloudStorageClient(storage.Client):  # type: ignore[misc]
    def __init__(self, credentials: types.SecretStrFromBase64) -> None:
        super().__init__(
            credentials=service_account.Credentials.from_service_account_info(
                json.loads(credentials.get_secret_value()),
            ),
        )

    def _sync_upload(self, bucket_name: str, path: str, content: bytes) -> storage.Blob:
        bucket = self.bucket(bucket_name)
        blob = bucket.blob(path)
        return blob.upload_from_string(content)

    async def upload(self, bucket_name: str, path: str, content: bytes) -> None:
        # Google Client is not async and not thread safe, so we isolate it in a thread
        await asyncio.to_thread(self._sync_upload, bucket_name, path, content)

    def _sync_download(self, bucket_name: str, path: str) -> storage.Blob | None:
        bucket = self.bucket(bucket_name)
        return bucket.get_blob(path)

    async def download(self, bucket_name: str, path: str) -> storage.Blob | None:
        # Google Client is not async and not thread safe, so we isolate it in a thread
        return await asyncio.to_thread(self._sync_download, bucket_name, path)
