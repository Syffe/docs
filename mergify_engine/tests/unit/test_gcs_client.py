from mergify_engine import settings
from mergify_engine.clients import google_cloud_storage


async def test_gcs_upload(prepare_google_cloud_storage_setup: None) -> None:
    client = google_cloud_storage.GoogleCloudStorageClient(
        settings.LOG_EMBEDDER_GCS_CREDENTIALS,
    )
    await client.upload(settings.LOG_EMBEDDER_GCS_BUCKET, "foobar", b"data")
    blobs = list(client.list_blobs(settings.LOG_EMBEDDER_GCS_BUCKET))
    assert len(blobs) == 1
    assert blobs[0].name == "foobar"
