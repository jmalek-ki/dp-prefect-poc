import asyncio
import datetime
import json

import freezegun
import pytest

from gcp_storage_emulator.server import create_server
from google.cloud import storage

from utilitites import bronzelogic

MOCK_TIME_ISO = "2020-01-01T22:11"
MOCK_TIME_DATETIME = datetime.datetime.fromisoformat(MOCK_TIME_ISO)

TIMEDELTA_SLACK_SECONDS = 120

MOCK_GCS_HOST = "localhost"
MOCK_GCS_PORT = 9023

TEST_INP_BUCKET = "testbucket"
TEST_OUT_BUCKET = "testbucket_out"


@pytest.fixture
def mock_gcs():
    gcs_server = create_server(MOCK_GCS_HOST, MOCK_GCS_PORT, in_memory=True)
    gcs_server.start()

    yield gcs_server

    gcs_server.wipe()
    gcs_server.stop()
    return


def test_hour_versioning():
    test_blob_name = "test.json"
    with freezegun.freeze_time(MOCK_TIME_DATETIME):
        result = bronzelogic.date_hour_versioning(test_blob_name)
    assert result == "2020-01-01T22:00/test.json"


def test_minute_versioning():
    test_blob_name = "test.json"
    with freezegun.freeze_time(MOCK_TIME_DATETIME):
        result = bronzelogic.date_hrs_minutes_versioning(test_blob_name)
    assert result == "2020-01-01T22:11/test.json"


def test_dataflow(mock_gcs, monkeypatch):
    # Setup: mock out GCS & create resources
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", f"http://{MOCK_GCS_HOST}:{MOCK_GCS_PORT}")
    client = storage.Client()

    out_bucket = client.create_bucket(TEST_OUT_BUCKET)
    assert out_bucket

    inp_bucket = client.create_bucket(TEST_INP_BUCKET)
    assert inp_bucket

    test_blob_name = "testjson.json"
    inp_blob = inp_bucket.blob(test_blob_name)
    inp_blob.upload_from_string(json.dumps({
        "data": [
            "some",
            {
                "val": "important"
            },
            {
                "name": "important_int",
                "val": 123
            }
        ]
    }))

    # Core test logic:
    flow_coro = bronzelogic.version_blob_flow(
        test_blob_name,
        src_bucket_name=TEST_INP_BUCKET,
        trg_bucket=TEST_OUT_BUCKET,
        versioning_strategy=bronzelogic.date_hrs_minutes_versioning,
        gcp_creds={"is_legit": True}  # nonsense, just to stop it from fetching from the Block.
    )

    result: str = asyncio.run(flow_coro)
    assert result

    datestr, fname = result.split("/")
    assert fname == test_blob_name

    dirname_date = datetime.datetime.fromisoformat(datestr)
    now = datetime.datetime.utcnow()

    # Unfortunately, freezegun breaks Prefect flows (probably due to how the scheduler works),
    # so we have to kludge this using actual real time, hence the slack factor to account for drift.
    assert (now - dirname_date).total_seconds() < TIMEDELTA_SLACK_SECONDS
    return
