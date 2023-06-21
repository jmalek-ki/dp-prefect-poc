import datetime

import freezegun

from utilitites import bronzelogic

MOCK_TIME_ISO = "2020-01-01T22:11"
MOCK_TIME_DATETIME = datetime.datetime.fromisoformat(MOCK_TIME_ISO)


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

