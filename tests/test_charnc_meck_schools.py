from datetime import datetime
from os.path import dirname, join

import pytest
from city_scrapers_core.constants import BOARD, TENTATIVE
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.charnc_meck_schools import CharncMeckSchoolsSpider


@pytest.fixture
def spider():
    return CharncMeckSchoolsSpider()


@pytest.fixture
def meetings_response(spider):
    response = file_response(
        join(dirname(__file__), "files", "charnc_meck_schools_meetings.json"),
        url="https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeetingsList?open&0.123456789012345",  # noqa
    )
    with freeze_time("2026-04-13"):
        return list(spider._parse_boarddocs_list(response))


@pytest.fixture
def parsed_items(spider):
    detail_response = file_response(
        join(dirname(__file__), "files", "charnc_meck_schools_detail.html"),
        url="https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeeting?open&0.123456789012345",  # noqa
    )
    agenda_response = file_response(
        join(dirname(__file__), "files", "charnc_meck_schools_agenda.html"),
        url="https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetAgenda?open&0.123456789012345",  # noqa
    )
    agenda_response.meta["raw_title"] = (
        detail_response.css(".meeting-name::text").get() or spider.agency
    )
    agenda_response.meta["meeting_date"] = detail_response.css(
        ".meeting-date::text"
    ).get()
    agenda_response.meta["raw_description"] = " ".join(
        detail_response.css(".meeting-description::text").getall()
    )

    with freeze_time("2026-04-13"):
        return [item for item in spider._parse_boarddocs_meeting(agenda_response)]


def test_title(parsed_items):
    assert (
        parsed_items[0]["title"]
        == "Facilities, Finance, & Operations Committee Meeting"
    )


def test_description(parsed_items):
    assert isinstance(parsed_items[0]["description"], str)
    assert len(parsed_items[0]["description"]) > 0


def test_start(parsed_items):
    assert parsed_items[0]["start"] == datetime(2026, 4, 13, 13, 0)


def test_end(parsed_items):
    assert parsed_items[0]["end"] is None


def test_time_notes(parsed_items):
    assert parsed_items[0]["time_notes"] == ""


def test_id(parsed_items):
    assert (
        parsed_items[0]["id"]
        == "charnc_meck_schools/202604131300/x/facilities_finance_operations_committee_meeting"  # noqa
    )


def test_status(parsed_items):
    assert parsed_items[0]["status"] == TENTATIVE


def test_location(parsed_items):
    assert parsed_items[0]["location"] == {"name": "Virtual", "address": ""}


def test_source(parsed_items):
    assert (
        parsed_items[0]["source"]
        == "https://go.boarddocs.com/nc/cmsnc/Board.nsf/Public"
    )


def test_links(parsed_items):
    assert isinstance(parsed_items[0]["links"], list)


def test_classification(parsed_items):
    assert parsed_items[0]["classification"] == BOARD


def test_all_day(parsed_items):
    assert parsed_items[0]["all_day"] is False
