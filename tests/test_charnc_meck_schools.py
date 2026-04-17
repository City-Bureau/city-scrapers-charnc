from datetime import datetime
from os.path import dirname, join

import pytest
from city_scrapers_core.constants import BOARD, TENTATIVE
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.charnc_meck_schools import CharncMeckSchoolsSpider

TEST_DIR = dirname(__file__)


@pytest.fixture
def spider():
    return CharncMeckSchoolsSpider()


@pytest.fixture
def calendar_items(spider):
    # Simulate what _parse_boarddocs_list would set before calendar requests run.
    from datetime import date as date_type

    spider.last_boarddocs_date = date_type(2026, 4, 17)
    response = file_response(
        join(TEST_DIR, "files", "charnc_meck_schools_calendar.html"),
        url="https://www.cmsk12.org/fs/elements/241856?is_draft=false&cal_date=2026-04-01&is_load_more=true&page_id=29911&parent_id=241856",  # noqa
    )
    with freeze_time("2026-04-18"):
        return list(spider._parse_calendar(response))


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
    agenda_response.meta["meeting_id"] = "DSNPR265CF98"

    with freeze_time("2026-04-13"):
        return [item for item in spider._parse_boarddocs_meeting(agenda_response)]


def test_boarddocs_list_sets_last_date_dynamically(
    spider, meetings_response
):  # noqa: ARG001
    """last_boarddocs_date is the max date among *filtered* BoardDocs records."""
    from datetime import date as date_type

    # Fixture max numberdate within the filtered window is 20260417.
    assert spider.last_boarddocs_date == date_type(2026, 4, 17)


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
    links = parsed_items[0]["links"]
    assert len(links) == 1
    assert links[0]["title"] == "Download Agenda as PDF"
    assert "DSNPR265CF98" in links[0]["href"]
    assert not any(lnk["title"] == "Agenda Item" for lnk in links)


def test_classification(parsed_items):
    assert parsed_items[0]["classification"] == BOARD


def test_all_day(parsed_items):
    assert parsed_items[0]["all_day"] is False


# ---------------------------------------------------------------------------
# Calendar (Finalsite list-view) request tests
#
# _parse_calendar now yields scrapy.Request objects (Playwright detail
# requests) rather than Meeting items directly.  Tests verify the request
# structure and the event_data payload that the async Playwright callback
# will receive.
# ---------------------------------------------------------------------------


def test_calendar_request_count(calendar_items):
    """Fixture has 3 CMS Board events all after last_boarddocs_date (2026-04-17)."""
    assert len(calendar_items) == 3


def test_calendar_requests_are_playwright(calendar_items):
    import scrapy

    for req in calendar_items:
        assert isinstance(req, scrapy.Request)
        assert req.meta.get("playwright") is True
        assert req.meta.get("playwright_include_page") is True


def test_calendar_board_retreat_event_data(calendar_items):
    """Event with div.fsLocation – parsed correctly before Playwright fetch."""
    ed = calendar_items[0].meta["event_data"]
    assert ed["title"] == "Special Meeting of the Board"
    assert ed["time_notes"] == "Board Retreat"
    assert ed["location"]["name"] == "Graylyn International Conference Center"
    assert ed["location"]["address"] == "Winston Salem, NC"
    assert ed["start"] == datetime(2026, 4, 20, 8, 0)
    assert ed["end"] == datetime(2026, 4, 20, 17, 0)


def test_calendar_board_retreat_request_url(calendar_items):
    """URL for the Playwright request encodes the event ID and occurrence date."""
    url = calendar_items[0].url
    assert "event=106924861" in url
    assert "occur=2026-04-20" in url


def test_calendar_board_retreat_meeting_id(calendar_items):
    ed = calendar_items[0].meta["event_data"]
    assert (
        ed["meeting_id"]
        == "charnc_meck_schools/202604200800/x/special_meeting_of_the_board"
    )


def test_calendar_cmgc_event_data(calendar_items):
    """CMGC location is extracted from the event title when no div.fsLocation."""
    ed = calendar_items[1].meta["event_data"]
    assert ed["title"] == "Regular Meeting of the Board"
    assert ed["time_notes"] == "Closed Session at 4:00pm"
    assert ed["location"]["name"].startswith("Charlotte-Mecklenburg Government Center")
    assert (
        ed["location"]["address"] == ""
    )  # no description yet; address populated after Playwright modal


# ---------------------------------------------------------------------------
# Direct tests for calendar parsing helper methods
# ---------------------------------------------------------------------------


def test_parse_calendar_title_details_with_parens(spider):
    """Parenthetical becomes time_notes; title is stripped."""
    title, loc, notes = spider._parse_calendar_title_details(
        "Special Meeting of the Board (Board Retreat)"
    )
    assert title == "Special Meeting of the Board"
    assert notes == "Board Retreat"
    assert loc == {"name": "", "address": ""}


def test_parse_calendar_title_details_cmgc(spider):
    """CMGC location and closed-session notes are both extracted."""
    title, loc, notes = spider._parse_calendar_title_details(
        "Regular Meeting of the Board - CMGC Chamber Room (Closed Session at 4:00pm)"
    )
    assert title == "Regular Meeting of the Board"
    assert notes == "Closed Session at 4:00pm"
    assert loc["name"].startswith("Charlotte-Mecklenburg Government Center")
    assert (
        loc["address"] == ""
    )  # no description text available; address only populated via _parse_location


def test_parse_calendar_location_pipe_format(spider):
    """'Name | City, ST' format produces name/address split."""
    loc = spider._parse_calendar_location(
        "Graylyn International Conference Center | Winston Salem, NC"
    )
    assert loc["name"] == "Graylyn International Conference Center"
    assert loc["address"] == "Winston Salem, NC"


def test_parse_calendar_location_virtual(spider):
    loc = spider._parse_calendar_location("Virtual")
    assert loc == {"name": "Virtual", "address": ""}


def test_parse_calendar_location_cmgc_with_room(spider):
    loc = spider._parse_calendar_location("CMGC Chamber Room")
    assert loc["name"].startswith("Charlotte-Mecklenburg Government Center")
    assert "CHAMBER ROOM" in loc["name"]
    assert (
        loc["address"] == ""
    )  # no description text available; address only populated via _parse_location


def test_parse_location_cmgc_extracts_address_from_description(spider):
    """Address is parsed dynamically from description text, not hardcoded."""
    description = (
        "The Board will meet in person in the Chamber of the "
        "Charlotte-Mecklenburg Government Center.  "
        "Char-Meck Govt Center 600 East 4th Street Charlotte, NC 28202"
    )
    loc = spider._parse_location(description)
    assert loc["name"].startswith("Charlotte-Mecklenburg Government Center")
    assert "600 East 4th Street" in loc["address"]
    assert "NC 28202" in loc["address"]


def test_parse_location_cmgc_no_address_in_description(spider):
    """When description has no address, address is left empty."""
    description = (
        "The Board will meet in person in the Chamber Room of the "
        "Charlotte-Mecklenburg Government Center."
    )
    loc = spider._parse_location(description)
    assert loc["name"].startswith("Charlotte-Mecklenburg Government Center")
    assert loc["address"] == ""
