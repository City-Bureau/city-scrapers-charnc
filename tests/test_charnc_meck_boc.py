"""
QA test suite for charnc_meck_boc spider.

Test categories:
  A. Schema validation  – all required fields present; correct types; naive datetimes
  B. Core output values – title, start, end, status, location, links, source, id
  C. Filtering logic    – non-meeting events, old events, deduplication
  D. Helper methods     – _clean_title, _is_non_meeting, _parse_classification,
                          _parse_location, _parse_dt, _significant_words,
                          _parse_legistar_start, _find_matching_legistar_event,
                          _legistar_links
  E. Error handling     – invalid JSON, missing/null fields
  F. Request chain      – start_requests, Legistar pagination, parse next-page
  G. Boundary conditions – since_year cutoff, source fallback, cancellation variants
"""

import json
import re
from datetime import datetime
from os.path import dirname, join

import pytest
from city_scrapers_core.constants import (
    ADVISORY_COMMITTEE,
    BOARD,
    CANCELLED,
    COMMISSION,
    COMMITTEE,
    NOT_CLASSIFIED,
    PASSED,
    TENTATIVE,
)
from city_scrapers_core.utils import file_response
from freezegun import freeze_time
from scrapy.http import TextResponse

from city_scrapers.spiders.charnc_meck_boc import CharncMeckBocSpider

# ---------------------------------------------------------------------------
# Module-level fixtures
# ---------------------------------------------------------------------------

test_response = file_response(
    join(dirname(__file__), "files", "charnc_meck_boc.json"),
    url=(
        "https://calendar.mecknc.gov/jsonapi/node/event"
        "?page%5Boffset%5D=0&page%5Blimit%5D=50"
    ),
)

with open(join(dirname(__file__), "files", "charnc_meck_boc_legistar.json")) as f:
    legistar_data = json.load(f)

# Spider instantiated inside freeze_time so _get_status() is deterministic.
freezer = freeze_time("2026-04-09")
freezer.start()

spider = CharncMeckBocSpider()
spider.legistar_events = legistar_data

parsed_items = [item for item in spider.parse(test_response)]

freezer.stop()


# ---------------------------------------------------------------------------
# A. Schema validation
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = [
    "id",
    "title",
    "description",
    "classification",
    "status",
    "start",
    "end",
    "all_day",
    "time_notes",
    "location",
    "links",
    "source",
]


@pytest.mark.parametrize("item", parsed_items)
def test_schema_all_required_fields_present(item):
    """Every yielded item must contain every field in the schema."""
    for field in REQUIRED_FIELDS:
        assert field in item, f"Missing required field: {field}"


@pytest.mark.parametrize("item", parsed_items)
def test_schema_start_is_naive_datetime(item):
    """start must be a timezone-naive datetime per project schema requirements."""
    assert isinstance(item["start"], datetime)
    assert item["start"].tzinfo is None, "start must not be timezone-aware"


@pytest.mark.parametrize("item", parsed_items)
def test_schema_end_is_naive_or_none(item):
    """end must be a timezone-naive datetime or None."""
    if item["end"] is not None:
        assert isinstance(item["end"], datetime)
        assert item["end"].tzinfo is None, "end must not be timezone-aware"


@pytest.mark.parametrize("item", parsed_items)
def test_schema_id_format(item):
    """id must match pattern: spider_name/YYYYMMDDHHMI/x/title_slug."""
    assert re.match(r"charnc_meck_boc/\d{12}/x/[\w_]+", item["id"])


@pytest.mark.parametrize("item", parsed_items)
def test_schema_location_structure(item):
    """location must be a dict with string 'name' and 'address' keys."""
    loc = item["location"]
    assert isinstance(loc, dict)
    assert "name" in loc and "address" in loc
    assert isinstance(loc["name"], str)
    assert isinstance(loc["address"], str)


@pytest.mark.parametrize("item", parsed_items)
def test_schema_links_structure(item):
    """Every link must be a dict with string 'href' and 'title' keys."""
    assert isinstance(item["links"], list)
    for link in item["links"]:
        assert "href" in link and "title" in link
        assert isinstance(link["href"], str) and link["href"]
        assert isinstance(link["title"], str) and link["title"]


@pytest.mark.parametrize("item", parsed_items)
def test_schema_all_day_is_false(item):
    assert item["all_day"] is False


@pytest.mark.parametrize("item", parsed_items)
def test_schema_description_is_string(item):
    assert isinstance(item["description"], str)


@pytest.mark.parametrize("item", parsed_items)
def test_schema_time_notes_is_string(item):
    assert isinstance(item["time_notes"], str)


@pytest.mark.parametrize("item", parsed_items)
def test_schema_source_is_nonempty_string(item):
    assert isinstance(item["source"], str) and item["source"]


# ---------------------------------------------------------------------------
# B. Core output values
# ---------------------------------------------------------------------------


def test_count():
    # Fixture has 6 calendar events:
    #   1. BOCC regular
    #   2. Budget/Policy  (fuzzy-matched to Legistar EventId=1802 → merged)
    #   3. Planning Commission
    #   4. Cancelled Waste Management (prefix stripped)
    #   5. BOCC regular duplicate (same id → deduplicated)
    #   6. Office closure (filtered by _is_non_meeting)
    # Fixture has 2 Legistar events:
    #   EventId=1802: Budget Public Policy 2025-01-14 → matched to calendar item 2
    #   EventId=1895: Board of Commissioners 2026-04-07 → unmatched → yielded standalone
    # Expected output: 5 meetings (4 from calendar + 1 Legistar-only).
    assert len(parsed_items) == 5


def test_title():
    assert (
        parsed_items[0]["title"]
        == "Mecklenburg Board of County Commissioners Regular Meeting"
    )
    assert parsed_items[1]["title"] == (
        "Mecklenburg Board of County Commissioners, Budget/Public Policy Meeting"
    )
    assert parsed_items[2]["title"] == "Charlotte-Mecklenburg Planning Commission"
    # Cancelled: "WILL NOT BE HELD:" prefix stripped from display title.
    assert parsed_items[3]["title"] == "Waste Management Advisory Board"
    # Legistar-only event (no matching calendar entry).
    assert parsed_items[4]["title"] == "Board of Commissioners"


def test_title_trailing_whitespace_stripped():
    """Fixture title for BOCC regular has trailing spaces; output must be clean."""
    assert not parsed_items[0]["title"].endswith(" ")


def test_start():
    # UTC 2026-04-21T22:00:00 → EDT (UTC-4) naive = 2026-04-21 18:00:00
    assert parsed_items[0]["start"] == datetime(2026, 4, 21, 18, 0)
    # UTC 2025-01-14T19:30:00 → EST (UTC-5) naive = 2025-01-14 14:30:00
    assert parsed_items[1]["start"] == datetime(2025, 1, 14, 14, 30)
    # UTC 2026-05-11T20:00:00 → EDT (UTC-4) naive = 2026-05-11 16:00:00
    assert parsed_items[2]["start"] == datetime(2026, 5, 11, 16, 0)
    # Legistar-only: EventDate=2026-04-07 + EventTime="5:00 PM"
    assert parsed_items[4]["start"] == datetime(2026, 4, 7, 17, 0)


def test_end():
    # UTC 2026-04-22T00:00:00 → EDT (UTC-4) naive = 2026-04-21 20:00:00
    assert parsed_items[0]["end"] == datetime(2026, 4, 21, 20, 0)
    # UTC 2025-01-14T21:00:00 → EST (UTC-5) naive = 2025-01-14 16:00:00
    assert parsed_items[1]["end"] == datetime(2025, 1, 14, 16, 0)
    # UTC 2026-05-11T22:00:00 → EDT (UTC-4) naive = 2026-05-11 18:00:00
    assert parsed_items[2]["end"] == datetime(2026, 5, 11, 18, 0)
    # UTC 2026-07-21T21:00:00 → EDT (UTC-4) naive = 2026-07-21 17:00:00
    assert parsed_items[3]["end"] == datetime(2026, 7, 21, 17, 0)


def test_all_day():
    assert parsed_items[0]["all_day"] is False


def test_time_notes():
    assert parsed_items[0]["time_notes"] == ""


def test_description():
    assert parsed_items[0]["description"] == ""


def test_classification():
    assert parsed_items[0]["classification"] == BOARD
    assert parsed_items[1]["classification"] == BOARD
    assert parsed_items[2]["classification"] == COMMISSION
    assert parsed_items[3]["classification"] == ADVISORY_COMMITTEE
    assert parsed_items[4]["classification"] == BOARD


def test_status():
    assert parsed_items[0]["status"] == TENTATIVE  # future
    assert parsed_items[1]["status"] == PASSED  # past
    assert parsed_items[2]["status"] == TENTATIVE  # future
    assert parsed_items[3]["status"] == CANCELLED  # "WILL NOT BE HELD:" in raw title
    assert parsed_items[4]["status"] == PASSED  # 2026-04-07 is past (frozen 2026-04-09)


def test_location():
    # line1 is a street → goes to address; line2 is room → goes to name
    assert parsed_items[0]["location"] == {
        "name": "Meeting Chamber",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }
    assert parsed_items[1]["location"] == {
        "name": "Room 267",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }
    # Planning Commission: address_line2 is empty → name is empty
    assert parsed_items[2]["location"] == {
        "name": "",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }
    # Cancelled event: field_event_address is null → empty location
    assert parsed_items[3]["location"] == {"name": "", "address": ""}


def test_links():
    # BOCC regular on 2026-04-21: no Legistar event on that date
    assert parsed_items[0]["links"] == []
    # Budget/Policy on 2025-01-14: fuzzy-matched to Legistar EventId=1802
    assert parsed_items[1]["links"] == [
        {
            "href": (
                "https://legistar1.granicus.com/Mecklenburg/meetings/2025/1"
                "/1802_A_Budget_Public_Policy_25-01-14_Meeting_Agenda.pdf"
            ),
            "title": "Agenda",
        }
    ]
    # Planning Commission: no Legistar match
    assert parsed_items[2]["links"] == []


def test_legistar_only_links():
    """Unmatched Legistar event must carry its own agenda link."""
    assert parsed_items[4]["links"] == [
        {
            "href": (
                "https://legistar1.granicus.com/Mecklenburg/meetings/2026/4"
                "/1895_A_Board_of_Commissioners_26-04-07_Meeting_Agenda.pdf"
            ),
            "title": "Agenda",
        }
    ]


def test_legistar_only_source():
    """Unmatched Legistar event source must be the Legistar meeting detail URL."""
    assert parsed_items[4]["source"] == (
        "https://mecklenburg.legistar.com/MeetingDetail.aspx"
        "?LEGID=1895&GID=194&G=9CAF76CE-14E5-4AB9-8178-ECFA61E02FDA"
    )


def test_legistar_only_description():
    """Unmatched Legistar event description comes from EventComment."""
    assert parsed_items[4]["description"] == "REVISED AGENDA"


def test_legistar_only_location():
    """Unmatched Legistar event with null EventLocation yields empty location."""
    assert parsed_items[4]["location"] == {"name": "", "address": ""}


def test_source():
    assert parsed_items[0]["source"] == (
        "https://calendar.mecknc.gov/event"
        "/mecklenburg-board-county-commissioners-regular-meeting"
    )


def test_id():
    assert parsed_items[0]["id"] == (
        "charnc_meck_boc/202604211800/x/"
        "mecklenburg_board_of_county_commissioners_regular_meeting"
    )


def test_legistar_only_id():
    assert parsed_items[4]["id"] == (
        "charnc_meck_boc/202604071700/x/board_of_commissioners"
    )


# ---------------------------------------------------------------------------
# C. Filtering logic
# ---------------------------------------------------------------------------


def test_non_meeting_filtered():
    """Office closure events must not appear in output."""
    titles = [item["title"] for item in parsed_items]
    assert not any("closed" in t.lower() for t in titles)


def test_deduplication():
    """Fixture has a duplicate BOCC regular; only one copy should appear."""
    bocc = [
        item
        for item in parsed_items
        if item["title"] == "Mecklenburg Board of County Commissioners Regular Meeting"
    ]
    assert len(bocc) == 1


def test_since_year_is_2022():
    """since_year must be the hardcoded constant 2022."""
    s = CharncMeckBocSpider()
    assert s.since_year == 2022


def test_old_calendar_events_included():
    """Calendar events before since_year must NOT be filtered.

    No year limit applies to the primary calendar source.
    """
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    old_body = json.dumps(
        {
            "data": [
                {
                    "type": "node--event",
                    "attributes": {
                        "title": "Old Board Meeting",
                        "field_event_datetime": [
                            {
                                "value": "2021-06-01T18:00:00+00:00",
                                "end_value": None,
                            }
                        ],
                        "field_event_address": None,
                        "absolute_url": "https://example.com/old",
                    },
                }
            ],
            "links": {},
        }
    ).encode()
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=old_body,
    )
    items = list(s.parse(response))
    assert len(items) == 1
    assert items[0]["title"] == "Old Board Meeting"
    assert items[0]["start"].year == 2021


def test_old_legistar_events_excluded():
    """Legistar events before since_year must be silently dropped."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    legistar_body = json.dumps(
        [
            {
                "EventId": 9999,
                "EventBodyName": "Old Board Meeting",
                "EventDate": "2021-06-01T00:00:00",
                "EventTime": "5:00 PM",
                "EventLocation": None,
                "EventAgendaFile": None,
                "EventMinutesFile": None,
                "EventVideoPath": None,
                "EventComment": "",
                "EventInSiteURL": None,
            }
        ]
    ).encode()
    response = TextResponse(
        url="https://webapi.legistar.com/v1/mecklenburg/events?%24top=1000&%24skip=0",
        body=legistar_body,
        headers={"Content-Type": "application/json"},
    )
    list(s._parse_legistar(response, skip=0))
    assert s.legistar_events == []


def test_source_falls_back_to_legistar_url():
    """When absolute_url is absent, source must be the Legistar calendar URL."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    body = json.dumps(
        {
            "data": [
                {
                    "type": "node--event",
                    "attributes": {
                        "title": "Board of Commissioners Meeting",
                        "field_event_datetime": [
                            {
                                "value": "2026-06-01T18:00:00+00:00",
                                "end_value": None,
                            }
                        ],
                        "field_event_address": None,
                        "absolute_url": None,  # no URL
                    },
                }
            ],
            "links": {},
        }
    ).encode()
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=body,
    )
    items = list(s.parse(response))
    assert len(items) == 1
    assert items[0]["source"] == s.legistar_url


# ---------------------------------------------------------------------------
# D. Helper method unit tests
# ---------------------------------------------------------------------------

# --- _clean_title ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            "WILL NOT BE HELD: Waste Management Advisory Board",
            "Waste Management Advisory Board",
        ),
        ("will not be held - Planning Commission", "Planning Commission"),
        ("WILL NOT BE HELD Board Meeting", "Board Meeting"),
        ("Will Not Be Held, Finance Committee", "Finance Committee"),
        ("  Board of Commissioners  ", "Board of Commissioners"),
        ("Board Meeting", "Board Meeting"),  # no prefix — unchanged
        (
            "Postponed Due to Weather - Board of Elections Meeting",
            "Board of Elections Meeting",
        ),
        (
            "Canceled - Criminal Justice Advisory Group (CJAG)",
            "Criminal Justice Advisory Group (CJAG)",
        ),
        ("Cancelled - Finance Committee", "Finance Committee"),
        ("Rescheduled: Planning Commission", "Planning Commission"),
    ],
)
def test_clean_title_variants(raw, expected):
    s = CharncMeckBocSpider()
    assert s._clean_title(raw) == expected


# --- _is_non_meeting ---


@pytest.mark.parametrize(
    "title,expected",
    [
        ("Mecklenburg County Government Offices Closed for Memorial Day", True),
        ("Government Offices Closed for Independence Day", True),
        ("GOVERNMENT OFFICES CLOSED", True),
        ("Board of Commissioners Regular Meeting", False),
        ("Planning Commission", False),
        ("Budget Public Policy", False),
    ],
)
def test_is_non_meeting(title, expected):
    s = CharncMeckBocSpider()
    assert s._is_non_meeting(title) == expected


# --- _parse_classification ---


@pytest.mark.parametrize(
    "title,expected",
    [
        ("BOCC Special Meeting", BOARD),  # starts with "bocc"
        ("Board of Commissioners", BOARD),
        ("Community Advisory Board", ADVISORY_COMMITTEE),  # advisory > board precedence
        ("Planning Commission", COMMISSION),
        ("Charlotte-Mecklenburg Planning Commission", COMMISSION),
        ("Finance Committee", COMMITTEE),
        ("Budget Public Policy Meeting", COMMITTEE),  # "policy" → COMMITTEE
        ("Mecklenburg County Tax Office", NOT_CLASSIFIED),
        ("Community Relations", NOT_CLASSIFIED),
    ],
)
def test_classification_variants(title, expected):
    s = CharncMeckBocSpider()
    assert s._parse_classification(title) == expected


# --- _clean_title_re ---


@pytest.mark.parametrize(
    "raw_title",
    [
        "WILL NOT BE HELD: Advisory Board Meeting",
        "will not be held - Finance Committee",
        "cancelled: Board Meeting",
        "CANCELED: Budget Workshop",  # American spelling
        "Postponed: Planning Commission",
        "RESCHEDULED: Board of Commissioners",
    ],
)
def test_cancellation_keywords_detected(raw_title):
    """_clean_title_re must match all cancellation phrase variants at title start."""
    assert CharncMeckBocSpider._clean_title_re.match(raw_title.strip()) is not None


# --- _parse_dt ---


def test_parse_dt_valid_utc_to_naive_edt():
    """UTC → EDT conversion must strip tzinfo from the result."""
    s = CharncMeckBocSpider()
    result = s._parse_dt(
        {"field_event_datetime": [{"value": "2026-04-21T22:00:00+00:00"}]},
        "value",
    )
    assert result == datetime(2026, 4, 21, 18, 0)
    assert result.tzinfo is None


def test_parse_dt_valid_utc_to_naive_est():
    """UTC → EST conversion (winter time, UTC-5)."""
    s = CharncMeckBocSpider()
    result = s._parse_dt(
        {"field_event_datetime": [{"value": "2025-01-14T19:30:00+00:00"}]},
        "value",
    )
    assert result == datetime(2025, 1, 14, 14, 30)
    assert result.tzinfo is None


def test_parse_dt_empty_list():
    s = CharncMeckBocSpider()
    assert s._parse_dt({"field_event_datetime": []}, "value") is None


def test_parse_dt_null_field():
    s = CharncMeckBocSpider()
    assert s._parse_dt({"field_event_datetime": None}, "value") is None


def test_parse_dt_missing_key():
    s = CharncMeckBocSpider()
    assert s._parse_dt({}, "value") is None


def test_parse_dt_null_value():
    s = CharncMeckBocSpider()
    assert s._parse_dt({"field_event_datetime": [{"value": None}]}, "value") is None


def test_parse_dt_malformed_string():
    """Malformed ISO string must log a warning and return None (not crash)."""
    s = CharncMeckBocSpider()
    assert (
        s._parse_dt({"field_event_datetime": [{"value": "not-a-date"}]}, "value")
        is None
    )


def test_parse_dt_missing_end_value():
    """Missing end_value key returns None without error."""
    s = CharncMeckBocSpider()
    assert (
        s._parse_dt(
            {"field_event_datetime": [{"value": "2026-04-21T22:00:00+00:00"}]},
            "end_value",
        )
        is None
    )


# --- _parse_location ---


def test_parse_location_null_addr():
    s = CharncMeckBocSpider()
    assert s._parse_location({"field_event_address": None}) == {
        "name": "",
        "address": "",
    }


def test_parse_location_missing_key():
    s = CharncMeckBocSpider()
    assert s._parse_location({}) == {"name": "", "address": ""}


def test_parse_location_empty_dict():
    s = CharncMeckBocSpider()
    assert s._parse_location({"field_event_address": {}}) == {
        "name": "",
        "address": "",
    }


def test_parse_location_string_value():
    """Non-dict address is coerced to string in the name field."""
    s = CharncMeckBocSpider()
    result = s._parse_location({"field_event_address": "Room 101"})
    assert result == {"name": "Room 101", "address": ""}


def test_parse_location_street_in_line1_splits_correctly():
    """Street in address_line1 → address; empty line2 → name is empty."""
    s = CharncMeckBocSpider()
    result = s._parse_location(
        {
            "field_event_address": {
                "address_line1": "600 E. 4th St",
                "address_line2": "",
                "locality": "Charlotte",
                "administrative_area": "NC",
                "postal_code": "28202",
            }
        }
    )
    assert result == {"name": "", "address": "600 E. 4th St, Charlotte, NC 28202"}


def test_parse_location_building_in_line1_street_in_line2():
    """Building name in line1, street in line2 → name=building, address=street."""
    s = CharncMeckBocSpider()
    result = s._parse_location(
        {
            "field_event_address": {
                "address_line1": "Employee Learning Center meeting room",
                "address_line2": "700 E. Fourth St.",
                "locality": "Charlotte",
                "administrative_area": "NC",
                "postal_code": "28202",
            }
        }
    )
    assert result == {
        "name": "Employee Learning Center meeting room",
        "address": "700 E. Fourth St., Charlotte, NC 28202",
    }


def test_parse_location_no_street_address_empty():
    """When no field contains a street number, address must be empty string."""
    s = CharncMeckBocSpider()
    result = s._parse_location(
        {
            "field_event_address": {
                "address_line1": "Meeting Room A",
                "address_line2": "Government Center",
            }
        }
    )
    assert result == {"name": "Meeting Room A, Government Center", "address": ""}


# --- _significant_words ---


def test_significant_words_strips_stop_words():
    s = CharncMeckBocSpider()
    assert s._significant_words("Board of Commissioners") == {"board", "commissioners"}


def test_significant_words_strips_punctuation():
    s = CharncMeckBocSpider()
    assert s._significant_words("Budget/Public Policy") == {
        "budget",
        "public",
        "policy",
    }


def test_significant_words_empty_string():
    s = CharncMeckBocSpider()
    assert s._significant_words("") == set()


def test_significant_words_all_stop_words():
    s = CharncMeckBocSpider()
    assert s._significant_words("a meeting of the") == set()


# --- _parse_legistar_start ---


def test_parse_legistar_start_with_time():
    s = CharncMeckBocSpider()
    result = s._parse_legistar_start(
        {"EventDate": "2025-01-14T00:00:00", "EventTime": "2:30 PM"}
    )
    assert result == datetime(2025, 1, 14, 14, 30)


def test_parse_legistar_start_no_time():
    """Missing EventTime must fall back to date-only parsing."""
    s = CharncMeckBocSpider()
    result = s._parse_legistar_start(
        {"EventDate": "2025-01-14T00:00:00", "EventTime": ""}
    )
    assert result == datetime(2025, 1, 14)


def test_parse_legistar_start_empty_date():
    s = CharncMeckBocSpider()
    assert s._parse_legistar_start({"EventDate": "", "EventTime": ""}) is None


def test_parse_legistar_start_bad_date():
    """Malformed date must log and return None (not crash)."""
    s = CharncMeckBocSpider()
    assert s._parse_legistar_start({"EventDate": "not-a-date", "EventTime": ""}) is None


# --- _get_status ---


def test_get_status_ignores_cancellation_in_description():
    """'Cancellation' checkbox in BOCC agenda boilerplate must NOT produce CANCELLED.

    BOCC meeting descriptions contain a form row:
    'Regular Meeting [X]  Special Meeting [ ]  Emergency Meeting [ ]  Cancellation [ ]
    This Meeting is rescheduled for ___'
    The base class _get_status scans description text and would falsely detect
    'Cancellation' as a cancellation keyword. Our override excludes description.
    """
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    item = {
        "title": "BOCC Intergovernmental Relations Committee",
        "description": (
            "Regular Meeting X  Special Meeting   Emergency Meeting   "
            "Cancellation   This Meeting is rescheduled for ___"
        ),
        "start": datetime(2026, 6, 1, 13, 0),  # future
    }
    assert s._get_status(item) == TENTATIVE


def test_get_status_subject_to_cancellation_not_cancelled():
    """Policy language 'subject to cancellation' must NOT mark a meeting CANCELLED."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    item = {
        "title": "BOCC Economic Development Committee",
        "description": (
            "3rd Tuesday meetings are subject to cancellation "
            "at the committee chair's discretion."
        ),
        "start": datetime(2026, 6, 1, 15, 30),  # future
    }
    assert s._get_status(item) == TENTATIVE


# --- _find_matching_legistar_event / _legistar_links ---


def test_match_legistar_links_empty_events():
    s = CharncMeckBocSpider()
    s.legistar_events = []
    date = datetime(2026, 4, 7).date()
    event = s._find_matching_legistar_event("Board of Commissioners", date)
    assert (s._legistar_links(event) if event else []) == []


def test_match_legistar_links_wrong_date():
    """Same body name but different date must not match."""
    s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-08T00:00:00",  # one day off
            "EventTime": "5:00 PM",
            "EventAgendaFile": "https://example.com/agenda.pdf",
            "EventMinutesFile": None,
            "EventVideoPath": None,
        }
    ]
    date = datetime(2026, 4, 7).date()
    event = s._find_matching_legistar_event("Board of Commissioners", date)
    assert (s._legistar_links(event) if event else []) == []


def test_match_legistar_links_single_word_does_not_match():
    """Sharing only one word (e.g. 'board') must NOT produce a match."""
    s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventAgendaFile": "https://example.com/bocc_agenda.pdf",
            "EventMinutesFile": None,
            "EventVideoPath": None,
        }
    ]
    # "Elections Board" shares only "board" with "Board of Commissioners"
    date = datetime(2026, 4, 7).date()
    event = s._find_matching_legistar_event("Elections Board", date)
    assert (s._legistar_links(event) if event else []) == []


def test_match_legistar_links_two_words_match():
    """Two shared significant words must produce a match."""
    s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventAgendaFile": "https://example.com/bocc_agenda.pdf",
            "EventMinutesFile": None,
            "EventVideoPath": None,
        }
    ]
    event = s._find_matching_legistar_event(
        "Mecklenburg Board of County Commissioners Regular Meeting",
        datetime(2026, 4, 7).date(),
    )
    result = s._legistar_links(event) if event else []
    assert result == [
        {"href": "https://example.com/bocc_agenda.pdf", "title": "Agenda"}
    ]


def test_match_legistar_links_includes_minutes_and_video():
    """All three link types (agenda, minutes, video) must be collected when present."""
    s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventAgendaFile": "https://example.com/agenda.pdf",
            "EventMinutesFile": "https://example.com/minutes.pdf",
            "EventVideoPath": "https://example.com/video.mp4",
        }
    ]
    event = s._find_matching_legistar_event(
        "Mecklenburg Board of County Commissioners Meeting", datetime(2026, 4, 7).date()
    )
    result = s._legistar_links(event) if event else []
    assert result == [
        {"href": "https://example.com/agenda.pdf", "title": "Agenda"},
        {"href": "https://example.com/minutes.pdf", "title": "Minutes"},
        {"href": "https://example.com/video.mp4", "title": "Video"},
    ]


def test_match_legistar_links_no_files_returns_empty():
    """Legistar match with no agenda/minutes/video must return empty list."""
    s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventAgendaFile": None,
            "EventMinutesFile": None,
            "EventVideoPath": None,
        }
    ]
    event = s._find_matching_legistar_event(
        "Mecklenburg Board of County Commissioners Meeting", datetime(2026, 4, 7).date()
    )
    result = s._legistar_links(event) if event else []
    assert result == []


# ---------------------------------------------------------------------------
# E. Error handling
# ---------------------------------------------------------------------------


def test_parse_invalid_json_returns_empty():
    """Invalid JSON body in parse() must log and yield nothing."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=b"not valid json {{",
    )
    assert list(s.parse(response)) == []


def test_parse_empty_data_array():
    """Empty data array must yield nothing."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=json.dumps({"data": [], "links": {}}).encode(),
    )
    assert list(s.parse(response)) == []


def test_parse_legistar_invalid_json():
    """Invalid JSON from Legistar must log and not add any events."""
    s = CharncMeckBocSpider()
    response = TextResponse(
        url="https://webapi.legistar.com/v1/mecklenburg/events?%24top=1000&%24skip=0",
        body=b"not valid json",
    )
    result = list(s._parse_legistar(response, skip=0))
    assert result == []
    assert s.legistar_events == []


def test_parse_event_missing_start_is_skipped():
    """Calendar event with no field_event_datetime must be silently skipped."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    body = json.dumps(
        {
            "data": [
                {
                    "type": "node--event",
                    "attributes": {
                        "title": "Board Meeting",
                        "field_event_datetime": [],
                        "field_event_address": None,
                        "absolute_url": "https://example.com/meeting",
                    },
                }
            ],
            "links": {},
        }
    ).encode()
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=body,
    )
    assert list(s.parse(response)) == []


def test_legistar_to_meeting_empty_title_returns_none():
    """_legistar_to_meeting must return None when EventBodyName is empty."""
    s = CharncMeckBocSpider()
    result = s._legistar_to_meeting(
        {
            "EventBodyName": "",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventLocation": None,
            "EventAgendaFile": None,
            "EventMinutesFile": None,
            "EventVideoPath": None,
            "EventComment": "",
            "EventInSiteURL": None,
        }
    )
    assert result is None


def test_legistar_to_meeting_missing_date_returns_none():
    """_legistar_to_meeting must return None when EventDate is absent."""
    s = CharncMeckBocSpider()
    result = s._legistar_to_meeting(
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "",
            "EventTime": "",
            "EventLocation": None,
            "EventAgendaFile": None,
            "EventMinutesFile": None,
            "EventVideoPath": None,
            "EventComment": "",
            "EventInSiteURL": None,
        }
    )
    assert result is None


def test_legistar_to_meeting_location_from_event_comment():
    """When EventLocation is null, location must be extracted from EventComment."""
    s = CharncMeckBocSpider()
    result = s._legistar_to_meeting(
        {
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventLocation": None,
            "EventAgendaFile": None,
            "EventMinutesFile": None,
            "EventVideoPath": None,
            "EventComment": (
                "Charlotte-Mecklenburg Government Center - Room 267\nSome extra note"
            ),
            "EventInSiteURL": (
                "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1"
            ),
        }
    )
    assert result is not None
    assert (
        result["location"]["name"]
        == "Charlotte-Mecklenburg Government Center - Room 267"
    )


def test_legistar_date_index_built_once():
    """Date index must not rebuild on subsequent calls (None sentinel)."""
    s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventId": 1,
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-04-07T00:00:00",
            "EventTime": "5:00 PM",
            "EventAgendaFile": "https://example.com/agenda.pdf",
            "EventMinutesFile": None,
            "EventVideoPath": None,
        }
    ]
    date = datetime(2026, 4, 7).date()
    assert s._legistar_by_date is None
    s._find_matching_legistar_event("Board of Commissioners", date)
    assert s._legistar_by_date is not None  # index now built
    first_index = id(s._legistar_by_date)
    s._find_matching_legistar_event("Board of Commissioners", date)
    assert id(s._legistar_by_date) == first_index  # same object, not rebuilt


def test_legistar_date_index_empty_events_not_rebuilt():
    """Empty legistar_events must not cause index to rebuild on every call."""
    s = CharncMeckBocSpider()
    s.legistar_events = []
    date = datetime(2026, 4, 7).date()
    s._find_matching_legistar_event("Board of Commissioners", date)
    assert s._legistar_by_date == {}  # built once, stays as empty dict
    s._find_matching_legistar_event("Board of Commissioners", date)
    assert s._legistar_by_date == {}  # still the same empty dict, not None


# ---------------------------------------------------------------------------
# F. Request chain
# ---------------------------------------------------------------------------


def test_start_requests():
    """start_requests() yields one Legistar request at skip=0 with since_year filter."""
    s = CharncMeckBocSpider()
    requests = list(s.start_requests())
    assert len(requests) == 1
    req = requests[0]
    assert "webapi.legistar.com" in req.url
    assert "%24top=1000" in req.url
    assert "%24skip=0" in req.url
    assert "2022" in req.url  # $filter includes since_year
    assert req.headers.get("Accept") == b"application/json"


def _legistar_response(num_events, skip=0):
    """Build a synthetic Legistar JSON response."""
    events = [
        {
            "EventId": i,
            "EventBodyName": f"Body {i}",
            "EventDate": "2025-01-01T00:00:00",
            "EventTime": "",
        }
        for i in range(num_events)
    ]
    return TextResponse(
        url=(
            f"https://webapi.legistar.com/v1/mecklenburg/events"
            f"?%24top=1000&%24skip={skip}"
        ),
        body=json.dumps(events).encode(),
        headers={"Content-Type": "application/json"},
    )


def test_legistar_pagination_continues():
    """Full page (1000 events) must yield a next Legistar request at skip=1000."""
    s = CharncMeckBocSpider()
    response = _legistar_response(num_events=s.legistar_page_size, skip=0)
    results = list(s._parse_legistar(response, skip=0))
    requests = [r for r in results if hasattr(r, "url")]
    assert len(requests) == 1
    assert "webapi.legistar.com" in requests[0].url
    assert "%24skip=1000" in requests[0].url  # next-page skip
    assert len(s.legistar_events) == s.legistar_page_size


def test_legistar_pagination_ends():
    """Partial page yields the primary calendar request, not another Legistar page."""
    s = CharncMeckBocSpider()
    response = _legistar_response(num_events=3, skip=0)
    results = list(s._parse_legistar(response, skip=0))
    requests = [r for r in results if hasattr(r, "url")]
    assert len(requests) == 1
    assert "calendar.mecknc.gov" in requests[0].url
    expected_ua = CharncMeckBocSpider._browser_ua.encode()
    assert requests[0].headers.get("User-Agent") == expected_ua
    assert requests[0].headers.get("Accept") == b"application/vnd.api+json"
    assert len(s.legistar_events) == 3


def test_legistar_pagination_accumulates():
    """Events from multiple Legistar pages must all be retained."""
    s = CharncMeckBocSpider()
    page1 = _legistar_response(num_events=s.legistar_page_size, skip=0)
    list(s._parse_legistar(page1, skip=0))
    page2 = _legistar_response(num_events=5, skip=s.legistar_page_size)
    list(s._parse_legistar(page2, skip=s.legistar_page_size))
    assert len(s.legistar_events) == s.legistar_page_size + 5


def test_calendar_fallback_yields_legistar_events():
    """When calendar returns 403, Legistar events are still yielded via parse()."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    s.legistar_events = [
        {
            "EventId": 42,
            "EventBodyName": "Board of Commissioners",
            "EventDate": "2026-06-01T00:00:00",
            "EventTime": "5:00 PM",
            "EventLocation": None,
            "EventAgendaFile": "https://example.com/agenda.pdf",
            "EventMinutesFile": None,
            "EventVideoPath": None,
            "EventComment": "",
            "EventInSiteURL": (
                "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=42"
            ),
        }
    ]
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        status=403,
        body=b"Forbidden",
    )
    items = list(s.parse(response))
    assert len(items) == 1
    assert items[0]["title"] == "Board of Commissioners"
    assert items[0]["links"] == [
        {"href": "https://example.com/agenda.pdf", "title": "Agenda"}
    ]


def test_parse_yields_next_page_request():
    """When response has a 'links.next', parse() must yield a follow-up request."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    next_url = (
        "https://calendar.mecknc.gov/jsonapi/node/event"
        "?page%5Boffset%5D=50&page%5Blimit%5D=50"
    )
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=json.dumps({"data": [], "links": {"next": {"href": next_url}}}).encode(),
    )
    results = list(s.parse(response))
    requests = [r for r in results if hasattr(r, "url")]
    assert len(requests) == 1
    assert requests[0].url == next_url
    expected_ua = CharncMeckBocSpider._browser_ua.encode()
    assert requests[0].headers.get("User-Agent") == expected_ua
    assert requests[0].headers.get("Accept") == b"application/vnd.api+json"


# ---------------------------------------------------------------------------
# G. Boundary conditions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_title,expected_status",
    [
        ("cancelled: Board Meeting", CANCELLED),
        ("CANCELED: Budget Workshop", CANCELLED),  # American spelling
        ("Postponed: Planning Commission", CANCELLED),
        ("RESCHEDULED: Board of Commissioners", CANCELLED),
        ("WILL NOT BE HELD: Advisory Board", CANCELLED),
    ],
)
def test_all_cancellation_variants_yield_cancelled_status(raw_title, expected_status):
    """All cancellation phrase variants must produce CANCELLED status."""
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    body = json.dumps(
        {
            "data": [
                {
                    "type": "node--event",
                    "attributes": {
                        "title": raw_title,
                        "field_event_datetime": [
                            {
                                "value": "2026-08-01T18:00:00+00:00",
                                "end_value": None,
                            }
                        ],
                        "field_event_address": None,
                        "absolute_url": "https://example.com/meeting",
                    },
                }
            ],
            "links": {},
        }
    ).encode()
    response = TextResponse(
        url=(
            "https://calendar.mecknc.gov/jsonapi/node/event"
            "?page%5Boffset%5D=0&page%5Blimit%5D=50"
        ),
        body=body,
    )
    items = list(s.parse(response))
    assert len(items) == 1
    assert items[0]["status"] == expected_status
