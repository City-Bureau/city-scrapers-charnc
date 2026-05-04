"""
QA test suite for charnc_meck_boc spider.

Test categories:
  A. Schema validation  – all required fields present; correct types; naive datetimes
  B. Core output values – title, start, end, status, location, links, source, id
  C. Filtering logic    – non-meeting events, old events, deduplication
  D. Helper methods     – _clean_title, _is_non_meeting, _parse_classification,
                          _split_location, _clean_location_name
  E. Boundary conditions – since_year cutoff, cancellation variants, source fallback
  F. Calendar path      – _parse_calendar merges primary + Legistar sources
  G. Error handling     – 403 and network error fallback to Legistar-only
"""

import json
import re
from datetime import datetime
from pathlib import Path

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
from freezegun import freeze_time
from scrapy.http import TextResponse

from city_scrapers.spiders.charnc_meck_boc import CharncMeckBocSpider

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_BOCC_URL = "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=2001&GID=194"
_BUDGET_URL = "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1802&GID=194"
_BUDGET_AGENDA = (
    "https://legistar1.granicus.com/Mecklenburg/meetings/2025/1"
    "/1802_A_Budget_Public_Policy_25-01-14_Meeting_Agenda.pdf"
)
_PLANNING_URL = (
    "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=2100&GID=194"
)
_PLANNING_AGENDA = (
    "https://legistar1.granicus.com/Mecklenburg/meetings/2026/5"
    "/2100_A_Planning_Commission_26-05-11_Meeting_Agenda.pdf"
)
_PLANNING_MINUTES = (
    "https://legistar1.granicus.com/Mecklenburg/meetings/2026/5"
    "/2100_M_Planning_Commission_26-05-11_Meeting_Minutes.pdf"
)
_WASTE_DETAILS_URL = (
    "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1750&GID=194"
)
_CALENDAR_URL = "https://mecklenburg.legistar.com/Calendar.aspx"

# Events as LegistarSpider._parse_legistar_events() would produce them.
# "Name", "Agenda", "Minutes", "Video" with URLs are dicts; plain text fields
# are strings. "iCalendar" is a dict used internally by LegistarSpider for dedup.
_TEST_EVENTS = [
    # 1. BOCC Regular — future (TENTATIVE), no links
    {
        "Name": {
            "label": "Mecklenburg Board of County Commissioners Regular Meeting",
            "url": _BOCC_URL,
        },
        "Meeting Date": "4/21/2026",
        "Meeting Time": "6:00 PM",
        "Meeting Location": "Meeting Chamber, 600 E. 4th St, Charlotte, NC 28202",
        "iCalendar": {"url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=2001"},
    },
    # 2. Budget/Policy — past (PASSED), with agenda link
    {
        "Name": {
            "label": (
                "Mecklenburg Board of County Commissioners,"
                " Budget/Public Policy Meeting"
            ),
            "url": _BUDGET_URL,
        },
        "Meeting Date": "1/14/2025",
        "Meeting Time": "2:30 PM",
        "Meeting Location": "Room 267, 600 E. 4th St, Charlotte, NC 28202",
        "Agenda": {"label": "Agenda", "url": _BUDGET_AGENDA},
        "iCalendar": {"url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1802"},
    },
    # 3. Planning Commission — future (TENTATIVE), agenda + minutes
    {
        "Name": {
            "label": "Charlotte-Mecklenburg Planning Commission",
            "url": _PLANNING_URL,
        },
        "Meeting Date": "5/11/2026",
        "Meeting Time": "4:00 PM",
        "Meeting Location": "600 E. 4th St, Charlotte, NC 28202",
        "Agenda": {"label": "Agenda", "url": _PLANNING_AGENDA},
        "Minutes": {"label": "Minutes", "url": _PLANNING_MINUTES},
        "iCalendar": {"url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=2100"},
    },
    # 4. Cancelled Waste Management — CANCELLED, name is plain string
    {
        "Name": "WILL NOT BE HELD: Waste Management Advisory Board",
        "Meeting Details": {"label": "Details", "url": _WASTE_DETAILS_URL},
        "Meeting Date": "7/21/2026",
        "Meeting Time": "12:00 PM",
        "Meeting Location": "",
        "iCalendar": {"url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1750"},
    },
    # 5. Non-meeting event — filtered by _is_non_meeting
    {
        "Name": {
            "label": "Mecklenburg County Government Offices Closed for Memorial Day",
            "url": "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=9999",
        },
        "Meeting Date": "5/26/2026",
        "Meeting Time": "8:00 AM",
        "Meeting Location": "",
        "iCalendar": {"url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=9999"},
    },
    # 6. Old event — filtered by since_year
    {
        "Name": {
            "label": "Board of Commissioners",
            "url": "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=100",
        },
        "Meeting Date": "6/1/2021",
        "Meeting Time": "5:00 PM",
        "Meeting Location": "",
        "iCalendar": {
            "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=100"
        },
    },
    # 7. Duplicate of event 1 — same title+date → same id → deduplicated
    {
        "Name": {
            "label": "Mecklenburg Board of County Commissioners Regular Meeting",
            "url": _BOCC_URL,
        },
        "Meeting Date": "4/21/2026",
        "Meeting Time": "6:00 PM",
        "Meeting Location": "Meeting Chamber, 600 E. 4th St, Charlotte, NC 28202",
        "iCalendar": {
            "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=20012"
        },
    },
]

# ---------------------------------------------------------------------------
# Module-level fixtures: spider + parsed items
# ---------------------------------------------------------------------------

freezer = freeze_time("2026-04-09")
freezer.start()

spider = CharncMeckBocSpider()
parsed_items = list(spider.parse_legistar(_TEST_EVENTS))

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
    for field in REQUIRED_FIELDS:
        assert field in item, f"Missing required field: {field}"


@pytest.mark.parametrize("item", parsed_items)
def test_schema_start_is_naive_datetime(item):
    assert isinstance(item["start"], datetime)
    assert item["start"].tzinfo is None


@pytest.mark.parametrize("item", parsed_items)
def test_schema_end_is_none(item):
    """LegistarSpider HTML source does not provide end times."""
    assert item["end"] is None


@pytest.mark.parametrize("item", parsed_items)
def test_schema_id_format(item):
    assert re.match(r"charnc_meck_boc/\d{12}/x/[\w_]+", item["id"])


@pytest.mark.parametrize("item", parsed_items)
def test_schema_location_structure(item):
    loc = item["location"]
    assert isinstance(loc, dict)
    assert "name" in loc and "address" in loc
    assert isinstance(loc["name"], str)
    assert isinstance(loc["address"], str)


@pytest.mark.parametrize("item", parsed_items)
def test_schema_links_structure(item):
    assert isinstance(item["links"], list)
    for link in item["links"]:
        assert "href" in link and "title" in link
        assert isinstance(link["href"], str) and link["href"]
        assert isinstance(link["title"], str) and link["title"]


@pytest.mark.parametrize("item", parsed_items)
def test_schema_all_day_is_false(item):
    assert item["all_day"] is False


@pytest.mark.parametrize("item", parsed_items)
def test_schema_description_is_empty_string(item):
    assert item["description"] == ""


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
    # Fixture has 7 events:
    #   1. BOCC regular       → yielded
    #   2. Budget/Policy      → yielded
    #   3. Planning Commission → yielded
    #   4. Cancelled (WILL NOT BE HELD: Waste Management) → yielded
    #   5. Office closure     → filtered by _is_non_meeting
    #   6. 2021 event         → filtered by since_year
    #   7. Duplicate of #1    → filtered by dedup
    # Expected: 4 meetings
    assert len(parsed_items) == 4


def test_title():
    assert (
        parsed_items[0]["title"]
        == "Mecklenburg Board of County Commissioners Regular Meeting"
    )
    assert parsed_items[1]["title"] == (
        "Mecklenburg Board of County Commissioners, Budget/Public Policy Meeting"
    )
    assert parsed_items[2]["title"] == "Charlotte-Mecklenburg Planning Commission"
    assert parsed_items[3]["title"] == "Waste Management Advisory Board"


def test_start():
    assert parsed_items[0]["start"] == datetime(2026, 4, 21, 18, 0)
    assert parsed_items[1]["start"] == datetime(2025, 1, 14, 14, 30)
    assert parsed_items[2]["start"] == datetime(2026, 5, 11, 16, 0)
    assert parsed_items[3]["start"] == datetime(2026, 7, 21, 12, 0)


def test_end():
    for item in parsed_items:
        assert item["end"] is None


def test_all_day():
    assert parsed_items[0]["all_day"] is False


def test_description():
    assert parsed_items[0]["description"] == ""


def test_classification():
    assert parsed_items[0]["classification"] == BOARD
    assert parsed_items[1]["classification"] == BOARD
    assert parsed_items[2]["classification"] == COMMISSION
    assert parsed_items[3]["classification"] == ADVISORY_COMMITTEE


def test_status():
    assert parsed_items[0]["status"] == TENTATIVE  # future (frozen 2026-04-09)
    assert parsed_items[1]["status"] == PASSED  # past
    assert parsed_items[2]["status"] == TENTATIVE  # future
    assert parsed_items[3]["status"] == CANCELLED  # "WILL NOT BE HELD:" prefix


def test_location():
    # "Meeting Chamber, 600 E. 4th St, Charlotte, NC 28202"
    # street at index 1 → name="Meeting Chamber", address=remainder
    assert parsed_items[0]["location"] == {
        "name": "Meeting Chamber",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }
    # "Room 267, 600 E. 4th St, Charlotte, NC 28202"
    # street at index 1 → name="Room 267"
    assert parsed_items[1]["location"] == {
        "name": "Room 267",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }
    # "600 E. 4th St, Charlotte, NC 28202"
    # street at index 0 → name=""
    assert parsed_items[2]["location"] == {
        "name": "",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }
    # empty Meeting Location
    assert parsed_items[3]["location"] == {"name": "", "address": ""}


def test_links():
    assert parsed_items[0]["links"] == []
    assert parsed_items[1]["links"] == [
        {"href": _BUDGET_AGENDA, "title": "Agenda"}
    ]
    assert parsed_items[2]["links"] == [
        {"href": _PLANNING_AGENDA, "title": "Agenda"},
        {"href": _PLANNING_MINUTES, "title": "Minutes"},
    ]
    assert parsed_items[3]["links"] == []


def test_source():
    assert parsed_items[0]["source"] == _BOCC_URL
    assert parsed_items[1]["source"] == _BUDGET_URL
    assert parsed_items[2]["source"] == _PLANNING_URL
    # Name is a plain string; falls through to Meeting Details url
    assert parsed_items[3]["source"] == _WASTE_DETAILS_URL


def test_source_fallback_to_calendar():
    """When neither Name nor Meeting Details has a URL, source is Calendar.aspx."""
    s = CharncMeckBocSpider()
    events = [
        {
            "Name": "Board of Commissioners Meeting",  # plain string, no url
            "Meeting Date": "6/1/2026",
            "Meeting Time": "5:00 PM",
            "Meeting Location": "",
            "iCalendar": {
                "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=X"
            },
        }
    ]
    items = list(s.parse_legistar(events))
    assert len(items) == 1
    assert items[0]["source"] == _CALENDAR_URL


def test_source_prefers_meeting_details_over_name():
    """'Meeting Details' URL (per-meeting) must win over 'Name' URL (per-department)."""
    s = CharncMeckBocSpider()
    dept_url = "https://mecklenburg.legistar.com/DepartmentDetail.aspx?ID=17716"
    meeting_url = "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1895"
    events = [
        {
            "Name": {"label": "Board of Commissioners", "url": dept_url},
            "Meeting Details": {"label": "Details", "url": meeting_url},
            "Meeting Date": "6/1/2026",
            "Meeting Time": "5:00 PM",
            "Meeting Location": "",
            "iCalendar": {
                "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1"
            },
        }
    ]
    items = list(s.parse_legistar(events))
    assert len(items) == 1
    assert items[0]["source"] == meeting_url


def test_location_as_dict_uses_label():
    """When Meeting Location is a linked cell, the label text must be used."""
    s = CharncMeckBocSpider()
    events = [
        {
            "Name": {
                "label": "Board of Commissioners",
                "url": "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1",
            },
            "Meeting Date": "6/1/2026",
            "Meeting Time": "5:00 PM",
            "Meeting Location": {
                "label": (
                    "Charlotte-Mecklenburg Government Center,"
                    " 600 E. 4th St, Charlotte, NC 28202"
                ),
                "url": "https://example.com/location",
            },
            "iCalendar": {
                "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1"
            },
        }
    ]
    items = list(s.parse_legistar(events))
    assert len(items) == 1
    assert items[0]["location"] == {
        "name": "Charlotte-Mecklenburg Government Center",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }


def test_id():
    assert parsed_items[0]["id"] == (
        "charnc_meck_boc/202604211800/x/"
        "mecklenburg_board_of_county_commissioners_regular_meeting"
    )
    assert parsed_items[3]["id"] == (
        "charnc_meck_boc/202607211200/x/waste_management_advisory_board"
    )


# ---------------------------------------------------------------------------
# C. Filtering logic
# ---------------------------------------------------------------------------


def test_non_meeting_filtered():
    titles = [item["title"] for item in parsed_items]
    assert not any("closed" in t.lower() for t in titles)


def test_deduplication():
    bocc = [
        item
        for item in parsed_items
        if item["title"] == "Mecklenburg Board of County Commissioners Regular Meeting"
    ]
    assert len(bocc) == 1


def test_since_year_is_2022():
    s = CharncMeckBocSpider()
    assert s.since_year == 2022


def test_old_events_excluded():
    s = CharncMeckBocSpider()
    events = [
        {
            "Name": {
                "label": "Old Board Meeting",
                "url": "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1",
            },
            "Meeting Date": "6/1/2021",
            "Meeting Time": "5:00 PM",
            "Meeting Location": "",
            "iCalendar": {
                "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1"
            },
        }
    ]
    assert list(s.parse_legistar(events)) == []


def test_empty_events_yields_nothing():
    s = CharncMeckBocSpider()
    assert list(s.parse_legistar([])) == []


# ---------------------------------------------------------------------------
# D. Helper methods
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
        ("Board Meeting", "Board Meeting"),
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
        ("BOCC Special Meeting", BOARD),
        ("Board of Commissioners", BOARD),
        ("Community Advisory Board", ADVISORY_COMMITTEE),
        ("Planning Commission", COMMISSION),
        ("Charlotte-Mecklenburg Planning Commission", COMMISSION),
        ("Finance Committee", COMMITTEE),
        ("Budget Public Policy Meeting", COMMITTEE),
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
        "CANCELED: Budget Workshop",
        "Postponed: Planning Commission",
        "RESCHEDULED: Board of Commissioners",
    ],
)
def test_cancellation_keywords_detected(raw_title):
    assert CharncMeckBocSpider._clean_title_re.match(raw_title.strip()) is not None


# --- _split_location ---


def test_split_location_street_first():
    """Street as first segment → empty name, full string as address."""
    s = CharncMeckBocSpider()
    result = s._split_location("600 E. 4th St, Charlotte, NC 28202")
    assert result == {"name": "", "address": "600 E. 4th St, Charlotte, NC 28202"}


def test_split_location_name_then_street():
    """Non-street name before street number → name captured, street in address."""
    s = CharncMeckBocSpider()
    result = s._split_location("Meeting Chamber, 600 E. 4th St, Charlotte, NC 28202")
    assert result == {
        "name": "Meeting Chamber",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }


def test_split_location_multi_name_then_street():
    """Multiple non-street segments before street."""
    s = CharncMeckBocSpider()
    result = s._split_location(
        "Central Piedmont Community College, Harris Conference Center, 3216 CPCC Drive"
    )
    assert result == {
        "name": "Central Piedmont Community College, Harris Conference Center",
        "address": "3216 CPCC Drive",
    }


def test_split_location_no_street():
    """No street number anywhere → entire string is name, address empty."""
    s = CharncMeckBocSpider()
    result = s._split_location("Government Center Auditorium")
    assert result == {"name": "Government Center Auditorium", "address": ""}


def test_split_location_empty():
    s = CharncMeckBocSpider()
    assert s._split_location("") == {"name": "", "address": ""}


def test_split_location_none():
    s = CharncMeckBocSpider()
    assert s._split_location(None) == {"name": "", "address": ""}


# --- _clean_location_name ---


def test_clean_location_name_strips_cancellation():
    s = CharncMeckBocSpider()
    result = s._clean_location_name(
        "600 East Fourth Street, Charlotte, NC 28202 WILL NOT BE HELD"
    )
    assert "WILL NOT BE HELD" not in result
    assert "600 East Fourth Street" in result


def test_clean_location_name_strips_cancelled_variant():
    s = CharncMeckBocSpider()
    result = s._clean_location_name("Government Center, 600 E. 4th St CANCELLED")
    assert "CANCELLED" not in result
    assert "Government Center" in result


def test_clean_location_name_linebreak_to_comma():
    s = CharncMeckBocSpider()
    result = s._clean_location_name("Room 267\n600 E. 4th St")
    assert result == "Room 267, 600 E. 4th St"


def test_clean_location_name_typo_correction():
    s = CharncMeckBocSpider()
    assert s._clean_location_name("Govenment Center") == "Government Center"


def test_clean_location_name_editorial_stripped():
    s = CharncMeckBocSpider()
    result = s._clean_location_name("Room 267 REVISED AGENDA")
    assert "REVISED" not in result


def test_clean_location_name_empty():
    s = CharncMeckBocSpider()
    assert s._clean_location_name("") == ""


def test_clean_location_name_none():
    s = CharncMeckBocSpider()
    assert s._clean_location_name(None) is None


# --- _get_status ---


def test_get_status_past_is_passed():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        item = {"title": "Board of Commissioners", "start": datetime(2026, 1, 1, 17, 0)}
        assert s._get_status(item) == PASSED


def test_get_status_future_is_tentative():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        item = {"title": "Board of Commissioners", "start": datetime(2026, 6, 1, 17, 0)}
        assert s._get_status(item) == TENTATIVE


def test_get_status_cancelled_via_text():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        item = {"title": "Board of Commissioners", "start": datetime(2026, 6, 1, 17, 0)}
        assert s._get_status(item, text="cancelled") == CANCELLED


# ---------------------------------------------------------------------------
# E. Boundary conditions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_title,expected_status",
    [
        ("cancelled: Board Meeting", CANCELLED),
        ("CANCELED: Budget Workshop", CANCELLED),
        ("Postponed: Planning Commission", CANCELLED),
        ("RESCHEDULED: Board of Commissioners", CANCELLED),
        ("WILL NOT BE HELD: Advisory Board", CANCELLED),
    ],
)
def test_all_cancellation_variants_yield_cancelled_status(raw_title, expected_status):
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        leg_url = "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1"
        events = [
            {
                "Name": {"label": raw_title, "url": leg_url},
                "Meeting Date": "8/1/2026",
                "Meeting Time": "6:00 PM",
                "Meeting Location": "",
                "iCalendar": {
                    "url": (
                        "https://mecklenburg.legistar.com/View.ashx"
                        f"?M=IC&ID={hash(raw_title)}"
                    )
                },
            }
        ]
        items = list(s.parse_legistar(events))
        assert len(items) == 1
        assert items[0]["status"] == expected_status


def test_missing_meeting_time_skips_event():
    """Meeting Time absent → legistar_start returns None → event skipped."""
    s = CharncMeckBocSpider()
    leg_url = "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1"
    events = [
        {
            "Name": {"label": "Board of Commissioners", "url": leg_url},
            "Meeting Date": "6/1/2026",
            "Meeting Time": None,
            "Meeting Location": "",
            "iCalendar": {
                "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1"
            },
        }
    ]
    assert list(s.parse_legistar(events)) == []


def test_empty_name_label_skips_event():
    """Event with empty Name label produces empty title and must be skipped."""
    s = CharncMeckBocSpider()
    leg_url = "https://mecklenburg.legistar.com/MeetingDetail.aspx?LEGID=1"
    events = [
        {
            "Name": {"label": "", "url": leg_url},
            "Meeting Date": "6/1/2026",
            "Meeting Time": "5:00 PM",
            "Meeting Location": "",
            "iCalendar": {
                "url": "https://mecklenburg.legistar.com/View.ashx?M=IC&ID=1"
            },
        }
    ]
    assert list(s.parse_legistar(events)) == []


# ---------------------------------------------------------------------------
# F. Calendar path (dual-source)
# ---------------------------------------------------------------------------

_FILES = Path(__file__).parent / "files"
_LEGISTAR_FIXTURE = json.loads((_FILES / "charnc_meck_boc_legistar.json").read_text())
_CAL_FIXTURE_BYTES = (_FILES / "charnc_meck_boc.json").read_bytes()

_CAL_BOCC_URL = (
    "https://calendar.mecknc.gov/event/"
    "mecklenburg-board-county-commissioners-regular-meeting"
)
_LEG_BOCC_URL = (
    "https://mecklenburg.legistar.com/MeetingDetail.aspx"
    "?LEGID=1895&GID=194&G=9CAF76CE-14E5-4AB9-8178-ECFA61E02FDA"
)

freezer_cal = freeze_time("2026-04-09")
freezer_cal.start()

cal_spider = CharncMeckBocSpider()
cal_spider.legistar_events = list(_LEGISTAR_FIXTURE)

cal_response = TextResponse(
    url="https://calendar.mecknc.gov/jsonapi/node/event",
    body=_CAL_FIXTURE_BYTES,
    encoding="utf-8",
)
cal_items = list(cal_spider._parse_calendar(cal_response))

freezer_cal.stop()


def test_cal_count():
    # Calendar fixture: BOCC Regular, Budget/Policy, Planning, Waste Mgmt CANCELLED,
    #   + duplicate BOCC (deduped) + Govt Offices Closed (filtered) = 4 calendar items
    # Legistar fixture: Budget/Policy (matched), BOCC Apr-7 (unmatched)
    # Unmatched Legistar: BOCC Apr-7 → 1 extra item
    assert len(cal_items) == 5


def test_cal_titles():
    titles = [i["title"] for i in cal_items]
    assert "Mecklenburg Board of County Commissioners Regular Meeting" in titles
    assert (
        "Mecklenburg Board of County Commissioners, Budget/Public Policy Meeting"
        in titles
    )
    assert "Charlotte-Mecklenburg Planning Commission" in titles
    assert "Waste Management Advisory Board" in titles
    assert "Board of Commissioners" in titles  # unmatched Legistar item


def test_cal_source_uses_calendar_url():
    bocc = next(i for i in cal_items if "Regular Meeting" in i["title"])
    assert bocc["source"] == _CAL_BOCC_URL


def test_cal_unmatched_legistar_uses_legistar_url():
    bocc_apr7 = next(i for i in cal_items if i["start"] == datetime(2026, 4, 7, 17, 0))
    assert bocc_apr7["source"] == _LEG_BOCC_URL


def test_cal_links_merged_from_legistar():
    # Budget/Policy calendar event picks up Agenda link from matched Legistar event.
    budget = next(i for i in cal_items if "Budget" in i["title"])
    assert len(budget["links"]) == 1
    assert budget["links"][0]["title"] == "Agenda"
    assert "1802" in budget["links"][0]["href"]


def test_cal_no_links_without_legistar_match():
    # BOCC Regular has no Legistar match in the fixture → empty links.
    bocc = next(i for i in cal_items if "Regular Meeting" in i["title"])
    assert bocc["links"] == []


def test_cal_end_set_from_calendar():
    # Calendar provides end times; Legistar-only items do not.
    bocc = next(i for i in cal_items if "Regular Meeting" in i["title"])
    # 2026-04-22T00:00:00+00:00 UTC = 2026-04-21 20:00 EDT
    assert bocc["end"] == datetime(2026, 4, 21, 20, 0)


def test_cal_unmatched_legistar_end_is_none():
    bocc_apr7 = next(i for i in cal_items if i["start"] == datetime(2026, 4, 7, 17, 0))
    assert bocc_apr7["end"] is None


def test_cal_start():
    bocc = next(i for i in cal_items if "Regular Meeting" in i["title"])
    # 2026-04-21T22:00:00+00:00 UTC = 2026-04-21 18:00 EDT
    assert bocc["start"] == datetime(2026, 4, 21, 18, 0)


def test_cal_location_from_address():
    bocc = next(i for i in cal_items if "Regular Meeting" in i["title"])
    assert bocc["location"] == {
        "name": "Meeting Chamber",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }


def test_cal_cancelled_status():
    waste = next(i for i in cal_items if "Waste Management" in i["title"])
    assert waste["status"] == CANCELLED


def test_cal_unmatched_legistar_status():
    # Board of Commissioners 2026-04-07, frozen at 2026-04-09 → PASSED
    bocc_apr7 = next(i for i in cal_items if i["start"] == datetime(2026, 4, 7, 17, 0))
    assert bocc_apr7["status"] == PASSED


def test_cal_description_empty_when_no_description_fields():
    # Fixture events have no field_date_time_description or field_details.
    for item in cal_items:
        assert item["description"] == ""


def test_cal_dedup_duplicate_calendar_event():
    bocc_items = [i for i in cal_items if "Regular Meeting" in i["title"]]
    assert len(bocc_items) == 1


# ---------------------------------------------------------------------------
# G. Error handling — fallback to Legistar-only
# ---------------------------------------------------------------------------


def test_403_falls_back_to_legistar_only():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
    s.legistar_events = list(_LEGISTAR_FIXTURE)
    forbidden = TextResponse(
        url="https://calendar.mecknc.gov/jsonapi/node/event",
        status=403,
        body=b"Forbidden",
        encoding="utf-8",
    )
    items = list(s._parse_calendar(forbidden))
    # Both legistar fixture events pass filtering (years >= 2022, valid titles)
    assert len(items) == 2
    titles = [i["title"] for i in items]
    assert any("Budget" in t for t in titles)
    assert any("Board of Commissioners" in t for t in titles)


def test_403_fallback_sources_are_legistar():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        s.legistar_events = list(_LEGISTAR_FIXTURE)
        forbidden = TextResponse(
            url="https://calendar.mecknc.gov/jsonapi/node/event",
            status=403,
            body=b"Forbidden",
            encoding="utf-8",
        )
        items = list(s._parse_calendar(forbidden))
        for item in items:
            assert "legistar.com" in item["source"]


def test_parse_description_strips_html():
    s = CharncMeckBocSpider()
    attrs = {"field_details": {"value": "<p>County offices are closed.</p>"}}
    assert s._parse_description(attrs) == "County offices are closed."


def test_parse_description_uses_field_date_time_description():
    s = CharncMeckBocSpider()
    attrs = {"field_date_time_description": "Meets the second Tuesday"}
    assert s._parse_description(attrs) == "Meets the second Tuesday"


def test_parse_description_none_returns_empty():
    s = CharncMeckBocSpider()
    assert s._parse_description(None) == ""


def test_parse_location_structured_address():
    s = CharncMeckBocSpider()
    attrs = {
        "field_event_address": {
            "address_line1": "600 E. 4th St",
            "address_line2": "Room 267",
            "locality": "Charlotte",
            "administrative_area": "NC",
            "postal_code": "28202",
            "organization": "",
        }
    }
    assert s._parse_location(attrs) == {
        "name": "Room 267",
        "address": "600 E. 4th St, Charlotte, NC 28202",
    }


def test_parse_location_org_sorted_by_street_re():
    """organization in street format goes to address, not name."""
    s = CharncMeckBocSpider()
    attrs = {
        "field_event_address": {
            "address_line1": "Hal Marshall Room",
            "address_line2": "2145 Suttle Ave",
            "locality": "Charlotte",
            "administrative_area": "NC",
            "postal_code": "28208",
            "organization": "",
        }
    }
    assert s._parse_location(attrs) == {
        "name": "Hal Marshall Room",
        "address": "2145 Suttle Ave, Charlotte, NC 28208",
    }


def test_parse_location_none_returns_empty():
    s = CharncMeckBocSpider()
    assert s._parse_location(None) == {"name": "", "address": ""}


def test_significant_words_excludes_stop_words():
    s = CharncMeckBocSpider()
    words = s._significant_words("Board of County Commissioners Regular Meeting")
    assert "of" not in words
    assert "board" in words
    assert "commissioners" in words


def test_find_matching_legistar_event_by_date_and_title():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        s.legistar_events = list(_LEGISTAR_FIXTURE)
        # Budget/Policy calendar event should match Legistar Budget/Policy on same date.
        match = s._find_matching_legistar_event(
            "Mecklenburg Board of County Commissioners, Budget/Public Policy Meeting",
            datetime(2025, 1, 14, 14, 30),
        )
        assert match is not None
        name = match.get("Name", {})
        label = name.get("label", "") if isinstance(name, dict) else str(name)
        assert "Budget" in label


def test_find_matching_legistar_event_no_match_wrong_date():
    with freeze_time("2026-04-09"):
        s = CharncMeckBocSpider()
        s.legistar_events = list(_LEGISTAR_FIXTURE)
        # Planning Commission has no Legistar event on its date.
        match = s._find_matching_legistar_event(
            "Charlotte-Mecklenburg Planning Commission",
            datetime(2026, 5, 11, 16, 0),
        )
        assert match is None
