from datetime import datetime
from os.path import dirname, join

import pytest
from city_scrapers_core.constants import BOARD, CITY_COUNCIL, COMMITTEE, NOT_CLASSIFIED
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.charnc_charlotte_city import (
    CharlotteAdvisoryBoardSpider,
    CharlotteCityCouncilBusinessMeetingsSpider,
    CharlotteCityCouncilCommitteeMeetingsSpider,
    CharlotteCityCouncilStrategySessionSpider,
    CharlotteCityZoningMeetingsSpider,
    CharlotteProjectPublicMeetingsSpider,
)

TEST_DIR = join(dirname(__file__), "files")


@pytest.fixture
def legistar_business_item():
    spider = CharlotteCityCouncilBusinessMeetingsSpider()
    response = file_response(
        join(TEST_DIR, "charnc_charlotte_city_legistar_business.html"),
        url="https://charlottenc.legistar.com/Calendar.aspx",
    )
    events = spider._parse_legistar_events(response)
    spider.parse_legistar(events)
    start = list(spider._legistar_by_start.keys())[0]
    return spider, start


@pytest.fixture
def safety_committee_item():
    spider = CharlotteCityCouncilCommitteeMeetingsSpider()
    response = file_response(
        join(TEST_DIR, "charnc_charlotte_city_safety_committee_detail.html"),
        url="https://www.charlottenc.gov/Events-directory/Safety-Committee",
    )
    response.meta["summary"] = {
        "source": "https://www.charlottenc.gov/Events-directory/Safety-Committee",
        "is_cancelled": False,
        "description": (
            "The committee reviews and recommends policies to increase safety "
            "for our residents and visitors."
        ),
    }
    with freeze_time("2026-03-31"):
        items = list(spider.parse_primary_detail(response))
    assert len(items) == 1
    return items[0]


@pytest.fixture
def strategy_session_item():
    spider = CharlotteCityCouncilStrategySessionSpider()
    start = datetime(2026, 3, 2, 9, 0)
    spider._legistar_by_start[start].append(
        {
            "title": "City Council Annual Strategy Meeting",
            "location": {
                "name": "The Ballantyne Hotel",
                "address": "10000 Ballantyne Commons Pkwy, Charlotte, NC, 28277",
            },
            "links": [
                {
                    "href": "https://charlottenc.legistar.com/View.ashx?M=A&ID=1397716&GUID=3343D179-3475-44F9-B45F-04FEE8EE5206",  # noqa
                    "title": "Agenda",
                }
            ],
            "source": "https://charlottenc.legistar.com/Calendar.aspx",
        }
    )
    response = file_response(
        join(TEST_DIR, "charnc_charlotte_city_strategy_day1_detail.html"),
        url="https://www.charlottenc.gov/Events-directory/2026-Annual-Strategy-Meeting",
    )
    response.meta["summary"] = {
        "source": "https://www.charlottenc.gov/Events-directory/2026-Annual-Strategy-Meeting",  # noqa
        "is_cancelled": False,
        "description": (
            "During this meeting, City Council and senior leadership convene "
            "offsite to discuss council priorities and set strategic direction "
            "for the year ahead."
        ),
    }
    with freeze_time("2026-03-31"):
        items = list(spider.parse_primary_detail(response))
    assert len(items) == 1
    return items[0]


@pytest.fixture
def legistar_zoning_item():
    spider = CharlotteCityZoningMeetingsSpider()
    response = file_response(
        join(TEST_DIR, "charnc_charlotte_city_legistar_zoning.html"),
        url="https://charlottenc.legistar.com/Calendar.aspx",
    )
    events = spider._parse_legistar_events(response)
    spider.parse_legistar(events)
    start = list(spider._legistar_by_start.keys())[0]
    return spider, start


@pytest.fixture
def crtpo_policy_board_item():
    spider = CharlotteAdvisoryBoardSpider()
    response = file_response(
        join(TEST_DIR, "charnc_charlotte_city_crtpo_policy_board_detail.html"),
        url="https://www.charlottenc.gov/Events-directory/CRTPO-Policy-Board",
    )
    response.meta["summary"] = {
        "source": "https://www.charlottenc.gov/Events-directory/CRTPO-Policy-Board",
        "is_cancelled": False,
        "description": "Standing CRTPO Policy Board monthly meeting.",
    }
    with freeze_time("2026-03-31"):
        items = list(spider.parse_primary_detail(response))
    assert len(items) == 1
    return items[0]


@pytest.fixture
def airport_neighborhood_item():
    spider = CharlotteProjectPublicMeetingsSpider()
    response = file_response(
        join(
            TEST_DIR, "charnc_charlotte_city_airport_neighborhood_committee_detail.html"
        ),
        url="https://www.charlottenc.gov/Events-directory/Airport-Neighborhood-Committee-Meeting",  # noqa
    )
    response.meta["summary"] = {
        "source": (
            "https://www.charlottenc.gov/Events-directory/"
            "Airport-Neighborhood-Committee-Meeting"
        ),
        "is_cancelled": False,
        "description": (
            "The Airport Neighborhood Committee (ANC) offers an open forum where "
            "residents, businesses and partners can be informed and discuss "
            "concerns related to the airport."
        ),
    }
    with freeze_time("2026-03-31"):
        items = list(spider.parse_primary_detail(response))
    assert len(items) == 5
    return items


def test_legistar_business_meeting(legistar_business_item):
    spider, start = legistar_business_item
    assert start in spider._legistar_by_start

    item = spider._legistar_by_start[start][0]
    assert item["title"] == "City Council Business Meeting"
    assert item["location"] == {
        "name": "Council Chamber",
        "address": "600 East 4th Street, Charlotte, NC 28202",
    }
    assert item["links"] == [
        {
            "href": "https://charlottenc.legistar.com/View.ashx?M=A&ID=1146416&GUID=904C52E8-783F-4DD4-AA0A-BD638847ABA5",  # noqa
            "title": "Agenda",
        },
        {
            "href": "https://charlottenc.legistar.com/View.ashx?M=M&ID=1146416&GUID=904C52E8-783F-4DD4-AA0A-BD638847ABA5",  # noqa
            "title": "Minutes",
        },
        {
            "href": "https://charlottenc.legistar.com/Video.aspx?Mode=Granicus&ID1=3496&Mode2=Video",  # noqa
            "title": "Video",
        },
    ]
    assert item["source"] == "https://charlottenc.legistar.com/Calendar.aspx"


def test_primary_detail_safety_committee(safety_committee_item):
    item = safety_committee_item
    assert item["title"] == "Safety Committee"
    assert item["description"] == (
        "The committee reviews and recommends policies to increase safety "
        "for our residents and visitors."
    )
    assert item["classification"] == COMMITTEE
    assert item["start"] == datetime(2027, 3, 1, 13, 0)
    assert item["end"] == datetime(2027, 3, 1, 14, 30)
    assert item["all_day"] is False
    assert item["time_notes"] == ""
    assert item["location"] == {
        "name": "Charlotte Mecklenburg Government Center",
        "address": "600 E. Fourth Street, Charlotte, NC, 28202",
    }
    assert item["links"] == []
    assert (
        item["source"]
        == "https://www.charlottenc.gov/Events-directory/Safety-Committee"
    )
    assert item["status"] == "tentative"
    assert (
        item["id"]
        == "charlotte_city_council_committee_meetings/202703011300/x/safety_committee"
    )


def test_primary_detail_strategy_session_with_legistar_agenda(strategy_session_item):
    item = strategy_session_item
    assert (
        item["title"]
        == "Special Meeting Notice: City Council Annual Strategy Meeting (Day 1)"
    )
    assert item["description"] == (
        "During this meeting, City Council and senior leadership convene "
        "offsite to discuss council priorities and set strategic direction "
        "for the year ahead."
    )
    assert item["classification"] == CITY_COUNCIL
    assert item["start"] == datetime(2026, 3, 2, 9, 0)
    assert item["end"] == datetime(2026, 3, 2, 21, 0)
    assert item["all_day"] is False
    assert item["time_notes"] == ""
    assert item["location"] == {
        "name": "The Ballantyne Hotel",
        "address": "10000 Ballantyne Commons Pkwy, Charlotte, NC, 28277",
    }
    assert item["links"] == [
        {
            "href": "https://charlottenc.legistar.com/View.ashx?M=A&ID=1397716&GUID=3343D179-3475-44F9-B45F-04FEE8EE5206",  # noqa
            "title": "Agenda",
        }
    ]
    assert (
        item["source"]
        == "https://www.charlottenc.gov/Events-directory/2026-Annual-Strategy-Meeting"
    )
    assert item["status"] == "passed"
    assert (
        item["id"] == "charlotte_city_council_strategy_session/202603020900/x/"
        "special_meeting_notice_city_council_annual_strategy_meeting_day_1_"
    )


def test_legistar_zoning_meeting(legistar_zoning_item):
    spider, start = legistar_zoning_item
    assert start in spider._legistar_by_start

    item = spider._legistar_by_start[start][0]
    assert item["title"] == "City Council Zoning Meeting"
    assert item["location"] == {
        "name": "Council Chamber",
        "address": "600 East 4th Street, Charlotte, NC 28202",
    }
    assert item["links"] == [
        {
            "href": "https://charlottenc.legistar.com/View.ashx?M=A&ID=1287857&GUID=51192E95-F2D1-4762-AE7F-859A1765952F",  # noqa
            "title": "Agenda",
        },
        {
            "href": "https://charlottenc.legistar.com/View.ashx?M=M&ID=1287857&GUID=51192E95-F2D1-4762-AE7F-859A1765952F",  # noqa
            "title": "Minutes",
        },
        {
            "href": "https://charlottenc.legistar.com/Video.aspx?Mode=Granicus&ID1=3534&Mode2=Video",  # noqa
            "title": "Video",
        },
    ]
    assert item["source"] == "https://charlottenc.legistar.com/Calendar.aspx"


def test_primary_detail_crtpo_policy_board(crtpo_policy_board_item):
    item = crtpo_policy_board_item
    assert item["title"] == "CRTPO Policy Board"
    assert item["description"] == "Standing CRTPO Policy Board monthly meeting."
    assert item["classification"] == BOARD
    assert item["start"] == datetime(2026, 10, 21, 18, 0)
    assert item["end"] == datetime(2026, 10, 21, 20, 0)
    assert item["all_day"] is False
    assert item["time_notes"] == ""
    assert item["location"] == {
        "name": "CMGC 267",
        "address": "600 E 4th Street, Charlotte, NC, 28202",
    }
    assert item["links"] == []
    assert (
        item["source"]
        == "https://www.charlottenc.gov/Events-directory/CRTPO-Policy-Board"
    )
    assert item["status"] == "tentative"
    assert item["id"] == "charlotte_advisory_board/202610211800/x/crtpo_policy_board"


def test_primary_detail_airport_neighborhood_committee_meeting(
    airport_neighborhood_item,
):
    items = airport_neighborhood_item
    item = next(i for i in items if i["start"] == datetime(2026, 12, 17, 18, 0))
    assert item["title"] == "Airport Neighborhood Committee (ANC) Meeting"
    assert item["description"] == (
        "The Airport Neighborhood Committee (ANC) offers an open forum where "
        "residents, businesses and partners can be informed and discuss "
        "concerns related to the airport."
    )
    assert item["classification"] == NOT_CLASSIFIED
    assert item["start"] == datetime(2026, 12, 17, 18, 0)
    assert item["end"] == datetime(2026, 12, 17, 19, 30)
    assert item["all_day"] is False
    assert item["time_notes"] == ""
    assert item["location"] == {
        "name": "CLT Center",
        "address": "5601 Wilkinson Blvd., Charlotte, NC, 28208",
    }
    assert item["links"] == []
    assert (
        item["source"]
        == "https://www.charlottenc.gov/Events-directory/Airport-Neighborhood-Committee-Meeting"  # noqa
    )
    assert item["status"] == "tentative"
    assert (
        item["id"] == "charlotte_project_public_meetings/202612171800/x/"
        "airport_neighborhood_committee_anc_meeting"
    )
