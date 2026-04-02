from datetime import datetime
from os.path import dirname, join
from zoneinfo import ZoneInfo

import pytest
from city_scrapers_core.constants import BOARD, CANCELLED, PASSED, TENTATIVE
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.charnc_meck_library_board import CharncMeckLibraryBoardSpider

test_response = file_response(
    join(dirname(__file__), "files", "charnc_meck_library_board.html"),
    url="https://www.cmlibrary.org/board-trustees-meetings",
)
spider = CharncMeckLibraryBoardSpider()

freezer = freeze_time("2026-03-19")
freezer.start()

parsed_items = [item for item in spider.parse(test_response)]

freezer.stop()


def test_count():
    assert len(parsed_items) == 80


def test_title():
    assert parsed_items[0]["title"] == "Board of Trustees Meeting"


def test_description():
    assert parsed_items[0]["description"] == ""
    assert "public comment" in parsed_items[2]["description"]
    assert "Rachel Bradley" in parsed_items[2]["description"]


def test_start():
    assert parsed_items[0]["start"] == datetime(
        2026, 1, 20, 16, 0, tzinfo=ZoneInfo("America/New_York")
    )


def test_end():
    assert parsed_items[0]["end"] == datetime(
        2026, 1, 20, 17, 30, tzinfo=ZoneInfo("America/New_York")
    )


def test_time_notes():
    assert parsed_items[0]["time_notes"] == ""


def test_id():
    assert "charnc_meck_library_board" in parsed_items[0]["id"]


def test_status():
    assert parsed_items[0]["status"] == PASSED
    assert parsed_items[3]["status"] == TENTATIVE


def test_location():
    assert parsed_items[0]["location"] == {"name": "Virtual Meeting", "address": ""}
    assert parsed_items[1]["location"] == {
        "name": "ImaginOn: The Joe & Joan Martin Center",
        "address": "",
    }


def test_source():
    assert (
        parsed_items[0]["source"] == "https://www.cmlibrary.org/board-trustees-meetings"
    )


def test_links():
    agenda_href = (
        "https://cmlibrary.org/sites/default/files/2026-02/"
        "Board%20Meeting%20Agenda%2001.20.26.pdf"
    )
    minutes_href = (
        "https://cmlibrary.org/sites/default/files/2026-02/"
        "Attachment%20%231%20January%2020th%20BOT%20Meeting%20Minutes.pdf"
    )
    assert parsed_items[0]["links"] == [
        {"href": agenda_href, "title": "Agenda"},
        {"href": minutes_href, "title": "Minutes"},
    ]
    assert parsed_items[2]["links"] == []


def test_classification():
    assert parsed_items[0]["classification"] == BOARD


def test_cancelled():
    cancelled = [item for item in parsed_items if item["status"] == CANCELLED]
    assert len(cancelled) == 0


@pytest.mark.parametrize("item", parsed_items)
def test_all_day(item):
    assert item["all_day"] is False
