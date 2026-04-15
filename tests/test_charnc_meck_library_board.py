from datetime import datetime
from os.path import dirname, join

import pytest
from city_scrapers_core.constants import BOARD, CANCELLED, PASSED, TENTATIVE
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.charnc_meck_library_board import CharncMeckLibraryBoardSpider


@pytest.fixture
def test_response():
    return file_response(
        join(dirname(__file__), "files", "charnc_meck_library_board.html"),
        url="https://www.cmlibrary.org/board-trustees-meetings",
    )


@pytest.fixture
def spider():
    return CharncMeckLibraryBoardSpider()


@pytest.fixture
@freeze_time("2026-03-19")
def parsed_items(test_response, spider):
    return [item for item in spider.parse(test_response)]


def test_count(parsed_items):
    assert len(parsed_items) == 81


def test_title(parsed_items):
    assert parsed_items[0]["title"] == "Board of Trustees Meeting"


def test_description(parsed_items):
    assert parsed_items[0]["description"] == ""
    assert "public comment" in parsed_items[2]["description"]
    assert "Rachel Bradley" in parsed_items[2]["description"]


def test_start(parsed_items):
    assert parsed_items[0]["start"] == datetime(2026, 1, 20, 16, 0)


def test_end(parsed_items):
    assert parsed_items[0]["end"] == datetime(2026, 1, 20, 17, 30)


def test_time_notes(parsed_items):
    assert parsed_items[0]["time_notes"] == (
        "For more accurate meeting location, please refer to the "
        "meeting attachments."
    )


def test_id(parsed_items):
    assert "charnc_meck_library_board" in parsed_items[0]["id"]


def test_status(parsed_items):
    assert parsed_items[0]["status"] == PASSED
    assert parsed_items[3]["status"] == TENTATIVE


def test_location(parsed_items):
    assert parsed_items[0]["location"] == {"name": "Virtual Meeting", "address": ""}
    assert parsed_items[1]["location"] == {
        "name": "ImaginOn: The Joe & Joan Martin Center",
        "address": "",
    }


def test_source(parsed_items):
    assert (
        parsed_items[0]["source"] == "https://www.cmlibrary.org/board-trustees-meetings"
    )


def test_links(parsed_items):
    agenda_href = (
        "https://www.cmlibrary.org/sites/default/files/2026-02/"
        "Board%20Meeting%20Agenda%2001.20.26.pdf"
    )
    minutes_href = (
        "https://www.cmlibrary.org/sites/default/files/2026-02/"
        "Attachment%20%231%20January%2020th%20BOT%20Meeting%20Minutes.pdf"
    )
    assert parsed_items[0]["links"] == [
        {"href": agenda_href, "title": "Agenda"},
        {"href": minutes_href, "title": "Minutes"},
    ]
    assert parsed_items[2]["links"] == []


def test_classification(parsed_items):
    assert parsed_items[0]["classification"] == BOARD


def test_cancelled(parsed_items):
    cancelled = [item for item in parsed_items if item["status"] == CANCELLED]
    assert len(cancelled) == 1
    assert cancelled[0]["title"] == "Board of Trustees Meeting"
    assert cancelled[0]["start"] == datetime(2025, 7, 21, 16, 0)


def test_all_day(parsed_items):
    for item in parsed_items:
        assert item["all_day"] is False


def test_links_are_absolute(parsed_items):
    for item in parsed_items:
        for link in item["links"]:
            assert link["href"].startswith("http"), f"Relative URL: {link['href']}"


def test_no_title_contamination(parsed_items):
    for item in parsed_items:
        assert "Meeting will be held" not in item["title"]
