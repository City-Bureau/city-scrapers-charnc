from datetime import datetime
from os.path import dirname, join

import pytest
import scrapy
from city_scrapers_core.constants import NOT_CLASSIFIED, PASSED, TENTATIVE
from city_scrapers_core.utils import file_response
from freezegun import freeze_time

from city_scrapers.spiders.charnc_meck_pd import CharncMeckPdSpider


@pytest.fixture()
def spider():
    return CharncMeckPdSpider()


@pytest.fixture()
def list_response():
    return file_response(
        join(dirname(__file__), "files", "charnc_meck_pd.html"),
        url="https://www.charlottenc.gov/cmpd/Events-directory",
    )


@pytest.fixture()
def detail_response():
    response = file_response(
        join(dirname(__file__), "files", "charnc_meck_pd_detail.html"),
        url="https://www.charlottenc.gov/cmpd/Events-directory/Community-Meeting-JH-Gunn",  # noqa
    )
    response.meta["description"] = (
        "This is a community meeting for the JH Gunn Community, where of the assigned community coordinator will discuss crime stats with the residents of the community."  # noqa
        "This is a reoccurring event that occurs the second Saturday of every month"
    )
    return response


@pytest.fixture()
def parsed_list(spider, list_response):
    with freeze_time("2026-04-01"):
        return list(spider.parse_main_page(list_response))


@pytest.fixture()
def parsed_items(spider, detail_response):
    with freeze_time("2026-04-01"):
        return list(spider.parse(detail_response))


# List page tests


def test_events_list_yields_results(parsed_list):
    assert len(parsed_list) == 11


def test_list_yields_requests(parsed_list):

    assert all(isinstance(r, scrapy.http.Request) for r in parsed_list)


def test_list_requests_have_desc_in_meta(parsed_list):

    requests = [r for r in parsed_list if isinstance(r, scrapy.http.Request)]
    assert any("description" in r.meta for r in requests)


def test_list_requests_have_valid_urls(parsed_list):

    requests = [r for r in parsed_list if isinstance(r, scrapy.http.Request)]
    assert all(r.url.startswith("https://www.charlottenc.gov") for r in requests)


# Detail page tests


def test_count(parsed_items):
    assert len(parsed_items) == 50


def test_title(parsed_items):
    assert parsed_items[0]["title"] == "Community Meeting - JH Gunn"


def test_description(parsed_items):
    # description comes from meta (list page)
    assert "JH Gunn Community" in parsed_items[0]["description"]


def test_start(parsed_items):
    assert parsed_items[29]["start"] == datetime(2026, 4, 11, 14, 0)


def test_end(parsed_items):
    assert parsed_items[29]["end"] == datetime(2026, 4, 11, 16, 0)


def test_time_notes(parsed_items):
    assert parsed_items[29]["time_notes"] == ""


def test_status_future(parsed_items):
    # frozen at 2026-04-01, April 11 is in the future
    assert parsed_items[29]["status"] == TENTATIVE


def test_status_past(parsed_items):
    assert parsed_items[0]["status"] == PASSED


def test_location(parsed_items):
    assert parsed_items[29]["location"] == {
        "name": "Hickory Grove Division",
        "address": "9505 Parkton Road",
    }


def test_source(parsed_items):
    assert parsed_items[29]["source"] == (
        "https://www.charlottenc.gov/cmpd/Events-directory/Community-Meeting-JH-Gunn"  # noqa
    )


def test_links(parsed_items):
    assert parsed_items[29]["links"] == [{"href": "", "title": ""}]


def test_classification(parsed_items):
    assert parsed_items[29]["classification"] == NOT_CLASSIFIED


def test_all_day(parsed_items):
    assert parsed_items[29]["all_day"] is False


def test_all_items_have_title(parsed_items):
    assert all(item["title"] != "" for item in parsed_items)


def test_all_items_all_day(parsed_items):
    assert all(item["all_day"] is False for item in parsed_items)


def test_all_items_have_start(parsed_items):
    assert all(isinstance(item["start"], datetime) for item in parsed_items)


def test_all_items_classification(parsed_items):
    assert all(item["classification"] == NOT_CLASSIFIED for item in parsed_items)
