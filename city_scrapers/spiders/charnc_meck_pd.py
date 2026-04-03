import re
from datetime import datetime
from urllib.parse import quote

import scrapy
from city_scrapers_core.constants import NOT_CLASSIFIED
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider


class CharncMeckPdSpider(CityScrapersSpider):
    name = "charnc_meck_pd"
    agency = "Charlotte-Mecklenburg Police Department"
    timezone = "America/New_York"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    BASE_URL = "https://www.charlottenc.gov/cmpd/Events-directory"

    CATEGORY = "Community events & fundraisers"

    def start_requests(self):
        yield scrapy.Request(
            url=self._build_url(1),
            callback=self.parse_main_page,
            meta={"page": 1},
        )

    # Main Page
    def parse_main_page(self, response):
        page = response.meta.get("page", 1)

        # Extract event URLs and descriptions
        for article in response.css(".list-item-container article"):
            evnts_link = article.css("a::attr(href)").get()
            description = " ".join(
                " ".join(article.css(".list-item-block-desc ::text").getall()).split()
            )

            if evnts_link:
                yield response.follow(
                    evnts_link,
                    callback=self.parse,
                    meta={"description": description},
                )

        # Pagination
        total_pages = self._get_total_pages(response)

        if page < total_pages:
            next_page = page + 1
            yield scrapy.Request(
                url=self._build_url(next_page),
                callback=self.parse_main_page,
                meta={"page": next_page},
            )

    def _build_url(self, page):
        category = quote(self.CATEGORY)
        return (
            f"{self.BASE_URL}?"
            f"dlv_OC%20CL%20Police%20Events%20Listing="
            f"(dd_OC%20Composite%20Date=)"
            f"(dd_OC%20Event%20Categories={category})"
            f"(pageindex={page})"
        )

    def _get_total_pages(self, response):
        text = response.css(".seamless-pagination-info::text").get("")
        match = re.search(r"Page\s+\d+\s+of\s+(\d+)", text)
        if match:
            return int(match.group(1))
        return 1

    # EVENT DETAIL PAGE
    def parse(self, response):

        raw_title = response.css("h1::text").get("").strip()
        title = self._parse_title(raw_title)

        location = self._parse_location(response)

        description = response.meta.get("description", "")

        items = response.css("li.multi-date-item")

        if not items:
            self.logger.warning(f"No dates found on {response.url}")
            return

        for item in items:
            start = self._parse_item_dt(item, "start")
            end = self._parse_item_dt(item, "end")

            if not start:
                continue

            meeting = Meeting(
                title=title,
                description=description,
                classification=NOT_CLASSIFIED,
                start=start,
                end=end,
                all_day=False,
                time_notes="",
                location=location,
                links=[{"title": "", "href": ""}],
                source=response.url,
            )

            meeting["status"] = self._get_status(meeting)
            meeting["id"] = self._get_id(meeting)

            yield meeting

    def _parse_item_dt(self, item, prefix):
        """
        Data structured in separate HTML attributes
        """
        try:
            year = int(item.attrib.get(f"data-{prefix}-year", 0))
            month = int(item.attrib.get(f"data-{prefix}-month", 0))
            day = int(item.attrib.get(f"data-{prefix}-day", 0))
            hour = int(item.attrib.get(f"data-{prefix}-hour", 0))
            mins = int(item.attrib.get(f"data-{prefix}-mins", 0))

            if not all([year, month, day]):
                return None

            return datetime(year, month, day, hour, mins)

        except (ValueError, TypeError):
            return None

    def _parse_title(self, raw):
        title = " ".join(raw.split())

        # normalize -- to -
        title = re.sub(r"\s*--\s*", " - ", title)

        return title

    def _parse_location(self, response):
        raw = " ".join(
            response.xpath(
                "//h2[contains(@class,'sub-title') and contains(text(),'Location')]"
                "/following-sibling::p[1]//text()"
            ).getall()
        ).strip()

        raw = re.sub(r"View Map", "", raw, flags=re.IGNORECASE)
        raw = raw.replace("\xa0", " ")
        raw = re.sub(r"\s+", " ", raw).strip().strip(",").strip()

        return self._split_location(raw)

    def _split_location(self, raw):
        if not raw:
            return {"name": "", "address": ""}

        parts = [p.strip() for p in raw.split(",")]

        # Check if first part looks like an address (starts with a number)
        if re.match(r"^\d+", parts[0]):
            return {"name": "", "address": raw}

        if len(parts) >= 2:
            return {
                "name": parts[0],
                "address": ", ".join(parts[1:]),
            }

        return {"name": raw, "address": ""}
