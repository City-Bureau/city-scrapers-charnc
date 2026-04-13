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
        location_section = response.xpath(
            "//h2[contains(@class,'sub-title') and contains(text(),'Location')]"
        )

        if not location_section:
            return {"name": "", "address": ""}

        all_p = location_section.xpath("following-sibling::p")
        map_p = all_p.xpath("self::p[.//a[contains(text(),'View Map')]]")

        if not map_p:
            return {"name": "", "address": ""}

        # Extract text nodes from map <p> (excludes the link text)
        raw = " ".join(map_p.xpath("text()").getall())
        raw = re.sub(r"\s+", " ", raw.replace("\xa0", " ")).strip().strip(",").strip()

        # Splitting name and address from the map <p> text
        result = self._split_name_address(raw)

        # If no name found in map <p>, fall back to preceding <p> after Location h2
        if not result["name"]:
            name_p = map_p.xpath(
                "preceding-sibling::p[not(@class)]"
                "[preceding-sibling::h2[contains(@class,'sub-title') and contains(text(),'Location')]]"  # noqa
                "[1]"
            )
            if name_p:
                name = " ".join(name_p.xpath(".//text()").getall())
                name = (
                    re.sub(r"\s+", " ", name.replace("\xa0", " "))
                    .strip()
                    .strip(",")
                    .strip()
                )
                result["name"] = name

        return result

    def _split_name_address(self, raw):
        if not raw:
            return {"name": "", "address": ""}

        parts = [p.strip() for p in raw.split(",")]

        # Find the first part that looks like a street number
        for i, part in enumerate(parts):
            if re.match(r"^\d+", part.strip()):
                name = ", ".join(parts[:i]).strip().strip(",").strip()
                address = ", ".join(parts[i:]).strip().strip(",").strip()
                return {"name": name, "address": address}

        # No address pattern found — everything is a name
        return {"name": raw, "address": ""}
