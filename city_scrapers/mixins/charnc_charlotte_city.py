"""
Mixin and metaclass template for Charlotte City spiders that share a common data
source.

Required class variables on child spiders:
    name (str): Spider name/slug
    agency (str): Full agency name
    id (str): Unique identifier for the spider
"""

import re
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from html import unescape
from urllib.parse import parse_qs, quote, urlencode

import scrapy
from city_scrapers_core.constants import CANCELLED, NOT_CLASSIFIED
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider
from dateutil.parser import parse as dateparser


class CharlotteCityMixinMeta(type):
    """
    Metaclass that enforces required static variables on child spiders.
    """

    def __init__(cls, name, bases, dct):
        if name == "CharncCharlotteCitySpiderMixin":
            super().__init__(name, bases, dct)
            return

        if any(
            getattr(base, "__name__", "") == "CharncCharlotteCitySpiderMixin"
            for base in bases
        ):
            required_static_vars = [
                "agency",
                "name",
                "category_label",
                "classification",
                "legistar_bodies",
            ]
            missing_vars = [var for var in required_static_vars if var not in dct]

            if missing_vars:
                missing_vars_str = ", ".join(missing_vars)
                raise NotImplementedError(
                    f"{name} must define the following static variable(s): "
                    f"{missing_vars_str}."
                )

        super().__init__(name, bases, dct)


class CharncCharlotteCitySpiderMixin(
    CityScrapersSpider, metaclass=CharlotteCityMixinMeta
):
    timezone = "America/New_York"
    upcoming_meetings_url = (
        "https://www.charlottenc.gov/City-Government/Council-Meetings/Upcoming-Meetings"
    )
    past_meetings_url = "https://charlottenc.legistar.com/Calendar.aspx"
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
    }
    since_year = 2023

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._legistar_by_start = defaultdict(list)
        self._pending_details = 0
        self._primary_pages_done = False
        self._unmatched_yielded = False
        self._scraped_urls = set()
        self._pending_legistar_years = 0
        self._primary_started = False

    def _build_primary_url(self, pageindex=1):
        category = quote(self.category_label)

        filters = f"(dd_OC%20Event%20Categories={category})"

        if pageindex > 1:
            filters += f"(pageindex={pageindex})"

        return (
            "https://www.charlottenc.gov/City-Government/Council-Meetings/"
            f"Upcoming-Meetings?dlv_City%20Council%20Events%20Listing={filters}"
        )

    def _is_within_scrape_range(self, dt):
        """Return True if dt is on or after Jan 1 of since_year."""
        if not dt:
            return False
        cutoff = datetime(self.since_year, 1, 1)
        return dt.replace(tzinfo=None) >= cutoff

    def start_requests(self):
        yield scrapy.Request(
            url=self.past_meetings_url,
            callback=self.parse_legistar_years,
        )

    def parse_legistar_years(self, response):
        """
        Start Legistar year dropdown POST requests for each year from since_year
        through the current year.
        """
        secrets = self._parse_legistar_secrets(response)
        current_year = datetime.now().year
        years = list(range(self.since_year, current_year + 1))
        self._pending_legistar_years = len(years)

        for year in years:
            yield scrapy.Request(
                response.url,
                method="POST",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                body=urlencode(
                    {
                        **secrets,
                        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$lstYears",
                        "ctl00_ContentPlaceHolder1_lstYears_ClientState": (
                            f'{{"value":"{year}"}}'
                        ),
                    }
                ),
                callback=self.parse_legistar,
                meta={"legistar_year": year},
                dont_filter=True,
            )

    def _parse_legistar_secrets(self, response):
        """
        Extract ASP.NET hidden form values needed for Legistar postbacks.
        """
        secrets = {
            "__EVENTARGUMENT": None,
            "__VIEWSTATE": response.css("[name='__VIEWSTATE']::attr(value)").get(),
        }
        event_validation = response.css("[name='__EVENTVALIDATION']::attr(value)").get()
        if event_validation:
            secrets["__EVENTVALIDATION"] = event_validation
        return secrets

    def _parse_legistar_rows(self, response):
        """
        Parse the Legistar table rows from the current response and store matched
        meetings in self._legistar_by_start.
        """
        rows = response.css(
            "table.rgMasterTable > tbody > tr.rgRow, "
            "table.rgMasterTable > tbody > tr.rgAltRow"
        )

        for item in rows:
            body = " ".join(
                part.strip()
                for part in item.css("td:nth-child(1) a *::text").getall()
                if part.strip()
            )

            if not self._legistar_matches_body(body):
                continue

            date = item.css("td:nth-child(2) *::text").get(default="").strip()
            time = " ".join(
                part.strip()
                for part in item.css("td:nth-child(4) *::text").getall()
                if part.strip()
            )
            raw = f"{date} {time}" if time else date

            start = self._safe_parse_datetime(raw)
            if not start or not self._is_within_scrape_range(start):
                continue

            location = " ".join(
                part.strip()
                for part in item.css("td:nth-child(5) *::text").getall()
                if part.strip()
            )

            links = []

            def add_link(selector, title):
                href = item.css(selector + "::attr(href)").get()
                if href:
                    links.append({"href": response.urljoin(href), "title": title})

            add_link("td:nth-child(7) a", "Agenda")

            minutes = item.css("td:nth-child(8) a::attr(href)").get()
            if minutes:
                links.append({"href": response.urljoin(minutes), "title": "Minutes"})

            video_js = item.css("td:nth-child(9) a::attr(onclick)").get()
            video_url = self._extract_js_url(video_js)
            if video_url:
                links.append({"href": response.urljoin(video_url), "title": "Video"})

            ical_url = item.css("td a[href*='View.ashx?M=IC']::attr(href)").get()
            if ical_url:
                ical_url = response.urljoin(ical_url)
                if ical_url in self._scraped_urls:
                    continue
                self._scraped_urls.add(ical_url)
            else:
                dedupe_key = f"{self._normalize(body)}|{start.isoformat()}"
                if dedupe_key in self._scraped_urls:
                    continue
                self._scraped_urls.add(dedupe_key)

            self._legistar_by_start[start].append(
                {
                    "title": body,
                    "location": {
                        "name": location,
                        "address": "600 East 4th Street, Charlotte, NC 28202",
                    },
                    "links": self._dedupe_links(links),
                    "source": response.url,
                }
            )

    def _parse_legistar_next_page(self, response):
        """
        Follow Legistar pagination for the currently selected year using ASP.NET
        postback payload.
        """
        next_page_link = response.css("a.rgCurrentPage + a")
        if len(next_page_link) == 0:
            return

        event_target = next_page_link.css("::attr(href)").re_first(r"'([^']+)'")
        request_body = response.request.body.decode("utf-8")
        next_page_payload = {
            **parse_qs(request_body),
            **self._parse_legistar_secrets(response),
            "__EVENTTARGET": event_target,
        }

        yield scrapy.Request(
            response.url,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urlencode(next_page_payload, doseq=True),
            callback=self.parse_legistar,
            meta=response.meta,
            dont_filter=True,
        )

    def _start_primary_if_ready(self):
        """
        Start the primary scraper only after all Legistar year/page work is done.
        """
        if self._pending_legistar_years == 0 and not self._primary_started:
            self._primary_started = True
            return scrapy.Request(
                url=self._build_primary_url(),
                callback=self.parse_primary,
                meta={"pageindex": 1},
            )
        return None

    def parse_primary(self, response):
        """
        Parse the primary meetings page.
        """
        for item in response.css("div.list-item-container"):
            page_category = self._parse_primary_category(item)

            if not self._primary_matches_category(page_category):
                continue
            detail_url = item.css("article > a::attr(href)").get()
            if not detail_url:
                continue
            is_cancelled = bool(item.css("h3.list-item-title span.canceled-tag"))
            summary = {
                "source": self._normalize_special_chars(response.urljoin(detail_url)),
                "is_cancelled": is_cancelled,
                "description": self._parse_primary_description(item),
            }

            self._pending_details += 1
            yield response.follow(
                detail_url,
                callback=self.parse_primary_detail,
                meta={"summary": summary},
            )

        current_page = response.meta.get("pageindex", 1)
        next_page = current_page + 1
        next_url = self._build_primary_url(pageindex=next_page)
        if response.css("div.list-item-container"):
            yield scrapy.Request(
                next_url, callback=self.parse_primary, meta={"pageindex": next_page}
            )
        else:
            self._primary_pages_done = True
            yield from self._maybe_yield_unmatched()

    def parse_primary_detail(self, response):
        summary = response.meta["summary"]

        detail_title = self._parse_detail_title(response)
        detail_location = self._parse_detail_location(response)
        time_notes = self._parse_primary_time_notes(response)

        occurrences = self._parse_primary_detail_occurrences(response)

        if not occurrences:
            fallback_start = self._parse_primary_detail_start(response)
            if fallback_start:
                occurrences = [{"start": fallback_start, "end": None}]

        for occ in occurrences:
            start_dt = occ["start"]
            end_dt = occ["end"]

            legistar_match = self._find_legistar_match(detail_title, start_dt)
            links = legistar_match["links"] if legistar_match else []

            meeting = Meeting(
                title=detail_title,
                description=summary["description"],
                classification=self._parse_classification(None),
                start=start_dt,
                end=end_dt,
                all_day=False,
                time_notes=time_notes,
                location=detail_location,
                links=links,
                source=summary["source"],
            )

            if summary.get("is_cancelled"):
                meeting["status"] = CANCELLED
            else:
                meeting["status"] = self._get_status(meeting)

            meeting["id"] = self._get_id(meeting)
            yield meeting

        self._pending_details -= 1
        yield from self._maybe_yield_unmatched()

    def _maybe_yield_unmatched(self):
        """Yield unmatched Legistar meetings only when all primary work is done."""
        if (
            self._primary_pages_done
            and self._pending_details == 0
            and not self._unmatched_yielded
        ):
            self._unmatched_yielded = True
            yield from self._yield_unmatched_legistar()

    def parse_legistar(self, response):
        """
        Parse one Legistar year page, follow pagination, and only start the primary
        source after all Legistar years are finished.
        """
        self._parse_legistar_rows(response)

        next_page_link = response.css("a.rgCurrentPage + a")
        if len(next_page_link) > 0:
            yield from self._parse_legistar_next_page(response)
            return

        self._pending_legistar_years -= 1

        primary_request = self._start_primary_if_ready()
        if primary_request:
            yield primary_request

    def _yield_unmatched_legistar(self):
        """Yield Legistar meetings that had no primary match."""
        for start, candidates in self._legistar_by_start.items():
            for entry in candidates:
                if entry.get("_matched"):
                    continue

                meeting = Meeting(
                    title=entry["title"],
                    description="",
                    classification=self._parse_classification(None),
                    start=start,
                    end=None,
                    all_day=False,
                    time_notes="",
                    location=entry["location"],
                    links=entry["links"],
                    source=entry["source"],
                )
                meeting["status"] = self._get_status(meeting)
                meeting["id"] = self._get_id(meeting)
                yield meeting

    def _parse_primary_description(self, item):
        raw = " ".join(
            part.strip()
            for part in item.css("span.list-item-block-desc *::text").getall()
            if part.strip()
        )
        return self._clean_text(raw)

    def _parse_primary_category(self, item):
        return item.css("p.tagged-as-list span.text::text").get(default="").strip()

    def _normalize_special_chars(self, text):
        if not text:
            return ""
        replacements = {
            "\u2019": "'",  # right single quotation mark
            "\u2018": "'",  # left single quotation mark
            "\u201c": '"',  # left double quotation mark
            "\u201d": '"',  # right double quotation mark
            "\u2013": "-",  # en dash
            "\u2014": "-",  # em dash
            "\u00a0": " ",  # non-breaking space
            "\u00ae": "",  # registered trademark
            "\u200b": "",  # zero-width space
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    def _clean_text(self, text):
        if not text:
            return ""
        text = unescape(text)
        text = self._normalize_special_chars(text)
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\s+,", ",", text)
        text = re.sub(r",\s+", ", ", text)
        return text.strip().rstrip(",")

    def _parse_location_block(self, wrapper):
        if not wrapper:
            return {
                "name": "",
                "address": "",
            }

        html = wrapper.get()
        if not html:
            return {
                "name": "",
                "address": "",
            }

        # Try <br>-based splitting first
        html_replaced = re.sub(r"<br\s*/?>", "|||", html, flags=re.I)
        text = re.sub(r"<[^>]+>", "", html_replaced)

        parts = [self._clean_text(p) for p in text.split("|||") if self._clean_text(p)]
        parts = [p for p in parts if p.lower() != "view map"]

        if len(parts) > 1:
            return {
                "name": parts[0],
                "address": ", ".join(parts[1:]),
            }

        # Fallback: single text blob, split at first street number
        full_text = self._clean_text(parts[0]) if parts else ""
        if not full_text:
            return {
                "name": "",
                "address": "",
            }

        match = re.search(r",\s*(\d+\s+\w)", full_text)
        if match:
            name = self._clean_text(full_text[: match.start()])
            address = self._clean_text(full_text[match.start() :].lstrip(", "))
            return {
                "name": name,
                "address": address,
            }

        return {
            "name": "",
            "address": full_text,
        }

    def _primary_matches_category(self, page_category):
        return self._normalize(self.category_label) in self._normalize(page_category)

    def _parse_primary_detail_occurrences(self, response):
        """
        Parse all upcoming dates from the detail page.

        Dates live in <li class="multi-date-item"> elements inside
        both ul.future-events-list and ul.past-events-list.
        Each <li> carries data-start-year/month/day attributes and
        has text like "Monday, April 27, 2026 | 05:00 PM - 09:00 PM".

        Return:
            [
                {"start": datetime(...), "end": datetime(...)},
                ...
            ]
        """
        occurrences = []

        for li in response.css("ul.multi-date-list li.multi-date-item"):
            year = li.attrib.get("data-start-year", "").strip()
            month = li.attrib.get("data-start-month", "").strip()
            day = li.attrib.get("data-start-day", "").strip()

            text = " ".join(
                part.strip() for part in li.css("*::text").getall() if part.strip()
            )

            start_dt = None
            end_dt = None

            if year and month and day:
                date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                time_part = ""
                if "|" in text:
                    time_part = text.split("|", 1)[1].strip()

                if time_part and "-" in time_part:
                    start_time_str, end_time_str = [
                        part.strip() for part in time_part.split("-", 1)
                    ]
                    start_dt = self._safe_parse_datetime(f"{date_str} {start_time_str}")
                    end_dt = self._safe_parse_datetime(f"{date_str} {end_time_str}")

                elif time_part:
                    start_dt = self._safe_parse_datetime(f"{date_str} {time_part}")

                else:
                    start_dt = self._safe_parse_datetime(date_str)

            if not start_dt:
                start_dt = self._safe_parse_datetime(text)

            if start_dt and self._is_within_scrape_range(start_dt):
                occurrences.append(
                    {
                        "start": start_dt,
                        "end": end_dt,
                    }
                )

        seen = set()
        deduped = []
        for occ in occurrences:
            key = (
                occ["start"].isoformat() if occ["start"] else None,
                occ["end"].isoformat() if occ["end"] else None,
            )
            if key not in seen:
                seen.add(key)
                deduped.append(occ)

        return deduped

    def _parse_primary_detail_start(self, response):
        """
        Fallback: try the block-date shown on the detail page header.
        """
        raw = " ".join(
            part.strip()
            for part in response.css(
                ".list-item-block-date *::text, "
                ".event-date *::text, "
                ".event-time *::text"
            ).getall()
            if part.strip()
        )
        return self._safe_parse_datetime(raw)

    def _parse_detail_title(self, response):
        title = response.css("h1.oc-page-title::text").get(default="").strip()
        if not title:
            title = " ".join(
                part.strip()
                for part in response.css("h1::text").getall()
                if part.strip()
            )
        return self._clean_text(title)

    def _parse_detail_location(self, response):
        wrapper = response.xpath(
            '//h2[contains(text(), "Location")]/following-sibling::div[1]//p'
            ' | //h2[contains(text(), "Location")]/following-sibling::p[1]'
        )
        if wrapper:
            return self._parse_location_block(wrapper)

        name = response.css(".gmap-info h2::text").get(default="").strip()
        address = self._clean_text(
            " ".join(
                part.strip()
                for part in response.css(".gmap-info p *::text").getall()
                if part.strip()
            )
        )
        if name or address:
            return {"name": name, "address": address}

        return {"name": "", "address": ""}

    def _parse_primary_time_notes(self, response):
        return " ".join(
            part.strip()
            for part in response.css(
                ".event-time-notes *::text, .time-notes *::text"
            ).getall()
            if part.strip()
        )

    def _find_legistar_match(self, primary_title, start_dt):
        candidates = self._legistar_by_start.get(start_dt, [])
        if not candidates:
            return None

        best = None
        best_score = 0

        for candidate in candidates:
            score = self._title_similarity(primary_title, candidate["title"])
            if score > best_score:
                best = candidate
                best_score = score

        if best_score >= 0.55:
            best["_matched"] = True
            return best
        return None

    def _title_similarity(self, left, right):
        left_norm = self._normalize_meeting_title(left)
        right_norm = self._normalize_meeting_title(right)

        if not left_norm or not right_norm:
            return 0

        if left_norm == right_norm:
            return 1.0

        if left_norm in right_norm or right_norm in left_norm:
            return 0.9

        left_tokens = set(left_norm.split())
        right_tokens = set(right_norm.split())

        if not left_tokens or not right_tokens:
            return 0

        overlap = len(left_tokens & right_tokens) / max(
            len(left_tokens), len(right_tokens)
        )
        seq = SequenceMatcher(None, left_norm, right_norm).ratio()

        return max(overlap, seq)

    def _normalize_meeting_title(self, text):
        text = self._normalize(text)

        stopwords = {
            "city",
            "council",
            "committee",
            "meeting",
            "meetings",
            "session",
        }

        tokens = [token for token in text.split() if token not in stopwords]
        return " ".join(tokens)

    def _legistar_matches_body(self, body):
        normalized_body = self._normalize(body)

        for allowed in self.legistar_bodies:
            if self._normalize(allowed) in normalized_body:
                return True

        return False

    def _parse_classification(self, item):
        return getattr(self, "classification", NOT_CLASSIFIED)

    def _normalize(self, text):
        if not text:
            return ""

        text = text.lower().strip()
        text = text.replace("&", "and")
        text = re.sub(r"[^\w\s]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text

    def _extract_js_url(self, js):
        if not js:
            return None
        match = re.search(r"window\.open\(\s*'(.*?)'", js, re.DOTALL)
        return match.group(1) if match else None

    def _safe_parse_datetime(self, raw):
        if not raw:
            return None

        try:
            dt = dateparser(raw, fuzzy=True)
            return dt.replace(tzinfo=None)
        except Exception:
            return None

    def _dedupe_links(self, links):
        seen = set()
        deduped = []

        for link in links:
            key = (link.get("href"), link.get("title"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(link)

        return deduped
