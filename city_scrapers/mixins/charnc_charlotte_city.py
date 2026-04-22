"""
Mixin and metaclass template for Charlotte City spiders that share a common data
source.

Required class variables on child spiders:
    name (str): Spider name/slug
    agency (str): Full agency name
    category_label (str): Event category label used to filter the primary site
    classification (str): Meeting classification constant from city_scrapers_core
    legistar_bodies (list of str): Legistar body names to match when collecting
        past meetings
"""

import re
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from html import unescape
from urllib.parse import quote
from zoneinfo import ZoneInfo

import scrapy
from city_scrapers_core.constants import CANCELLED, NOT_CLASSIFIED
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import LegistarSpider
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
                "name",
                "agency",
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


class CharncCharlotteCitySpiderMixin(LegistarSpider, metaclass=CharlotteCityMixinMeta):
    timezone = "America/New_York"
    upcoming_meetings_url = (
        "https://www.charlottenc.gov/City-Government/Council-Meetings/Upcoming-Meetings"
    )
    past_meetings_url = "https://charlottenc.legistar.com/Calendar.aspx"
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "FEED_EXPORT_ENCODING": "utf-8",
    }
    default_legistar_address = "600 East 4th Street, Charlotte, NC 28202"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.since_year = 2023
        self._legistar_by_start = defaultdict(list)
        self._pending_details = 0
        self._primary_pages_done = False
        self._unmatched_yielded = False
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

    def _localize_dt(self, dt):
        if not dt:
            return None

        tz = ZoneInfo(self.timezone)

        if dt.tzinfo is None:
            return dt.replace(tzinfo=tz)

        return dt.astimezone(tz)

    def _is_within_scrape_range(self, dt):
        if not dt:
            return False
        cutoff = self._localize_dt(datetime(self.since_year, 1, 1))
        return self._localize_dt(dt) >= cutoff

    def start_requests(self):
        yield scrapy.Request(
            url=self.past_meetings_url,
            callback=self.parse,
        )

    def parse(self, response):
        current_year = datetime.now().year
        self._pending_legistar_years = len(range(self.since_year, current_year + 1))
        yield from super().parse(response)

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
                "source": response.urljoin(detail_url),
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
        # Cap at 50 pages to guard against infinite loops if the site ever
        # repeats its last page instead of returning an empty terminating page.
        if response.css("div.list-item-container") and current_page < 50:
            yield scrapy.Request(
                next_url, callback=self.parse_primary, meta={"pageindex": next_page}
            )
        else:
            self._primary_pages_done = True
            yield from self._maybe_yield_unmatched()

    def parse_primary_detail(self, response):
        try:
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
        finally:
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

    def parse_legistar(self, events):
        for item in events:
            body = self._get_legistar_body(item)

            if not self._legistar_matches_body(body):
                continue

            start = self._localize_dt(self.legistar_start(item))

            if not start or not self._is_within_scrape_range(start):
                continue

            location_text = self._get_legistar_location(item)
            links = self._dedupe_links(self.legistar_links(item))
            source = self.past_meetings_url

            self._legistar_by_start[start].append(
                {
                    "title": body,
                    "location": {
                        "name": location_text,
                        "address": self.default_legistar_address,
                    },
                    "links": links,
                    "source": source,
                }
            )

    def _parse_legistar_events(self, response):
        events_tables = response.css("table.rgMasterTable")
        if not events_tables:
            return []

        events_table = events_tables[0]

        headers = []
        for header in events_table.css("th[class^='rgHeader']"):
            header_text = (
                " ".join(header.css("*::text").extract()).replace("&nbsp;", " ").strip()
            )
            header_inputs = header.css("input")
            if header_text:
                headers.append(header_text)
            elif len(header_inputs) > 0:
                headers.append(header_inputs[0].attrib["value"])
            else:
                imgs = header.css("img")
                headers.append(imgs[0].attrib.get("alt", "") if imgs else "")

        events = []
        for row in events_table.css("tr.rgRow, tr.rgAltRow"):
            try:
                data = defaultdict(lambda: None)
                for header, field in zip(headers, row.css("td")):
                    field_text = (
                        " ".join(field.css("*::text").extract())
                        .replace("&nbsp;", " ")
                        .strip()
                    )
                    url = None
                    if len(field.css("a")) > 0:
                        link_el = field.css("a")[0]
                        onclick = link_el.attrib.get("onclick", "").strip()
                        if onclick and onclick.startswith(
                            ("radopen('", "window.open", "OpenTelerikWindow")
                        ):
                            url = response.urljoin(onclick.split("'")[1])
                        elif "href" in link_el.attrib:
                            url = response.urljoin(link_el.attrib["href"])
                    if url:
                        if header in ["", "ics"] and "View.ashx?M=IC" in url:
                            header = "iCalendar"
                            value = {"url": url}
                        else:
                            value = {"label": field_text, "url": url}
                    else:
                        value = field_text

                    data[header] = value

                ical_url = data.get("iCalendar", {}).get("url")
                if ical_url:
                    if ical_url in self._scraped_urls:
                        continue
                    self._scraped_urls.add(ical_url)

                events.append(dict(data))
            except Exception as e:
                self.logger.warning(
                    "Failed to parse Legistar row for %s: %s",
                    response.url,
                    e,
                )

        return events

    def _get_legistar_body(self, item):
        for key in ["Name", "Meeting Details", "Body", "Title"]:
            value = item.get(key)
            if isinstance(value, dict):
                label = value.get("label", "").strip()
                if label:
                    return label
            elif isinstance(value, str):
                value = value.strip()
                if value:
                    return value
        return ""

    def _get_legistar_location(self, item):
        for key in ["Meeting Location", "Location"]:
            value = item.get(key)
            if isinstance(value, dict):
                return value.get("label", "").strip()
            if isinstance(value, str):
                return value.strip()
        return ""

    def _parse_legistar_events_page(self, response):
        legistar_events = self._parse_legistar_events(response)
        self.parse_legistar(legistar_events)

        next_requests = list(self._parse_next_page(response))
        if next_requests:
            yield from next_requests
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
                    start=start.replace(tzinfo=None),
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
        categories = item.css("p.tagged-as-list span.text::text").getall()
        return " ".join(c.strip() for c in categories if c.strip())

    def _clean_text(self, text):
        if not text:
            return ""
        text = unescape(text)
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
        seen = set()
        occurrences = []

        for item in response.css("li.multi-date-item"):
            start = self._parse_item_dt(item, "start")
            end = self._parse_item_dt(item, "end")

            if not start or not self._is_within_scrape_range(start):
                continue

            key = (start, end)
            if key not in seen:
                seen.add(key)
                occurrences.append({"start": start, "end": end})

        return occurrences

    def _parse_item_dt(self, item, prefix):
        """
        Extract a datetime from data-{prefix}-year/month/day attributes
        and inline time text (e.g. "06:00 PM - 07:30 PM").
        """
        try:
            year = int(item.attrib.get(f"data-{prefix}-year", 0))
            month = int(item.attrib.get(f"data-{prefix}-month", 0))
            day = int(item.attrib.get(f"data-{prefix}-day", 0))

            if not all([year, month, day]):
                return None

            text = " ".join(item.css("::text").getall())
            times = re.findall(r"\d{1,2}:\d{2}\s*[AP]M", text, re.I)

            idx = 0 if prefix == "start" else 1
            if len(times) > idx:
                t = datetime.strptime(times[idx].strip(), "%I:%M %p")
                return datetime(year, month, day, t.hour, t.minute)

            return datetime(year, month, day)

        except (ValueError, TypeError) as e:
            self.logger.warning(
                "Failed to parse datetime from item attributes: %s",
                e,
                extra={"item_attributes": item.attrib},
            )
            return None

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

    LEGISTAR_MATCH_WINDOW_MINUTES = 60
    LEGISTAR_TITLE_SIMILARITY_THRESHOLD = 0.6

    # Accept a title match only if the best candidate scores at least this value.
    # The score is not raw SequenceMatcher alone: _title_similarity() returns the
    # stronger of (1) normalized word overlap and (2) SequenceMatcher ratio, after
    # stripping generic meeting words like "city", "council", and "meeting".
    #
    # We keep this threshold near 0.6 because difflib treats ~0.6 as a reasonable
    # "close match" rule of thumb, and because title matching here is already gated
    # by a ±60 minute start-time window. In practice this lets us match small
    # wording differences between the primary site and Legistar without requiring
    # exact title equality, while still rejecting weak same-day candidates.

    def _find_legistar_match(self, primary_title, start_dt):
        """Return best Legistar entry matching title and start time, or None."""
        best = None
        best_score = 0
        start_dt = self._localize_dt(start_dt)

        for key, entries in self._legistar_by_start.items():
            key = self._localize_dt(key)
            # abs() gets the absolute value of the time difference, ensuring we compare
            # the magnitude of time difference regardless of which datetime is earlier
            if (
                abs((key - start_dt).total_seconds())
                > self.LEGISTAR_MATCH_WINDOW_MINUTES * 60
            ):  # noqa
                continue
            for candidate in entries:
                score = self._title_similarity(primary_title, candidate["title"])
                if score > best_score:
                    best = candidate
                    best_score = score

        if best_score >= self.LEGISTAR_TITLE_SIMILARITY_THRESHOLD:
            best["_matched"] = True
            return best
        return None

    def _title_similarity(self, left, right):
        """
        Score title similarity on a 0.0-1.0 scale.

        Matching strategy:
        - 1.0 for exact equality after normalization
        - 0.9 when one normalized title contains the other
        - otherwise use the higher of:
            * token overlap ratio
            * SequenceMatcher character-level ratio

        This hybrid approach handles both wording changes
        ("Budget Meeting" vs "Budget Committee Meeting")
        and small string variations/abbreviations.
        """
        left_norm = self._normalize_meeting_title(left)
        right_norm = self._normalize_meeting_title(right)

        if not left_norm or not right_norm:
            return 0

        if left_norm == right_norm:
            return 1.0

        # One title fully contains the other, e.g. "Budget Committee" inside
        # "Special Budget Committee Meeting".
        if left_norm in right_norm or right_norm in left_norm:
            return 0.9

        left_tokens = set(left_norm.split())
        right_tokens = set(right_norm.split())

        if not left_tokens or not right_tokens:
            return 0

        # Word overlap: shared words / total unique words in the longer title.
        overlap = len(left_tokens & right_tokens) / max(
            len(left_tokens), len(right_tokens)
        )
        # SequenceMatcher catches structural similarity that word overlap misses,
        # e.g. "Zoning Mtg" vs "Zoning Meeting" where tokens differ but strings
        # are close character-by-character.
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

    def _safe_parse_datetime(self, raw):
        if not raw:
            return None

        try:
            dt = dateparser(raw, fuzzy=True)
            return dt.replace(tzinfo=None)
        except Exception as e:
            self.logger.warning(
                "Failed to parse datetime from raw string: %s",
                e,
                extra={"raw_string": raw},
            )
            return None

    def _get_status(self, meeting):
        location = meeting.get("location", {})
        location_text = ""

        if isinstance(location, dict):
            location_text = f"{location.get('name', '')} {location.get('address', '')}"
        elif isinstance(location, str):
            location_text = location

        if location_text and "canceled" in location_text.lower():
            return CANCELLED

        start = meeting.get("start")
        if start and start.tzinfo is not None:
            meeting = {**meeting, "start": start.replace(tzinfo=None)}

        return super()._get_status(meeting)

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
