import re
from datetime import datetime
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import scrapy
from city_scrapers_core.constants import (
    ADVISORY_COMMITTEE,
    BOARD,
    COMMISSION,
    COMMITTEE,
    NOT_CLASSIFIED,
)
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider


class CharncMeckBocSpider(CityScrapersSpider):
    name = "charnc_meck_boc"
    agency = "Mecklenburg County"
    timezone = "America/New_York"
    start_urls = ["https://calendar.mecknc.gov/jsonapi/node/event"]

    legistar_url = "https://mecklenburg.legistar.com/Calendar.aspx"
    legistar_api = "https://webapi.legistar.com/v1/mecklenburg/events"
    legistar_page_size = 1000
    primary_page_size = 50
    since_year = 2022
    _tz = ZoneInfo("America/New_York")

    custom_settings = {"ROBOTSTXT_OBEY": False}

    _browser_ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    _stop_words = frozenset(
        [
            "a",
            "an",
            "and",
            "at",
            "for",
            "in",
            "meeting",
            "meetings",
            "of",
            "the",
            "to",
        ]
    )
    _clean_title_re = re.compile(
        r"^(will\s+not\s+be\s+held|postponed|canceled|cancelled|rescheduled)"
        r"(\s+due\s+to\s+\w+)?"
        r"[\s:,\-]*",
        re.IGNORECASE,
    )
    _significant_words_re = re.compile(r"[^a-z0-9\s]")
    _cancelled_patterns = re.compile(
        r"will\s+not\s+be\s+held|cancelled|canceled|postponed|rescheduled",
        re.IGNORECASE,
    )
    _location_comment_re = re.compile(
        r"(\d|room|suite|floor|ave|st\b|blvd|dr\b|rd\b|hwy|bldg|govt|government"
        r" center)",
        re.IGNORECASE,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.legistar_events = []
        self._seen_ids = set()
        self._matched_legistar_ids = set()
        self._legistar_by_date = None  # None = not built; {} = built (may be empty)

    # ------------------------------------------------------------------
    # Request chain: Legistar (paginated) → primary JSON:API (paginated)
    # ------------------------------------------------------------------

    def start_requests(self):
        """Fetch all Legistar events first so agenda/minutes links are ready."""
        yield self._legistar_request(skip=0)

    def _legistar_request(self, skip):
        params = {
            "$top": self.legistar_page_size,
            "$skip": skip,
            "$filter": f"EventDate ge datetime'{self.since_year}-01-01T00:00:00'",
        }
        return scrapy.Request(
            f"{self.legistar_api}?{urlencode(params)}",
            headers={"Accept": "application/json"},
            callback=self._parse_legistar,
            cb_kwargs={"skip": skip},
            errback=self.handle_error,
        )

    def _parse_legistar(self, response, skip):
        try:
            events = response.json()
        except ValueError:
            self.logger.warning(
                "Invalid JSON from Legistar (skip=%d): %s", skip, response.url
            )
            return

        filtered = [
            e
            for e in events
            if (s := self._parse_legistar_start(e)) and s.year >= self.since_year
        ]
        self.legistar_events.extend(filtered)
        self.logger.info(
            "Legistar page skip=%d: loaded %d events (%d after since_year filter)",
            skip,
            len(events),
            len(filtered),
        )

        if len(events) == self.legistar_page_size:
            # Full page — there may be more; fetch the next page.
            yield self._legistar_request(skip=skip + self.legistar_page_size)
        else:
            # Partial (or empty) page — Legistar load is complete.
            self.logger.info(
                "Legistar load complete: %d total events", len(self.legistar_events)
            )
            url = (
                f"{self.start_urls[0]}"
                f"?page%5Boffset%5D=0&page%5Blimit%5D={self.primary_page_size}"
            )
            yield scrapy.Request(
                url,
                headers={
                    "User-Agent": self._browser_ua,
                    "Accept": "application/vnd.api+json",
                },
                callback=self.parse,
                errback=self.handle_error,
                meta={"handle_httpstatus_list": [403]},
            )

    def parse(self, response):
        if response.status == 403:
            self.logger.warning(
                "Calendar blocked (403): %s — falling back to Legistar-only",
                response.url,
            )
            yield from self._yield_unmatched_legistar()
            return

        try:
            data = response.json()
        except ValueError:
            self.logger.warning("Invalid JSON from primary source: %s", response.url)
            return

        for event in data.get("data", []):
            attrs = event.get("attributes", {})
            raw_title = attrs.get("title", "")
            title = self._clean_title(raw_title)
            if not title or self._is_non_meeting(title):
                continue
            start = self._parse_dt(attrs, "value")
            if not start:
                continue
            end = self._parse_dt(attrs, "end_value")
            all_day = self._is_all_day(start, end)
            links = self._match_legistar_links(title, start)
            # If start time is midnight (parsing artifact), try real time from Legistar
            if start and start.time() == datetime.min.time() and not all_day:
                legistar_event = self._find_matching_legistar_event(title, start.date())
                if legistar_event:
                    start = self._parse_legistar_start(legistar_event)
            meeting = Meeting(
                title=title,
                description="",
                classification=self._parse_classification(title),
                start=start,
                end=end,
                all_day=all_day,
                time_notes="",
                location=self._parse_location(attrs),
                links=links,
                source=attrs.get("absolute_url") or self.legistar_url,
            )
            # Pass "cancelled" hint so _get_status() returns CANCELLED when the
            # raw title contained a cancellation phrase that was stripped for display.
            cancel_text = (
                "cancelled" if self._cancelled_patterns.search(raw_title) else ""
            )
            meeting["status"] = self._get_status(meeting, text=cancel_text)
            meeting["id"] = self._get_id(meeting)
            if meeting["id"] in self._seen_ids:
                continue
            self._seen_ids.add(meeting["id"])
            yield meeting

        next_href = (data.get("links") or {}).get("next", {})
        if isinstance(next_href, dict):
            next_href = next_href.get("href")
        if next_href:
            yield scrapy.Request(
                next_href,
                headers={
                    "User-Agent": self._browser_ua,
                    "Accept": "application/vnd.api+json",
                },
                callback=self.parse,
                errback=self.handle_error,
                meta={"handle_httpstatus_list": [403]},
            )
        else:
            # Last calendar page — yield Legistar events not matched to any
            # calendar event (e.g. meetings only listed on Legistar).
            yield from self._yield_unmatched_legistar()

    def _yield_unmatched_legistar(self):
        for legistar_event in self.legistar_events:
            if legistar_event.get("EventId") in self._matched_legistar_ids:
                continue
            meeting = self._legistar_to_meeting(legistar_event)
            if meeting and meeting["id"] not in self._seen_ids:
                self._seen_ids.add(meeting["id"])
                yield meeting

    def handle_error(self, failure):
        self.logger.error(
            "Request failed: %s — %s", failure.request.url, failure.getErrorMessage()
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _clean_title(self, title):
        """Strip whitespace and remove leading cancellation prefixes."""
        return self._clean_title_re.sub("", title.strip()).strip()

    def _is_non_meeting(self, title):
        """Return True for non-meeting events such as office closure notices."""
        return "government offices closed" in title.lower()

    def _parse_dt(self, attrs, key):
        """Parse UTC ISO datetime from field_event_datetime; return naive ET datetime.

        Start and end must be timezone-naive per project schema requirements.
        Conversion: UTC ISO string → America/New_York → strip tzinfo.
        """
        dt_list = attrs.get("field_event_datetime") or []
        if not dt_list or not dt_list[0].get(key):
            return None
        try:
            dt_utc = datetime.fromisoformat(dt_list[0][key])
            return dt_utc.astimezone(self._tz).replace(tzinfo=None)
        except (ValueError, TypeError):
            self.logger.warning(
                "Could not parse datetime field %r: %r", key, dt_list[0].get(key)
            )
            return None

    def _parse_location(self, attrs):
        addr = attrs.get("field_event_address") or {}
        if not isinstance(addr, dict):
            name = str(addr).strip()
        else:
            line1 = (addr.get("address_line1") or "").strip()
            line2 = (addr.get("address_line2") or "").strip()
            city = (addr.get("locality") or "").strip()
            state = (addr.get("administrative_area") or "").strip()
            postal = (addr.get("postal_code") or "").strip()
            name_parts = [p for p in [line1, line2] if p]
            city_parts = [p for p in [city, f"{state} {postal}".strip()] if p]
            if city_parts:
                name_parts.append(", ".join(city_parts))
            name = ", ".join(name_parts)
        return {"name": self._normalize_location_name(name), "address": ""}

    def _normalize_location_name(self, name):
        """Normalize location names by fixing typos and standardizing formats."""
        if not name:
            return name
        name = name.replace("Freedom Dive", "Freedom Drive")
        name = name.replace("600v E. 4th St", "600 E. 4th St")
        name = name.replace("600 E. 4st", "600 E. 4th St")
        name = re.sub(r",\s*Charlotte\s*,\s*Charlotte", ", Charlotte", name)
        name = re.sub(r",\s*NC\s*$", ", NC", name)
        return name.strip()

    def _clean_location_name(self, name):
        """Strip editorial phrases and normalize line breaks in location names."""
        if not name:
            return name
        # Replace line breaks with a comma separator, then collapse any resulting
        # double-commas or trailing commas left by a trailing newline in the source.
        name = re.sub(r"\r?\n", ", ", name)
        name = re.sub(r",\s*,", ",", name)
        name = name.strip(" ,")
        for pattern in [r"\bREVISED AGENDA\b", r"\bREVISED\b", r"\bin-person\b"]:
            name = re.sub(pattern, "", name, flags=re.IGNORECASE)
        return self._normalize_location_name(name.strip())

    def _is_all_day(self, start, end):
        """Return True if the event spans a full day (00:00 to 23:59)."""
        if not start or not end:
            return False
        return start.time() == datetime.min.time() and (
            end.time().hour == 23 and end.time().minute == 59
        )

    def _find_matching_legistar_event(self, title, date):
        """Return the first Legistar event fuzzy-matching title on date, or None.

        Requires ≥2 significant words in common, or that all words in the Legistar
        body name appear in the primary title. This prevents single-word matches
        (e.g. "board") from cross-matching unrelated bodies on the same date.

        Uses a date-keyed index (built on first call) for O(1) date lookup instead
        of scanning all Legistar events on every calendar item.
        """
        if self._legistar_by_date is None:
            self._legistar_by_date = {}
            for event in self.legistar_events:
                s = self._parse_legistar_start(event)
                if s:
                    self._legistar_by_date.setdefault(s.date(), []).append(event)

        title_words = self._significant_words(title)
        for event in self._legistar_by_date.get(date, []):
            legistar_words = self._significant_words(event.get("EventBodyName", ""))
            overlap = title_words & legistar_words
            if len(overlap) >= 2 or (legistar_words and legistar_words <= title_words):
                return event
        return None

    def _match_legistar_links(self, title, start):
        """Fuzzy-match title + date against Legistar to pull agenda/minutes links."""
        legistar_event = self._find_matching_legistar_event(title, start.date())
        if legistar_event:
            self._matched_legistar_ids.add(legistar_event.get("EventId"))
            return self._legistar_links(legistar_event)
        return []

    def _significant_words(self, text):
        words = set(self._significant_words_re.sub(" ", text.lower()).split())
        return words - self._stop_words

    def _parse_legistar_start(self, event):
        date_str = event.get("EventDate", "")
        time_str = event.get("EventTime", "")
        if not date_str:
            return None
        try:
            date_part = date_str[:10]
            if time_str:
                return datetime.strptime(f"{date_part} {time_str}", "%Y-%m-%d %I:%M %p")
            return datetime.strptime(date_part, "%Y-%m-%d")
        except ValueError:
            self.logger.warning(
                "Could not parse Legistar event date %r time %r", date_str, time_str
            )
            return None

    def _legistar_links(self, event):
        links = []
        if event.get("EventAgendaFile"):
            links.append({"href": event["EventAgendaFile"], "title": "Agenda"})
        if event.get("EventMinutesFile"):
            links.append({"href": event["EventMinutesFile"], "title": "Minutes"})
        if event.get("EventVideoPath"):
            links.append({"href": event["EventVideoPath"], "title": "Video"})
        return links

    def _legistar_to_meeting(self, event):
        """Build a Meeting from a Legistar API event dict."""
        raw_title = event.get("EventBodyName", "")
        title = self._clean_title(raw_title)
        if not title or self._is_non_meeting(title):
            return None
        start = self._parse_legistar_start(event)
        if not start:
            return None
        location_name = (event.get("EventLocation") or "").strip()
        if not location_name:
            # Location is sometimes the first line of EventComment
            comment = (event.get("EventComment") or "").strip()
            first_line = comment.splitlines()[0].strip() if comment else ""
            # Only use if it looks like an address or room reference
            if first_line and self._location_comment_re.search(first_line):
                location_name = first_line
        location_name = self._clean_location_name(location_name)
        meeting = Meeting(
            title=title,
            description=(event.get("EventComment") or "").strip(),
            classification=self._parse_classification(title),
            start=start,
            end=None,
            all_day=False,
            time_notes="",
            location={"name": location_name, "address": ""},
            links=self._legistar_links(event),
            source=event.get("EventInSiteURL") or self.legistar_url,
        )
        cancel_text = "cancelled" if self._cancelled_patterns.search(raw_title) else ""
        meeting["status"] = self._get_status(meeting, text=cancel_text)
        meeting["id"] = self._get_id(meeting)
        return meeting

    def _parse_classification(self, title):
        title_lower = title.lower()
        if "advisory" in title_lower:
            return ADVISORY_COMMITTEE
        if "board" in title_lower or title_lower.startswith("bocc"):
            return BOARD
        if "commission" in title_lower:
            return COMMISSION
        if "committee" in title_lower or "policy" in title_lower:
            return COMMITTEE
        return NOT_CLASSIFIED
