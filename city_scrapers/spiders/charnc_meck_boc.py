import re
from collections import defaultdict
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import scrapy
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
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import LegistarSpider


class CharncMeckBocSpider(LegistarSpider):
    name = "charnc_meck_boc"
    agency = "Mecklenburg County"
    timezone = "America/New_York"
    start_urls = ["https://mecklenburg.legistar.com/Calendar.aspx"]

    primary_url = "https://calendar.mecknc.gov"
    primary_api = (
        "https://calendar.mecknc.gov/jsonapi/node/event"
        "?page[limit]=50"
        "&sort=-field_event_datetime.value"
    )

    custom_settings = {"ROBOTSTXT_OBEY": False, "FEED_EXPORT_ENCODING": "utf-8"}

    _tz = ZoneInfo("America/New_York")
    _browser_ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    _stop_words = frozenset(
        "a an the of and or in at for to by with from as on is are was were be "
        "been being have has had do does did will would could should may might".split()
    )
    _significant_words_re = re.compile(r"[a-z]+")

    _clean_title_re = re.compile(
        r"^(will\s+not\s+be\s+held|postponed|canceled|cancelled|rescheduled)"
        r"(\s+due\s+to\s+\w+)?"
        r"[\s:,\-]*",
        re.IGNORECASE,
    )
    _street_re = re.compile(r"^\s*\d+\s+")
    _linebreak_re = re.compile(r"\r?\n")
    _dupe_comma_re = re.compile(r",\s*,")
    _editorial_re = re.compile(
        r"\bREVISED AGENDA\b|\bREVISED\b|\bin-person\b", re.IGNORECASE
    )
    _dupe_charlotte_re = re.compile(r",\s*Charlotte\s*,\s*Charlotte")
    _nc_trail_re = re.compile(r",\s*NC\s*$")
    _cancel_in_location_re = re.compile(
        r"[,\s]*\b(will\s+not\s+be\s+held|cancelled|canceled|postponed|rescheduled)"
        r"\b.*$",
        re.IGNORECASE,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.since_year = 2022
        self.legistar_events = []
        self._pending_legistar_pages = 0
        self._matched_legistar_ids = set()
        self._seen_ids = set()

    # ------------------------------------------------------------------
    # Legistar collection phase
    # ------------------------------------------------------------------

    def parse(self, response):
        """Override to count pending year pages; fire primary source when all done."""
        secrets = self._parse_secrets(response)
        current_year = datetime.now().year
        years = list(range(self.since_year, current_year + 1))
        self._pending_legistar_pages = len(years)
        if not years:
            yield from self._calendar_request()
            return
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
                callback=self._parse_legistar_events_page,
                dont_filter=True,
            )

    def _parse_legistar_events_page(self, response):
        """Collect Legistar events; fire primary source when all pages are done."""
        events = self._parse_legistar_events(response)
        self.legistar_events.extend(events)

        next_requests = list(self._parse_next_page(response))
        if next_requests:
            self._pending_legistar_pages += len(next_requests)
            yield from next_requests

        self._pending_legistar_pages -= 1
        if self._pending_legistar_pages == 0:
            yield from self._calendar_request()

    def parse_legistar(self, events):
        """Yield meetings directly from Legistar events.

        Called by tests and used as 403/error fallback.
        """
        seen_ids = set()
        for event in events:
            yield from self._yield_legistar_event(event, seen_ids)

    def _parse_legistar_events(self, response):
        """Override parent to use meeting URL as dedup key.

        LegistarSpider deduplicates via the iCalendar link URL, which
        Mecklenburg's table does not include, causing every row to be skipped.
        Use the meeting detail URL from the "Meeting Details" column instead.
        """
        tables = response.css("table.rgMasterTable")
        if not tables:
            self.logger.warning(
                "No meetings table found in Legistar response; skipping event parsing"
            )
            return []

        events_table = tables[0]

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
                headers.append(header.css("img")[0].attrib["alt"])

        events = []
        for row in response.css("tr.rgRow, tr.rgAltRow"):
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
                        if "onclick" in link_el.attrib and link_el.attrib[
                            "onclick"
                        ].startswith(("radopen('", "window.open", "OpenTelerikWindow")):
                            url = response.urljoin(
                                link_el.attrib["onclick"].split("'")[1]
                            )
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

                # "Meeting Details" URL is per-meeting; "Name" URL points to the
                # department page (same for every meeting of a given body).
                detail_val = data.get("Meeting Details")
                name_val = data.get("Name")
                ical_val = data.get("iCalendar")
                dedup_url = (
                    (detail_val.get("url") if isinstance(detail_val, dict) else None)
                    or (name_val.get("url") if isinstance(name_val, dict) else None)
                    or (ical_val.get("url") if isinstance(ical_val, dict) else None)
                )
                if dedup_url is None or dedup_url in self._scraped_urls:
                    continue
                self._scraped_urls.add(dedup_url)
                events.append(dict(data))
            except Exception:
                pass

        return events

    # ------------------------------------------------------------------
    # Primary source (calendar.mecknc.gov) phase
    # ------------------------------------------------------------------

    def _calendar_request(self):
        yield scrapy.Request(
            self.primary_api,
            headers={
                "Accept": "application/vnd.api+json",
                "User-Agent": self._browser_ua,
            },
            callback=self._parse_calendar,
            errback=self._handle_calendar_error,
        )

    def _parse_calendar(self, response):
        """Parse primary calendar source and merge with collected Legistar events."""
        if response.status != 200:
            self.logger.warning(
                "Calendar returned HTTP %s; falling back to Legistar-only",
                response.status,
            )
            yield from self.parse_legistar(self.legistar_events)
            return

        try:
            data = response.json()
        except Exception:
            self.logger.warning("Calendar response was not valid JSON; falling back")
            yield from self.parse_legistar(self.legistar_events)
            return

        links = data.get("links", {})
        next_link = links.get("next")
        has_next = bool(next_link and next_link.get("href"))
        if has_next:
            yield scrapy.Request(
                next_link["href"],
                headers={
                    "Accept": "application/vnd.api+json",
                    "User-Agent": self._browser_ua,
                },
                callback=self._parse_calendar,
                errback=self._handle_calendar_error,
            )

        for event_node in data.get("data", []):
            attrs = event_node.get("attributes", {})
            raw_title = (attrs.get("title") or "").strip()
            title = self._clean_title(raw_title)
            if not title or self._is_non_meeting(title):
                continue

            dt_list = attrs.get("field_event_datetime") or []
            if not dt_list:
                continue
            dt_entry = dt_list[0]
            start = self._parse_dt(dt_entry.get("value"))
            end = self._parse_dt(dt_entry.get("end_value"))
            if not start:
                continue

            source = attrs.get("absolute_url") or self.primary_url
            description = self._parse_description(attrs)
            location = self._parse_location(attrs)

            leg_event = self._find_matching_legistar_event(title, start)
            event_links = []
            if leg_event:
                self._matched_legistar_ids.add(id(leg_event))
                event_links = self.legistar_links(leg_event)
                leg_start = self.legistar_start(leg_event)
                if leg_start:
                    diff = abs((start - leg_start).total_seconds())
                    if 1800 <= diff <= 3600:
                        start = max(start, leg_start)

            meeting = Meeting(
                title=title,
                description=description,
                classification=self._parse_classification(title),
                start=start,
                end=end,
                all_day=False,
                time_notes="",
                location=location,
                links=event_links,
                source=source,
            )
            self._finalize_meeting(meeting, raw_title)
            if meeting["id"] in self._seen_ids:
                continue
            self._seen_ids.add(meeting["id"])
            yield meeting

        if not has_next:
            yield from self._yield_unmatched_legistar()

    def _handle_calendar_error(self, failure):
        self.logger.warning("Calendar request failed: %s", failure)
        yield from self.parse_legistar(self.legistar_events)

    def _yield_unmatched_legistar(self):
        """Yield Legistar events that had no matching calendar event."""
        for event in self.legistar_events:
            if id(event) not in self._matched_legistar_ids:
                yield from self._yield_legistar_event(event, self._seen_ids)

    # ------------------------------------------------------------------
    # Shared Legistar event helpers
    # ------------------------------------------------------------------

    def _yield_legistar_event(self, event, seen_ids):
        name_val = event.get("Name")
        raw_title = (
            name_val.get("label", "")
            if isinstance(name_val, dict)
            else str(name_val or "")
        )
        title = self._clean_title(raw_title)
        if not title or self._is_non_meeting(title):
            return
        start = self.legistar_start(event)
        if not start or start.year < self.since_year:
            return
        loc_raw = event.get("Meeting Location") or ""
        if isinstance(loc_raw, dict):
            loc_raw = loc_raw.get("label", "")
        location_str = self._clean_location_name(loc_raw.strip())
        detail = event.get("Meeting Details")
        source = (
            (detail.get("url") if isinstance(detail, dict) else None)
            or (name_val.get("url") if isinstance(name_val, dict) else None)
            or self.start_urls[0]
        )
        meeting = Meeting(
            title=title,
            description="",
            classification=self._parse_classification(title),
            start=start,
            end=None,
            all_day=False,
            time_notes="",
            location=self._split_location(location_str),
            links=self.legistar_links(event),
            source=source,
        )
        self._finalize_meeting(meeting, raw_title)
        if meeting["id"] in seen_ids:
            return
        seen_ids.add(meeting["id"])
        yield meeting

    # ------------------------------------------------------------------
    # Primary source parsing helpers
    # ------------------------------------------------------------------

    def _parse_dt(self, value):
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            return dt.astimezone(self._tz).replace(tzinfo=None)
        except (ValueError, TypeError):
            return None

    def _parse_description(self, attrs):
        if not attrs:
            return ""
        parts = []
        schedule_note = (attrs.get("field_date_time_description") or "").strip()
        if schedule_note:
            parts.append(schedule_note)
        html = (attrs.get("field_details") or {}).get("value") or ""
        if html:
            chunks = []

            class _Stripper(HTMLParser):
                def handle_data(self, data):
                    chunks.append(data)

            _Stripper().feed(html)
            body = " ".join(c.strip() for c in chunks if c.strip())
            if body:
                parts.append(body)
        return " ".join(parts)

    def _parse_location(self, attrs):
        addr = (attrs or {}).get("field_event_address") or {}
        org = (addr.get("organization") or "").strip()
        line1 = (addr.get("address_line1") or "").strip()
        line2 = (addr.get("address_line2") or "").strip()
        city = (addr.get("locality") or "").strip()
        state = (addr.get("administrative_area") or "").strip()
        postal = (addr.get("postal_code") or "").strip()
        city_str = ", ".join(p for p in [city, f"{state} {postal}".strip()] if p)
        name_parts = []
        street_parts = []
        for field in [org, line1, line2]:
            if not field:
                continue
            if self._street_re.match(field):
                street_parts.append(field)
            else:
                name_parts.append(field)
        if city_str:
            street_parts.append(city_str)
        return {
            "name": self._normalize_location_name(", ".join(name_parts)),
            "address": self._normalize_location_name(", ".join(street_parts)),
        }

    # ------------------------------------------------------------------
    # Fuzzy title matching
    # ------------------------------------------------------------------

    def _significant_words(self, title):
        words = self._significant_words_re.findall(title.lower())
        return {w for w in words if w not in self._stop_words}

    def _find_matching_legistar_event(self, calendar_title, calendar_start):
        cal_words = self._significant_words(calendar_title)
        cal_date = calendar_start.date()
        for event in self.legistar_events:
            leg_start = self.legistar_start(event)
            if not leg_start:
                continue
            if abs((leg_start.date() - cal_date).days) > 1:
                continue
            name_val = event.get("Name")
            raw_name = (
                name_val.get("label", "")
                if isinstance(name_val, dict)
                else str(name_val or "")
            )
            leg_words = self._significant_words(self._clean_title(raw_name))
            shared = cal_words & leg_words
            if len(shared) >= 2 or (leg_words and leg_words <= cal_words):
                return event
        return None

    # ------------------------------------------------------------------
    # Classification and title helpers
    # ------------------------------------------------------------------

    def _clean_title(self, title):
        return self._clean_title_re.sub("", title.strip()).strip()

    def _is_non_meeting(self, title):
        return "government offices closed" in title.lower()

    def _parse_classification(self, title):
        title_lower = title.lower()
        if "advisory" in title_lower:
            return ADVISORY_COMMITTEE
        if "board" in title_lower or "bocc" in title_lower:
            return BOARD
        if "commission" in title_lower or "district" in title_lower:
            return COMMISSION
        if (
            "committee" in title_lower
            or "policy" in title_lower
            or "council" in title_lower
        ):
            return COMMITTEE
        return NOT_CLASSIFIED

    # ------------------------------------------------------------------
    # Location helpers
    # ------------------------------------------------------------------

    def _normalize_location_name(self, name):
        if not name:
            return name
        for old, new in {
            "Freedom Dive": "Freedom Drive",
            "600v E. 4th St": "600 E. 4th St",
            "600 E. 4st": "600 E. 4th St",
            "Govenment Center": "Government Center",
        }.items():
            name = name.replace(old, new)
        name = self._dupe_charlotte_re.sub(", Charlotte", name)
        name = self._nc_trail_re.sub(", NC", name)
        return name.strip(" ,")

    def _split_location(self, name):
        if not name:
            return {"name": "", "address": ""}
        parts = [p.strip() for p in name.split(",")]
        street_idx = next(
            (i for i, p in enumerate(parts) if self._street_re.match(p)), None
        )
        if street_idx is None:
            return {"name": name, "address": ""}
        return {
            "name": ", ".join(parts[:street_idx]),
            "address": ", ".join(parts[street_idx:]),
        }

    def _clean_location_name(self, name):
        if not name:
            return name
        name = self._linebreak_re.sub(", ", name)
        name = self._dupe_comma_re.sub(",", name)
        name = name.strip(" ,")
        name = self._editorial_re.sub("", name)
        name = self._cancel_in_location_re.sub("", name)
        return self._normalize_location_name(name.strip())

    # ------------------------------------------------------------------
    # Status and ID helpers
    # ------------------------------------------------------------------

    def _finalize_meeting(self, meeting, raw_title):
        loc = meeting["location"]
        if self._clean_title_re.match(raw_title.strip()):
            meeting["status"] = CANCELLED
        else:
            status_text = " ".join(
                filter(
                    None,
                    [
                        raw_title,
                        loc.get("name"),
                        loc.get("address"),
                        meeting["time_notes"],
                    ],
                )
            )
            meeting["status"] = self._get_status(meeting, text=status_text)
        meeting["id"] = self._get_id(meeting)
        return meeting

    def _get_status(self, item, text=""):
        check = " ".join([item.get("title", ""), text]).lower()
        if any(w in check for w in ["cancel", "rescheduled", "postpone"]):
            return CANCELLED
        now_et = datetime.now(tz=self._tz).replace(tzinfo=None)
        if item["start"] < now_et:
            return PASSED
        return TENTATIVE
