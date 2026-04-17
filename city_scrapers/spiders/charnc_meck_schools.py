import random
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import scrapy
from city_scrapers_core.constants import BOARD
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider
from dateutil.parser import parse as dt_parse
from dateutil.relativedelta import relativedelta
from scrapy_playwright.page import PageMethod


class CharncMeckSchoolsSpider(CityScrapersSpider):
    name = "charnc_meck_schools"
    agency = "Charlotte Mecklenburg Schools"
    timezone = "America/New_York"
    years_back = 1
    months_ahead = 3

    boarddocs_api_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeetingsList?open&0.{random_digit}"  # noqa
    boarddocs_detail_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeeting?open&0.{random_digit}"  # noqa
    boarddocs_agenda_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetAgenda?open&0.{random_digit}"  # noqa
    boarddocs_attachment_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/goto?open&id={attachment_id}"  # noqa
    boarddocs_public_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/Public"
    boarddocs_committee_id = "A4EP6J588C05"

    calendar_url = "https://www.cmsk12.org/board/calendar-for-the-board-of-education"
    calendar_api_url = "https://www.cmsk12.org/fs/elements/241856"
    calendar_page_id = "29911"
    calendar_parent_id = "241856"

    custom_settings = {
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_ids = set()
        # Set dynamically in _parse_boarddocs_list from the API response.
        self.last_boarddocs_date = None

    def start_requests(self):
        # Scrape BoardDocs first so we can determine the cutover date before
        # issuing any Finalsite calendar requests.
        random_digit = random.randint(10**14, 10**15 - 1)
        yield scrapy.Request(
            url=self.boarddocs_api_url.format(random_digit=random_digit),
            method="POST",
            body=f"current_committee_id={self.boarddocs_committee_id}",
            callback=self._parse_boarddocs_list,
            meta={"source": "boarddocs"},
        )

    def _parse_boarddocs_list(self, response):
        all_meetings = response.json()

        # Filter first so that last_boarddocs_date reflects only the meetings
        # actually scraped from BoardDocs (within the date window).  Using the
        # global max across ALL records would push the cutover far into the
        # future if BoardDocs has placeholder entries for distant dates, which
        # would suppress calendar events (e.g. Jun 9, Jun 23) that fall between
        # the last scraped BoardDocs meeting and those distant placeholder dates.
        filtered_meetings = self._filter_meetings_by_date(all_meetings)

        valid_dates = [
            datetime.strptime(m["numberdate"], "%Y%m%d").date()
            for m in filtered_meetings
            if m and m.get("numberdate")
        ]
        if valid_dates:
            self.last_boarddocs_date = max(valid_dates)
        else:
            self.last_boarddocs_date = datetime.now(tz=ZoneInfo(self.timezone)).date()

        for meeting in filtered_meetings:
            meeting_id = meeting.get("unique")
            random_digit = random.randint(10**14, 10**15 - 1)
            yield scrapy.Request(
                url=self.boarddocs_detail_url.format(random_digit=random_digit),
                method="POST",
                body=(
                    f"current_committee_id={self.boarddocs_committee_id}"
                    f"&id={meeting_id}"
                ),
                meta={"meeting_id": meeting_id},
                callback=self._parse_boarddocs_detail,
            )

        # Yield Finalsite calendar requests starting from the month that
        # contains the day after the last BoardDocs date.  Events on or before
        # last_boarddocs_date are filtered out per-event in _parse_calendar so
        # there is no overlap between the two sources.
        today = datetime.now(tz=ZoneInfo(self.timezone))
        start_date = (self.last_boarddocs_date + relativedelta(days=1)).replace(day=1)
        end_date = (today + relativedelta(months=self.months_ahead)).date()

        current_date = start_date
        while current_date <= end_date:
            cal_date = current_date.strftime("%Y-%m-01")
            cache_buster = random.randint(10**12, 10**13 - 1)
            calendar_api_url = (
                f"{self.calendar_api_url}?"
                f"is_draft=false&"
                f"cal_date={cal_date}&"
                f"is_load_more=true&"
                f"page_id={self.calendar_page_id}&"
                f"parent_id={self.calendar_parent_id}&"
                f"_={cache_buster}"
            )
            yield scrapy.Request(
                url=calendar_api_url,
                callback=self._parse_calendar,
                meta={"cal_date": current_date},
            )
            current_date += relativedelta(months=1)

    def _filter_meetings_by_date(self, data):
        today = datetime.now(tz=ZoneInfo(self.timezone))
        start_date = today - relativedelta(years=self.years_back)
        end_date = today + relativedelta(months=self.months_ahead)

        filtered_data = []
        for item in data:
            if not item:
                continue
            try:
                item_date = datetime.strptime(item["numberdate"], "%Y%m%d").date()
                item_datetime = datetime.combine(
                    item_date, datetime.min.time()
                ).replace(tzinfo=ZoneInfo(self.timezone))
                if start_date <= item_datetime <= end_date:
                    filtered_data.append(item)
            except (ValueError, KeyError) as e:
                self.logger.warning(f"Failed to parse date from item: {e}")
                continue

        # Sort chronologically by numberdate
        filtered_data.sort(key=lambda x: x.get("numberdate", ""))
        return filtered_data

    def _parse_boarddocs_detail(self, response):
        raw_description = " ".join(response.css(".meeting-description::text").getall())
        meeting_id = response.meta["meeting_id"]
        raw_title = response.css(".meeting-name::text").get() or self.agency
        meeting_date = response.css(".meeting-date::text").get()
        random_digit = random.randint(10**14, 10**15 - 1)
        yield scrapy.Request(
            url=self.boarddocs_agenda_url.format(random_digit=random_digit),
            method="POST",
            body=f"current_committee_id={self.boarddocs_committee_id}&id={meeting_id}",
            meta={
                "raw_title": raw_title,
                "meeting_date": meeting_date,
                "raw_description": raw_description,
                "meeting_id": meeting_id,
                "source": "boarddocs",
            },
            callback=self._parse_boarddocs_meeting,
        )

    def _parse_boarddocs_meeting(self, response):
        raw_description = response.meta["raw_description"]
        raw_title = response.meta["raw_title"]
        meeting_date = response.meta["meeting_date"]
        meeting_id = response.meta.get("meeting_id", "")

        title, title_time_str, title_location = self._parse_boarddocs_title(raw_title)
        start = self._parse_start(raw_description, meeting_date, title_time_str)
        location = (
            title_location
            if title_location["name"] or title_location["address"]
            else self._parse_location(raw_description)
        )

        meeting = Meeting(
            title=title,
            description=raw_description.strip(),
            classification=BOARD,
            start=start,
            end=None,
            all_day=False,
            time_notes="",
            location=location,
            links=self._parse_links(response, meeting_id),
            source=self.boarddocs_public_url,
        )

        meeting["status"] = self._get_status(meeting, text=raw_description)
        meeting["id"] = self._get_id(meeting)

        # Skip duplicates
        if meeting["id"] not in self.seen_ids:
            self.seen_ids.add(meeting["id"])
            yield meeting

    def _parse_boarddocs_title(self, raw_title):
        """Extract clean title, time string, and location from BoardDocs meeting titles.

        Common patterns:
        - "Regular Board Meeting -- 6:00pm -"
        - "Emergency Meeting of the Board - 6:00pm - Virtual"
        - "Special Meeting – Budget Workshop - 5:00pm - CMGC Room 267"
        - "Regular Board Meeting -- 6:00pm (Closed Session at 4:00 pm) VIRTUAL MEETING"
        - "Committee Meeting -- 2:00pm -- Virtual"
        - "Family & Community Engagement Committee Meeting -- 12:00pm -- CMGC 527/528"
        """
        title = raw_title.strip()
        time_str = ""
        location = {"name": "", "address": ""}

        # Extract trailing "VIRTUAL MEETING" flag before other parsing
        virtual_match = re.search(r"\s+VIRTUAL\s+MEETING\s*$", title, re.IGNORECASE)
        if virtual_match:
            location = {"name": "Virtual", "address": ""}
            title = title[: virtual_match.start()].strip()

        # Remove trailing parentheticals before time extraction to handle
        # patterns like "-- 6:00pm (Closed Session at 4:00pm) -"
        title = re.sub(r"\s*\([^)]*\)\s*[-–]*\s*$", "", title)

        # Match: [-- or -] time [-- or -] optional_location at end of string
        # The [^-–] ensures location doesn't start with another dash
        time_loc_match = re.search(
            r"\s*[-–]{1,2}\s*(\d{1,2}:\d{2}\s*[aApP][mM])"
            r"(?:\s*[-–]+\s*([^-–].+?))?\s*[-–]*\s*$",
            title,
            re.IGNORECASE,
        )
        if time_loc_match:
            time_str = time_loc_match.group(1).strip()
            loc_str = (time_loc_match.group(2) or "").strip()
            if loc_str and not location["name"] and not location["address"]:
                location = self._parse_calendar_location(loc_str)
            title = title[: time_loc_match.start()].strip()

        title = self._parse_title(title)
        return title, time_str, location

    def _parse_title(self, raw_title):
        title = raw_title.strip()
        title = re.sub(r"\s*\([^)]*\)\s*$", "", title)
        title = re.sub(r"\s+\d{1,2}[./]\d{1,2}[./]\d{2,4}\s*$", "", title)
        title = re.sub(r"\s*[-–]+\s*$", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        return title or self.agency

    def _parse_start(self, raw_description, date, title_time_str=""):
        if not date:
            return None

        # Prefer time extracted from title — more reliable than description
        if title_time_str:
            try:
                return dt_parse(f"{date} {title_time_str}", ignoretz=True)
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse title time '{title_time_str}': {e}"
                )

        # Fall back: search description for the main meeting time, skipping
        # closed-session / preparatory times that appear earlier in the text
        description = raw_description.lower()
        # Find all times, prefer the last one (main meeting usually listed last)
        time_matches = list(
            re.finditer(r"(\d{1,2}:\d{2})\s*([AaPp]\.?[Mm]\.?)", description)
        )
        if time_matches:
            m = time_matches[-1]
            time_str = f"{m.group(1)} {m.group(2).replace('.', '')}"
            try:
                return dt_parse(f"{date} {time_str}", ignoretz=True)
            except Exception as e:
                self.logger.warning(f"Failed to parse datetime: {e}")

        try:
            return dt_parse(f"{date} 9:00 AM", ignoretz=True)
        except Exception as e:
            self.logger.warning(f"Failed to parse date: {e}")
            return None

    def _parse_location(self, raw_description):
        street_types = r"Street|St\.?|Avenue|Ave\.?|Drive|Dr\.?|Road|Rd\.?|Boulevard|Blvd\.?|Way|Lane|Ln\.?"  # noqa: E501
        # Matches addresses with or without a comma before the city name:
        #   "600 East 4th Street Charlotte, NC 28202"
        #   "600 East Fourth Street, Charlotte, NC 28202"
        address_pattern = (
            r"(\d+\s+[A-Za-z0-9\s\.\#]+?(?:" + street_types + r")[^,\n]{0,40}[,\s]*"
            r"[A-Za-z][A-Za-z\s\.\-]{1,30}[,\s]+"
            r"[A-Z]{2}\s*\d{5})"
        )

        if re.search(
            r"government center|govt\.?\s+center|cmgc|char-meck",
            raw_description,
            re.IGNORECASE,
        ):
            # Match specific room identifiers including "Chamber" alone or with "Room"
            room_match = re.search(
                r"(?:chamber(?:\s+room)?|room\s+\d+|ch\d+|\d+/\d+|assembly\s+room)",
                raw_description,
                re.IGNORECASE,
            )
            room = f", {room_match.group(0).upper()}" if room_match else ""
            name = f"Charlotte-Mecklenburg Government Center{room}"
            address_match = re.search(address_pattern, raw_description, re.IGNORECASE)
            return {
                "name": name,
                "address": address_match.group(1).strip() if address_match else "",
            }

        # Only treat as virtual when the meeting itself is virtual, not just
        # viewable online ("view the meeting online at youtube.com" is not virtual)
        if re.search(r"\bvirtual\b", raw_description, re.IGNORECASE):
            return {"name": "Virtual", "address": ""}

        # Try to extract a venue name that appears before a parenthetical address.
        # Example: "held at the Valerie Woodard Center (3205 Freedom Dr Ste 1000,
        #           Charlotte, NC 28208)"
        venue_paren_pattern = (
            r"(?:held\s+at|at)\s+(?:the\s+)?([A-Z][^()]{2,60}?)\s*"
            r"\((\d+\s+[A-Za-z0-9\s\.\#,]+(?:" + street_types + r")[^)]*?\d{5}[^)]*)\)"
        )
        venue_match = re.search(venue_paren_pattern, raw_description, re.IGNORECASE)
        if venue_match:
            return {
                "name": venue_match.group(1).strip().rstrip(",").strip(),
                "address": venue_match.group(2).strip(),
            }

        # Fall back to plain address extraction (no venue name available).
        address_match = re.search(address_pattern, raw_description, re.IGNORECASE)
        if address_match:
            return {"name": "", "address": address_match.group(1).strip()}

        return {"name": "", "address": ""}

    def _parse_calendar(self, response):
        # Get date filtering info
        last_bd_date = self.last_boarddocs_date
        today = datetime.now(tz=ZoneInfo(self.timezone))
        end_date = (today + relativedelta(months=self.months_ahead)).date()

        # Select calendar events from API response.
        # Element 241856 (list view) returns article elements that include
        # a div.fsLocation for location data.
        all_events = response.css("article")
        self.logger.info(f"Found {len(all_events)} calendar events")

        for event_article in all_events:
            try:
                # Get the primary event link (not the "Read More" link).
                # The first fsCalendarEventLink in the article is the title link;
                # the second (fsReadMoreLink) is the "Read More" link.
                event_links = event_article.css(
                    "a.fsCalendarEventLink:not(.fsReadMoreLink)"
                )
                if not event_links:
                    continue
                event_link = event_links[0]

                title_elem = event_link.css("::text").get()
                if not title_elem:
                    self.logger.debug("Skipping event with no title")
                    continue

                # Parse data-occur-id format: eventId_startISO_endISO
                occur_id = event_link.css("::attr(data-occur-id)").get()
                if not occur_id:
                    self.logger.warning(f"No occur_id for event: {title_elem}")
                    continue

                parts = occur_id.split("_")
                if len(parts) < 3:
                    self.logger.warning(f"Invalid occur_id format: {occur_id}")
                    continue

                start_iso = parts[1]
                end_iso = parts[2]

                # Parse start and end times (ISO timestamps are in UTC,
                # convert to local)
                try:
                    start_utc = dt_parse(start_iso)
                    end_utc = dt_parse(end_iso)
                    tz = ZoneInfo(self.timezone)
                    start = start_utc.astimezone(tz).replace(tzinfo=None)
                    end = end_utc.astimezone(tz).replace(tzinfo=None)
                except Exception as e:
                    self.logger.warning(f"Failed to parse times from {occur_id}: {e}")
                    continue

                # Filter: only meetings after last BoardDocs date
                if start.date() <= last_bd_date:
                    continue

                # Filter: only meetings within date range
                if start.date() > end_date:
                    continue

                # Extract title and time notes from title text.
                title, title_location, time_notes = self._parse_calendar_title_details(
                    title_elem
                )

                # Prefer location from div.fsLocation element (available in the
                # list view / element 241856 API). Fall back to any location
                # parsed from the event title.
                location_text = event_article.css("div.fsLocation::text").get()
                if location_text and location_text.strip():
                    location = self._parse_calendar_location(location_text.strip())
                else:
                    location = title_location

                # Preliminary Meeting id (used for dedup before Playwright fetch)
                temp_meeting = Meeting(
                    title=title,
                    description="",
                    classification=BOARD,
                    start=start,
                    end=end,
                    all_day=False,
                    time_notes=time_notes,
                    location=location,
                    links=[],
                    source=self.calendar_url,
                )
                temp_meeting["status"] = self._get_status(temp_meeting)
                temp_meeting["id"] = self._get_id(temp_meeting)

                if temp_meeting["id"] in self.seen_ids:
                    self.logger.debug(f"Skipping duplicate event: {title} on {start}")
                    continue
                self.seen_ids.add(temp_meeting["id"])

                # Yield a Playwright request to fetch the full event detail
                # (description and street-level location) from the JS modal.
                event_id = occur_id.split("_")[0]
                occur_date = start.strftime("%Y-%m-%d")
                detail_url = f"{self.calendar_url}?event={event_id}&occur={occur_date}"
                yield scrapy.Request(
                    url=detail_url,
                    callback=self._parse_calendar_event_page,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        "playwright_page_methods": [
                            PageMethod(
                                "wait_for_selector",
                                "article",
                                timeout=10000,
                            ),
                        ],
                        "event_data": {
                            "title": title,
                            "location": location,
                            "start": start,
                            "end": end,
                            "time_notes": time_notes,
                            "occur_id": occur_id,
                            "meeting_id": temp_meeting["id"],
                            "meeting_status": temp_meeting["status"],
                        },
                    },
                    dont_filter=True,
                )

            except Exception as e:
                self.logger.error(f"Failed to parse calendar event: {e}", exc_info=True)

    async def _parse_calendar_event_page(self, response):
        """Use Playwright to open the event modal and extract description/location.

        The Finalsite calendar loads event body text and detailed location
        (including street address) only through a JavaScript modal.  This
        callback clicks the event title link, waits for the modal to appear,
        and harvests the content before closing the modal.
        """
        page = response.meta["playwright_page"]
        event_data = response.meta["event_data"]
        occur_id = event_data["occur_id"]

        description = ""
        location = event_data["location"]

        try:
            # Click the event title link (not the "Read More" button) to open
            # the Finalsite modal for this specific occurrence.
            event_selector = (
                f'a.fsCalendarEventLink[data-occur-id="{occur_id}"]'
                ":not(.fsReadMoreLink)"
            )
            await page.click(event_selector, timeout=8000)

            # Wait for the modal body to appear.
            modal_body_selector = (
                ".fsModal .fsBody, .fsModal .fsDescription, "
                ".fsLightbox .fsBody, [class*='EventModal'] .fsBody"
            )
            await page.wait_for_selector(modal_body_selector, timeout=8000)

            # Extract description paragraphs from the modal body.
            modal_text = await page.inner_text(
                modal_body_selector.split(",")[0].strip()
            )
            if modal_text:
                description = modal_text.strip()

            # Try to get a more detailed location (with street address) from
            # the modal if present.
            try:
                modal_loc = await page.inner_text(
                    ".fsModal .fsLocation, .fsLightbox .fsLocation"
                )
                if modal_loc and modal_loc.strip():
                    location = self._parse_calendar_location(modal_loc.strip())
            except Exception:
                pass  # Fall back to the location already parsed from the list view

            # Close the modal gracefully.
            try:
                close_selector = (
                    "button[aria-label='Close'], .fsModalClose, "
                    ".fsClose, button.fsCloseButton"
                )
                await page.click(close_selector, timeout=3000)
            except Exception:
                await page.keyboard.press("Escape")

            await page.wait_for_timeout(300)

        except Exception as e:
            self.logger.warning(
                f"Playwright modal interaction failed for {occur_id}: {e}"
            )
        finally:
            await page.close()

        # If modal gave us a description but location still has no address,
        # try to extract the address from the description text.
        if description and not location.get("address"):
            parsed = self._parse_location(description)
            if parsed.get("address"):
                location = {**location, "address": parsed["address"]}

        meeting = Meeting(
            title=event_data["title"],
            description=description,
            classification=BOARD,
            start=event_data["start"],
            end=event_data["end"],
            all_day=False,
            time_notes=event_data["time_notes"],
            location=location,
            links=[],
            source=self.calendar_url,
        )
        meeting["status"] = event_data["meeting_status"]
        meeting["id"] = event_data["meeting_id"]
        yield meeting

    def _parse_calendar_title_details(self, raw_title):
        """Extract title, location, and time notes from calendar event titles.

        Examples:
        - "Regular Meeting of the Board - CMGC Chamber Room (Closed Session at 4:00pm)"
          -> title: "Regular Meeting of the Board", location: CMGC Chamber Room,
             time_notes: "Closed Session at 4:00pm"
        """
        title = raw_title.strip()
        location = {"name": "", "address": ""}
        time_notes = ""

        # Extract parenthetical time notes (e.g., "Closed Session at 4:00pm")
        paren_match = re.search(r"\(([^)]+)\)\s*$", title)
        if paren_match:
            time_notes = paren_match.group(1).strip()
            title = title[: paren_match.start()].strip()

        # Extract location from title (e.g., "- CMGC Chamber Room")
        location_patterns = [
            r"\s*-\s*(CMGC\s+[^-]+?)$",  # "- CMGC Chamber Room"
            r"\s*--\s*([^-]+?)$",  # "-- Virtual" or "-- CMGC 527/528"
        ]

        for pattern in location_patterns:
            loc_match = re.search(pattern, title, re.IGNORECASE)
            if loc_match:
                location_str = loc_match.group(1).strip()
                title = title[: loc_match.start()].strip()
                location = self._parse_calendar_location(location_str)
                break

        # Clean up title
        title = self._parse_title(title)

        return title, location, time_notes

    def _parse_calendar_location(self, location_text):
        if not location_text:
            return {"name": "", "address": ""}

        location_text = location_text.strip()

        if "virtual" in location_text.lower():
            return {"name": "Virtual", "address": ""}

        if (
            "government center" in location_text.lower()
            or "cmgc" in location_text.lower()
        ):
            # Extract room info if present
            room_match = re.search(
                r"(?:chamber(?:\s+room)?|room\s+\d+|ch\d+|\d+/\d+)",
                location_text,
                re.IGNORECASE,
            )
            room = f", {room_match.group(0).upper()}" if room_match else ""
            return {
                "name": f"Charlotte-Mecklenburg Government Center{room}",
                "address": "",
            }

        # Handle "Name | City, State" format used by Finalsite calendar
        if "|" in location_text:
            parts = location_text.split("|", 1)
            return {"name": parts[0].strip(), "address": parts[1].strip()}

        if "," in location_text:
            parts = location_text.split(",", 1)
            return {"name": parts[0].strip(), "address": parts[1].strip()}

        return {"name": location_text, "address": ""}

    def _parse_links(self, response, meeting_id=""):
        if not meeting_id:
            return []
        return [
            {
                "title": "Download Agenda as PDF",
                "href": self.boarddocs_attachment_url.format(attachment_id=meeting_id),
            }
        ]
