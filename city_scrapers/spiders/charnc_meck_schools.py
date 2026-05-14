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


class CharncMeckSchoolsSpider(CityScrapersSpider):
    name = "charnc_meck_schools"
    agency = "Charlotte Mecklenburg Schools"
    timezone = "America/New_York"
    years_back = 1
    months_ahead = 3

    boarddocs_headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "origin": "https://go.boarddocs.com",
        "referer": "https://go.boarddocs.com/nc/cmsnc/Board.nsf/Public",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",  # noqa
    }

    boarddocs_api_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeetingsList?open&0.{random_digit}"  # noqa
    boarddocs_detail_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeeting?open&0.{random_digit}"  # noqa
    boarddocs_agenda_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetAgenda?open&0.{random_digit}"  # noqa
    boarddocs_attachment_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/goto?open&id={attachment_id}"  # noqa
    boarddocs_public_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/Public"
    boarddocs_committee_id = "A4EP6J588C05"

    # Finalsite calendar API - element 236115 provides full event details
    calendar_api_base_url = "https://www.cmsk12.org/fs/elements/236115"
    calendar_public_url = "https://www.cmsk12.org/calendar"
    calendar_page_id = "29911"
    calendar_parent_id = "236115"

    # Charlotte-Mecklenburg Government Center address
    cmgc_address = "600 East 4th Street Charlotte, NC 28202"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
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
            headers=self.boarddocs_headers,
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
                headers=self.boarddocs_headers,
                meta={"meeting_id": meeting_id},
                callback=self._parse_boarddocs_detail,
            )

        # Yield Finalsite calendar requests starting from the day after the
        # last BoardDocs meeting through months_ahead from today.
        # This prevents duplicates between BoardDocs and calendar sources.
        today = datetime.now(tz=ZoneInfo(self.timezone))
        start_date = (self.last_boarddocs_date + relativedelta(days=1)).replace(day=1)
        end_date = (today + relativedelta(months=self.months_ahead)).date()

        current_date = start_date
        while current_date <= end_date:
            cal_date = current_date.strftime("%Y-%m-01")
            cache_buster = random.randint(10**12, 10**13 - 1)

            # Build URL from base URL and parameters
            params = [
                "is_draft=false",
                f"cal_date={cal_date}",
                "is_load_more=true",
                f"page_id={self.calendar_page_id}",
                f"parent_id={self.calendar_parent_id}",
                f"_={cache_buster}",
            ]
            calendar_api_url = f"{self.calendar_api_base_url}?{'&'.join(params)}"

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

    def _is_board_meeting(self, title):
        """Filter meetings using fuzzy match on title keywords.

        Returns True if title contains any of: Special, Board, Committee
        (case insensitive)
        """
        if not title:
            return False
        title_lower = title.lower()
        keywords = ["special", "board", "committee"]
        return any(keyword in title_lower for keyword in keywords)

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
            headers=self.boarddocs_headers,
            meta={
                "raw_title": raw_title,
                "meeting_date": meeting_date,
                "raw_description": raw_description,
                "meeting_id": meeting_id,
                "source": "boarddocs",
            },
            callback=self._parse_boarddocs_meeting,
        )

    def _create_meeting(
        self,
        title,
        description,
        start,
        end=None,
        time_notes="",
        location=None,
        links=None,
        source=None,
    ):
        """Create a Meeting object with status and id populated.

        Args:
            title: Meeting title
            description: Meeting description
            start: Start datetime
            end: End datetime (optional)
            time_notes: Additional time information (optional)
            location: Location dict with name and address (optional)
            links: List of link dicts (optional)
            source: Source URL (optional)

        Returns:
            Meeting object with status and id fields populated
        """
        meeting = Meeting(
            title=title,
            description=description,
            classification=BOARD,
            start=start,
            end=end,
            all_day=False,
            time_notes=time_notes,
            location=location or {"name": "", "address": ""},
            links=links or [],
            source=source or self.boarddocs_public_url,
        )
        meeting["status"] = self._get_status(meeting, text=description)
        meeting["id"] = self._get_id(meeting)
        return meeting

    def _parse_boarddocs_meeting(self, response):
        raw_description = response.meta["raw_description"]
        raw_title = response.meta["raw_title"]
        meeting_date = response.meta["meeting_date"]
        meeting_id = response.meta["meeting_id"]

        title, title_time_str, title_location = self._parse_boarddocs_title(raw_title)
        start = self._parse_start(raw_description, meeting_date, title_time_str)
        location = (
            title_location
            if title_location["name"] or title_location["address"]
            else self._parse_location(raw_description)
        )
        # If location was found via title but has no address, try to extract
        # it from the description (BoardDocs often includes address there).
        if location["name"] and not location["address"]:
            desc_location = self._parse_location(raw_description)
            if desc_location.get("address"):
                location = {**location, "address": desc_location["address"]}

        meeting = self._create_meeting(
            title=title,
            description=raw_description.strip(),
            start=start,
            location=location,
            links=self._parse_links(response, meeting_id),
            source=self.boarddocs_public_url,
        )

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

        # Match: [-- or -] [optional day "Monday at"] time [-- or -] optional_location
        # The [^-–] ensures location doesn't start with another dash
        time_loc_match = re.search(
            r"\s*[-–]{1,2}\s*(?:\w+\s+at\s+)?(\d{1,2}:\d{2}\s*[aApP][mM])"
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
                # Use regex to extract time components for more reliable parsing
                time_match = re.search(
                    r"(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\.?",
                    title_time_str,
                    re.IGNORECASE,
                )
                if time_match:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2)) if time_match.group(2) else 0
                    meridiem = time_match.group(3).lower()

                    # Convert to 24-hour format
                    if meridiem == "p" and hour != 12:
                        hour += 12
                    elif meridiem == "a" and hour == 12:
                        hour = 0

                    # Parse the date and combine with time
                    parsed_date = dt_parse(date, ignoretz=True)
                    return parsed_date.replace(hour=hour, minute=minute)
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse title time '{title_time_str}': {e}"
                )

        # Fall back: search description for the main meeting time, skipping
        # closed-session / preparatory times that appear earlier in the text
        description = raw_description.lower()

        # Improved regex to match various time formats:
        # - "8:00am", "8:00 am", "8:00 a.m.", "8:00 a.m"
        # - "8am", "8 am", "8 a.m."
        # - Handles optional spaces and periods
        time_pattern = r"(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\.?"
        time_matches = list(re.finditer(time_pattern, description))

        if time_matches:
            # Prefer the last match (main meeting usually listed last)
            m = time_matches[-1]
            hour = int(m.group(1))
            minute = int(m.group(2)) if m.group(2) else 0
            meridiem = m.group(3).lower()

            # Convert to 24-hour format
            if meridiem == "p" and hour != 12:
                hour += 12
            elif meridiem == "a" and hour == 12:
                hour = 0

            try:
                parsed_date = dt_parse(date, ignoretz=True)
                return parsed_date.replace(hour=hour, minute=minute)
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
                "address": (
                    address_match.group(1).strip()
                    if address_match
                    else self.cmgc_address
                ),
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
        # Filter calendar events to only include those AFTER the last BoardDocs
        # meeting date through months_ahead from today.
        today = datetime.now(tz=ZoneInfo(self.timezone))
        last_bd_date = self.last_boarddocs_date
        end_date = (today + relativedelta(months=self.months_ahead)).date()

        # Select calendar events from API response.
        # Element 236115 returns events in a calendar grid format
        all_event_links = response.css("a.fsCalendarEventLink")
        self.logger.info(f"Found {len(all_event_links)} calendar events")

        for event_link in all_event_links:
            try:
                title_elem = event_link.css("::text").get()
                if not title_elem or title_elem.strip() == "Read More":
                    self.logger.debug("Skipping event with no title or Read More link")
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

                # Filter: only meetings AFTER last BoardDocs date
                # and within months_ahead
                if start.date() <= last_bd_date or start.date() > end_date:
                    continue

                # Extract title and time notes from title text.
                title, title_location, time_notes = self._parse_calendar_title_details(
                    title_elem
                )

                # Filter: only include meetings with Special/Board/Committee keywords
                if not self._is_board_meeting(title):
                    self.logger.debug(f"Skipping non-board meeting: {title}")
                    continue

                # Element 236115 (grid view) doesn't include fsLocation in the grid,
                # so we rely on location parsed from the event title.
                # The full location will be fetched from the detail API call.
                location = title_location

                # Preliminary Meeting id (used for deduplication)
                temp_meeting = self._create_meeting(
                    title=title,
                    description="",
                    start=start,
                    end=end,
                    time_notes=time_notes,
                    location=location,
                    source=self.calendar_public_url,
                )

                if temp_meeting["id"] in self.seen_ids:
                    self.logger.debug(f"Skipping duplicate event: {title} on {start}")
                    continue
                self.seen_ids.add(temp_meeting["id"])

                # Fetch full event detail from the API using occur_id
                cache_buster = random.randint(10**12, 10**13 - 1)

                # Build detail URL with occur_id parameter
                params = [
                    f"occur_id={occur_id}",
                    "show_ath_event=false",
                    "show_event=true",
                    "is_draft=false",
                    f"_={cache_buster}",
                ]
                detail_url = f"{self.calendar_api_base_url}?{'&'.join(params)}"

                yield scrapy.Request(
                    url=detail_url,
                    callback=self._parse_calendar_event_detail,
                    meta={
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

    def _parse_calendar_event_detail(self, response):
        """Parse event detail from the API response.

        The element 236115 API returns full event details including description
        and location when called with the occur_id parameter.
        """
        event_data = response.meta["event_data"]

        description = ""
        location = event_data["location"]

        try:
            # Extract description from the API response
            # The response contains the full event body text
            body_text = response.css("article ::text").getall()
            if body_text:
                # Filter out navigation/header text and join paragraphs
                description = " ".join([t.strip() for t in body_text if t.strip()])
                # Remove common header text
                description = re.sub(
                    r"^District Events Calendar\s*", "", description
                ).strip()
                description = re.sub(r"^Calendar RSS Feeds\s*", "", description).strip()

            # Try to get location from the response
            location_text = response.css(".fsLocation::text").get()
            if location_text and location_text.strip():
                location = self._parse_calendar_location(location_text.strip())

        except Exception as e:
            self.logger.warning(
                f"Failed to parse event detail for {event_data['occur_id']}: {e}"
            )

        # If we got a description but location still has no address,
        # try to extract the address from the description text.
        if description and not location.get("address"):
            parsed = self._parse_location(description)
            if parsed.get("address"):
                location = {**location, "address": parsed["address"]}

        meeting = self._create_meeting(
            title=event_data["title"],
            description=description,
            start=event_data["start"],
            end=event_data["end"],
            time_notes=event_data["time_notes"],
            location=location,
            source=self.calendar_public_url,
        )
        # Override with pre-calculated status and id from temp_meeting
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
                "address": self.cmgc_address,
            }

        # Handle "Name | City, State" format used by Finalsite calendar
        if "|" in location_text:
            parts = location_text.split("|", 1)
            return {"name": parts[0].strip(), "address": parts[1].strip()}

        if "," in location_text:
            parts = location_text.split(",", 1)
            return {"name": parts[0].strip(), "address": parts[1].strip()}

        return {"name": location_text, "address": ""}

    def _parse_links(self, response, meeting_id):
        attachments = [
            {
                "title": "Meeting Details",
                "href": self.boarddocs_attachment_url.format(attachment_id=meeting_id),
            }
        ]
        agenda_id = response.css("li.XXXXXXui-corner-all::attr(unique)").get()
        if agenda_id:
            attachments.append(
                {
                    "title": "Agenda",
                    "href": self.boarddocs_attachment_url.format(
                        attachment_id=agenda_id
                    ),
                }
            )
        return attachments
