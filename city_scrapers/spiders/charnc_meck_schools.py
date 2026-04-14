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
    years_back = 3
    months_ahead = 3

    boarddocs_api_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeetingsList?open&0.{random_digit}"  # noqa
    boarddocs_detail_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetMeeting?open&0.{random_digit}"  # noqa
    boarddocs_agenda_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/BD-GetAgenda?open&0.{random_digit}"  # noqa
    boarddocs_attachment_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/goto?open&id={attachment_id}"  # noqa
    boarddocs_public_url = "https://go.boarddocs.com/nc/cmsnc/Board.nsf/Public"
    boarddocs_committee_id = "A4EP6J588C05"

    calendar_url = "https://www.cmsk12.org/board/calendar-for-the-board-of-education"
    calendar_api_url = "https://www.cmsk12.org/fs/elements/236115"
    calendar_page_id = "29911"
    calendar_parent_id = "236115"
    last_boarddocs_date = "2026-04-17"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
    }

    def start_requests(self):
        # Scrape BoardDocs for historical and current meetings
        random_digit = random.randint(10**14, 10**15 - 1)
        yield scrapy.Request(
            url=self.boarddocs_api_url.format(random_digit=random_digit),
            method="POST",
            body=f"current_committee_id={self.boarddocs_committee_id}",
            callback=self._parse_boarddocs_list,
            meta={"source": "boarddocs"},
        )

        # Scrape calendar API for future meetings after last BoardDocs date
        # Use Finalsite calendar API to get events month by month
        today = datetime.now(tz=ZoneInfo(self.timezone))
        last_bd_date = dt_parse(self.last_boarddocs_date).date()

        # Start from month containing day after last BoardDocs date
        start_date = (last_bd_date + relativedelta(days=1)).replace(day=1)
        # End date: 3 months ahead as per user rules
        end_date = (today + relativedelta(months=self.months_ahead)).date()

        # Generate requests for each month in the range
        current_date = start_date
        while current_date <= end_date:
            # Format: YYYY-MM-01
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

            # Move to next month
            current_date = current_date + relativedelta(months=1)

    def _parse_boarddocs_list(self, response):
        meetings = response.json()
        filtered_meetings = self._filter_meetings_by_date(meetings)

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
        random_digit = random.randint(10**14, 10**15 - 1)
        yield scrapy.Request(
            url=self.boarddocs_agenda_url.format(random_digit=random_digit),
            method="POST",
            body=f"current_committee_id={self.boarddocs_committee_id}&id={meeting_id}",
            meta={
                "detail_response": response,
                "raw_description": raw_description,
                "source": "boarddocs",
            },
            callback=self._parse_boarddocs_meeting,
        )

    def _parse_boarddocs_meeting(self, response):
        detail_response = response.meta["detail_response"]
        raw_description = response.meta["raw_description"]

        raw_title = detail_response.css(".meeting-name::text").get() or self.agency
        title = self._parse_title(raw_title)
        start = self._parse_start(raw_description, detail_response)
        location = self._parse_location(raw_description)

        meeting = Meeting(
            title=title,
            description=raw_description.strip(),
            classification=BOARD,
            start=start,
            end=None,
            all_day=False,
            time_notes="",
            location=location,
            links=self._parse_links(response),
            source=self.boarddocs_public_url,
        )

        meeting["status"] = self._get_status(meeting, text=raw_description)
        meeting["id"] = self._get_id(meeting)

        yield meeting

    def _parse_title(self, raw_title):
        title = raw_title.strip()
        title = re.sub(r"\s*\([^)]*\)\s*$", "", title)
        title = re.sub(r"\s+\d{1,2}[./]\d{1,2}[./]\d{2,4}\s*$", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        return title or self.agency

    def _parse_start(self, raw_description, detail_response):
        date = detail_response.css(".meeting-date::text").get()
        if not date:
            return None

        description = raw_description.lower()
        time_match = re.search(r"(\d{1,2}:\d{2})\s*([AaPp]\.?[Mm]\.?)", description)

        if time_match:
            time_str = f"{time_match.group(1)} {time_match.group(2).replace('.', '')}"
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
        description = raw_description.lower()

        if "government center" in description or "cmgc" in description:
            room_match = re.search(
                r"(chamber|room|assembly)\s*(\d+|[a-z]+)", description, re.I
            )
            room = f" {room_match.group(0).upper()}" if room_match else ""
            return {
                "name": "Charlotte-Mecklenburg Government Center",
                "address": f"600 East Fourth Street{room}, Charlotte, NC 28202",
            }

        if "virtual" in description or "zoom" in description or "online" in description:
            return {"name": "Virtual", "address": ""}

        address_pattern = (
            r"(\d+\s+[A-Z][a-z]+\s+(?:Street|St|Avenue|Ave|Drive|Dr|Road|Rd|"
            r"Boulevard|Blvd)[^,]*,\s*[A-Z][a-z]+,\s*[A-Z]{2}\s*\d{5})"
        )
        address_match = re.search(address_pattern, raw_description, re.IGNORECASE)

        if address_match:
            address = address_match.group(1).strip()
            return {"name": "", "address": address}

        return {"name": "", "address": ""}

    def _parse_calendar(self, response):
        # Get date filtering info
        last_bd_date = dt_parse(self.last_boarddocs_date).date()
        today = datetime.now(tz=ZoneInfo(self.timezone))
        end_date = (today + relativedelta(months=self.months_ahead)).date()

        # Select calendar events from API response
        all_events = response.css("div.fsCalendarEvent")
        self.logger.info(f"Found {len(all_events)} calendar events")

        # Collect meetings first, then sort before yielding
        meetings = []
        for event_article in all_events:
            try:
                # Get the event link (not the "Read More" link)
                event_links = event_article.css("a.fsCalendarEventLink")
                if not event_links:
                    continue
                event_link = event_links[0]

                title_elem = event_link.css("::text").get()
                if not title_elem:
                    self.logger.debug("Skipping event with no title")
                    continue

                # Skip "Read More" links
                if title_elem.strip().lower() == "read more":
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

                # Parse start and end times
                try:
                    start = dt_parse(start_iso).replace(tzinfo=None)
                    end = dt_parse(end_iso).replace(tzinfo=None)
                except Exception as e:
                    self.logger.warning(f"Failed to parse times from {occur_id}: {e}")
                    continue

                # Filter: only meetings after last BoardDocs date
                if start.date() <= last_bd_date:
                    continue

                # Filter: only meetings within date range
                if start.date() > end_date:
                    continue

                # Extract event URL if available
                event_url = event_link.css("::attr(href)").get() or ""

                title = self._parse_title(title_elem)

                # Extract location if available
                location_text = event_article.css("div.fsLocation::text").get()
                if location_text:
                    location = self._parse_calendar_location(location_text)
                else:
                    location = {"name": "", "address": ""}

                # For calendar events, we only have basic info from list view
                meeting = Meeting(
                    title=title,
                    description="",
                    classification=BOARD,
                    start=start,
                    end=end,
                    all_day=False,
                    time_notes="",
                    location=location,
                    links=[{"title": "Event", "href": event_url}] if event_url else [],
                    source=self.calendar_url,
                )

                meeting["status"] = self._get_status(meeting)
                meeting["id"] = self._get_id(meeting)

                meetings.append(meeting)
                self.logger.debug(f"Parsed calendar event: {title} on {start}")

            except Exception as e:
                self.logger.error(f"Failed to parse calendar event: {e}", exc_info=True)

        # Sort meetings chronologically by start date before yielding
        meetings.sort(key=lambda m: m.get("start") or datetime.min)
        self.logger.info(f"Yielding {len(meetings)} calendar meetings")
        for meeting in meetings:
            yield meeting

    def _parse_calendar_datetime(self, date_str, time_str):
        try:
            if time_str:
                return dt_parse(f"{date_str} {time_str}", ignoretz=True)
            else:
                return dt_parse(f"{date_str} 9:00 AM", ignoretz=True)
        except Exception as e:
            self.logger.warning(f"Failed to parse calendar datetime: {e}")
            return None

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
            return {
                "name": "Charlotte-Mecklenburg Government Center",
                "address": "600 East Fourth Street, Charlotte, NC 28202",
            }

        if "," in location_text:
            parts = location_text.split(",", 1)
            return {"name": parts[0].strip(), "address": parts[1].strip()}

        return {"name": location_text, "address": ""}

    def _parse_links(self, response):
        links = []
        for item in response.css("li.ui-corner-all"):
            agenda_id = item.css("::attr(unique)").get()
            if agenda_id:
                links.append(
                    {
                        "title": "Agenda",
                        "href": self.boarddocs_attachment_url.format(
                            attachment_id=agenda_id
                        ),
                    }
                )
        return links
