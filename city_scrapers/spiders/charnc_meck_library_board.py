import re
from datetime import datetime
from zoneinfo import ZoneInfo

from city_scrapers_core.constants import (
    BOARD,
    CANCELLED,
    COMMITTEE,
    NOT_CLASSIFIED,
    PASSED,
    TENTATIVE,
)
from city_scrapers_core.items import Meeting
from city_scrapers_core.spiders import CityScrapersSpider
from dateutil.parser import ParserError
from dateutil.parser import parse as dt_parse
from dateutil.relativedelta import relativedelta


class CharncMeckLibraryBoardSpider(CityScrapersSpider):
    name = "charnc_meck_library_board"
    agency = "Charlotte Mecklenburg Library"
    timezone = "America/New_York"
    start_urls = ["https://www.cmlibrary.org/board-trustees-meetings"]
    years_back = 6

    MONTH_TYPO_MAP = {
        "ocotober": "october",
    }

    DT_RE = re.compile(
        r"(\w+ \d{1,2}, \d{4}),?\s*"
        r"(\d{1,2}:\d{2}\s*[ap]m)"
        r"(?:\s*[-\u2013]\s*(\d{1,2}:\d{2}\s*[ap]m))?",
        re.I,
    )

    def decode_cloudflare_email(self, encoded):
        """Decode Cloudflare-protected email from data-cfemail attribute."""
        if not encoded:
            return ""
        try:
            key = int(encoded[:2], 16)
            decoded = "".join(
                chr(int(encoded[i : i + 2], 16) ^ key)
                for i in range(2, len(encoded), 2)
            )
            return decoded
        except (ValueError, IndexError) as e:
            self.logger.warning(
                f"Failed to decode Cloudflare email: {encoded}, error: {e}"
            )
            return ""

    def parse(self, response):
        today = datetime.now(tz=ZoneInfo(self.timezone))
        cutoff = today - relativedelta(years=self.years_back)
        for p in response.css("p"):
            strong_text = re.sub(
                r"[\s\xa0]+",
                " ",
                " ".join(p.css("strong::text").getall()).strip(),
            )
            if not re.search(r"\b\w+ \d{1,2}, \d{4}\b", strong_text):
                continue
            start = self._parse_start(strong_text)
            if not start or start < cutoff:
                continue
            end = self._parse_end(strong_text)
            raw_title = self._get_raw_title(p)
            title = self._parse_title(raw_title)
            location = self._parse_location(p)
            meeting = Meeting(
                title=title,
                description=self._parse_description(p, location["name"]),
                classification=self._parse_classification(
                    "{} {}".format(title, self.agency)
                ),
                start=start,
                end=end,
                all_day=False,
                time_notes=(
                    "For more accurate meeting location, please refer to the "
                    "meeting attachments."
                ),
                location=location,
                links=self._parse_links(p),
                source=response.url,
            )
            meeting["status"] = self._get_status(meeting, text=strong_text)
            meeting["id"] = self._get_id(meeting)
            yield meeting

    def _get_status(self, meeting, text=""):
        if "cancel" in text.lower():
            return CANCELLED
        if meeting["start"] < datetime.now(tz=ZoneInfo(self.timezone)):
            return PASSED
        return TENTATIVE

    def _parse_description(self, p, location_name=""):
        parts = []

        # Extract text with emails from the main paragraph
        main_text = self._extract_text_with_emails(p)

        # Remove the strong text (date/time and title) from main_text
        strong_text = re.sub(
            r"[\s\xa0]+",
            " ",
            " ".join(p.css("strong::text").getall()).strip(),
        )
        if strong_text and main_text.startswith(strong_text):
            main_text = main_text[len(strong_text) :].strip()

        # Also remove the raw title if present
        p_texts = p.xpath("./text()").getall()
        if p_texts:
            title_text = re.sub(r"[\s\xa0]+", " ", p_texts[0]).strip()
            if title_text and main_text.startswith(title_text):
                main_text = main_text[len(title_text) :].strip()

        # Add remaining text if it's not a location
        if main_text and main_text != location_name:
            if not self._is_location_string(main_text, location_name):
                # Remove "Agenda Minutes" from the end of text
                main_text = re.sub(
                    r"\s*Agenda\s+Minutes\s*$", "", main_text, flags=re.I
                ).strip()
                if main_text:
                    parts.append(main_text)

        # Sibling paragraphs: non-location, non-agenda/minutes-only content
        for i in range(1, 5):
            sib = p.xpath("following-sibling::p[{}]".format(i))
            if not sib:
                break
            if sib.css("strong"):
                break

            # Extract text with decoded emails
            sib_clean = self._extract_text_with_emails(sib)
            sib_clean = re.sub(r"^Location:\s*", "", sib_clean).strip()

            if not sib_clean:
                continue
            if location_name and sib_clean == location_name:
                continue
            # Skip if this looks like a location with address
            if self._is_location_string(sib_clean, location_name):
                continue
            direct = re.sub(
                r"\s+",
                " ",
                " ".join(
                    t.strip()
                    for t in sib.xpath(".//text()[not(ancestor::a)]").getall()
                    if t.strip() and t.strip() != "\xa0"
                ),
            ).strip()
            agenda_minutes_links = [
                a
                for a in sib.css("a")
                if any(
                    kw in " ".join(a.css("::text").getall()).lower()
                    for kw in ["agenda", "minutes"]
                )
            ]
            if agenda_minutes_links and not direct:
                continue
            # Remove "Agenda Minutes" from the end of text
            sib_clean = re.sub(
                r"\s*Agenda\s+Minutes\s*$", "", sib_clean, flags=re.I
            ).strip()
            if sib_clean:
                parts.append(sib_clean)

        return " ".join(part for part in parts if part)

    def _extract_text_with_emails(self, selector):
        """Extract text from selector, decoding Cloudflare-protected emails."""
        result = []

        # Process all child nodes in order
        for child in selector.xpath("./node()"):
            # Check for text node
            if not hasattr(child.root, "tag"):
                text = child.get().strip().strip("\xa0")
                if text:
                    result.append(text)
            elif child.root.tag == "a":
                # Link element - check for Cloudflare-protected email
                cf_email = child.xpath(
                    ".//span[@class='__cf_email__']/@data-cfemail"
                ).get()
                if cf_email:
                    decoded = self.decode_cloudflare_email(cf_email)
                    if decoded:
                        result.append(decoded)
                else:
                    # Regular link text
                    text = "".join(child.xpath(".//text()").getall()).strip()
                    if text and text != "\xa0":
                        result.append(text)
            else:
                # Other elements - recursively extract text
                text = "".join(child.xpath(".//text()").getall()).strip()
                if text and text != "\xa0":
                    result.append(text)

        return re.sub(r"\s+", " ", " ".join(result)).strip()

    def _is_location_string(self, text, location_name):
        """
        Check if text is a location string that should be excluded from
        description.
        """
        if not text:
            return False

        # Check if it matches the location name
        if location_name and text == location_name:
            return True

        # Check if it starts with the location name followed by address info
        if location_name and text.startswith(location_name):
            # Check if there's a comma followed by address-like content
            remainder = text[len(location_name) :].strip()
            if remainder.startswith(",") and re.search(r"\d+\s+\w+", remainder):
                return True

        # Check if it's just a library name followed by "Agenda Minutes"
        if re.match(r"^[\w\s:&-]+\s+(Library|Center)\s+Agenda\s+Minutes$", text):
            return True

        return False

    def _get_raw_title(self, p):
        texts = [
            t.strip("\xa0").strip()
            for t in p.xpath("./text()").getall()
            if t.strip() and t.strip() != "\xa0"
        ]
        return texts[0] if texts else self.agency

    def _parse_title(self, raw_title):
        title = raw_title.strip().strip("\xa0").strip()
        title = re.sub(r"\s*\([^)]*\)\s*$", "", title)
        title = re.sub(r"\s+\d{1,2}[./]\d{1,2}[./]\d{2,4}\s*$", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        return title or self.agency

    def _parse_start(self, strong_text):
        return self._parse_dt(strong_text, "start")

    def _parse_end(self, strong_text):
        return self._parse_dt(strong_text, "end")

    def _parse_dt(self, text, which):
        text = re.sub(r"[\s\xa0]+", " ", text.strip().strip("\xa0"))
        for typo, fix in self.MONTH_TYPO_MAP.items():
            text = re.sub(typo, fix, text, flags=re.I)
        m = self.DT_RE.search(text)
        if not m:
            return None
        date_str = m.group(1)
        start_time = m.group(2).replace(" ", "")
        end_time = m.group(3).replace(" ", "") if m.group(3) else None
        try:
            if which == "start":
                naive_dt = dt_parse("{} {}".format(date_str, start_time), ignoretz=True)
                return naive_dt.replace(tzinfo=ZoneInfo(self.timezone))
            elif which == "end" and end_time:
                naive_dt = dt_parse("{} {}".format(date_str, end_time), ignoretz=True)
                return naive_dt.replace(tzinfo=ZoneInfo(self.timezone))
        except (ValueError, ParserError) as e:
            self.logger.warning(
                f"Failed to parse datetime from '{text}' ({which}): {e}"
            )
        return None

    def _parse_classification(self, title):
        classification_map = {
            "board": BOARD,
            "committee": COMMITTEE,
        }
        for keyword, classification in classification_map.items():
            if keyword in title.lower():
                return classification
        return NOT_CLASSIFIED

    def _clean_text(self, selector, strip_location_prefix=False):
        """
        Extract and clean text from a selector, removing extra whitespace
        and nbsp.
        """
        text = re.sub(
            r"\s+",
            " ",
            " ".join(
                t.strip()
                for t in selector.css("::text").getall()
                if t.strip() and t.strip() != "\xa0"
            ),
        ).strip()
        if strip_location_prefix:
            text = re.sub(r"^Location:\s*", "", text).strip()
        return text

    def _split_location(self, location_text):
        """Split location text into name and address components."""
        if not location_text:
            return {"name": "", "address": ""}

        # Pattern 1: Full address with City, State ZIP
        full_address_pattern = r",\s*\d+\s+[^,]+,\s*[^,]+,\s*[A-Z]{2}\s+\d{5}"
        match = re.search(full_address_pattern, location_text)

        if match:
            name = location_text[: match.start()].strip()
            address = location_text[match.start() + 1 :].strip()
            return {"name": name, "address": address}

        # Pattern 2: Partial address (street number + street name)
        partial_address_pattern = r",\s*\d+\s+[^,]+\.?$"
        match = re.search(partial_address_pattern, location_text)

        if match:
            name = location_text[: match.start()].strip()
            address = location_text[match.start() + 1 :].strip()
            return {"name": name, "address": address}

        # Hardcode address for Library Administration Center
        if "library administration center" in location_text.lower():
            return {
                "name": "Library Administration Center",
                "address": "510 Stitt Road, Charlotte, NC 28213",
            }

        # No address found, return as name only
        return {"name": location_text, "address": ""}

    def _parse_location(self, p):
        texts = [
            t.strip("\xa0").strip()
            for t in p.xpath("./text()").getall()
            if t.strip() and t.strip() != "\xa0"
        ]
        if len(texts) > 1:
            candidate = texts[1]
            if len(candidate) < 80 and not any(
                kw in candidate.lower()
                for kw in [
                    "email",
                    "please",
                    "meeting will be",
                    "board of trustees is",
                    "closed session",
                    "pursuant",
                ]
            ):
                loc = re.sub(r"^Location:\s*", "", candidate).strip()
                if loc:
                    return self._split_location(loc)

        sib = p.xpath("following-sibling::p[1]")
        if sib:
            sib_text = self._clean_text(sib, strip_location_prefix=True)
            if (
                sib_text
                and len(sib_text) < 120
                and not any(
                    kw in sib_text.lower()
                    for kw in [
                        "email",
                        "please",
                        "meeting will be",
                        "closed session",
                        "pursuant",
                        "speak during",
                    ]
                )
            ):
                has_agenda = any(
                    "agenda" in t.lower() for t in sib.css("a::text").getall()
                )
                has_minutes = any(
                    "minutes" in t.lower() for t in sib.css("a::text").getall()
                )
                if has_agenda or has_minutes:
                    loc_text = re.sub(
                        r"\s+",
                        " ",
                        " ".join(
                            t.strip()
                            for t in sib.xpath(".//text()[not(ancestor::a)]").getall()
                            if t.strip() and t.strip() != "\xa0"
                        ),
                    ).strip()
                    if loc_text:
                        return self._split_location(loc_text)
                else:
                    return self._split_location(sib_text)

        return {"name": "", "address": ""}

    def _parse_links(self, p):
        links = []
        candidates = [p]
        for i in range(1, 4):
            sib = p.xpath("following-sibling::p[{}]".format(i))
            if sib:
                candidates.append(sib)

        for candidate in candidates:
            if candidate is not p and candidate.css("strong"):
                break
            for a in candidate.css("a"):
                href = a.attrib.get("href", "").strip()
                if not href:
                    continue
                text = " ".join(a.css("::text").getall()).strip().strip("\xa0").strip()
                if "agenda" in text.lower():
                    links.append({"href": href, "title": "Agenda"})
                elif "minutes" in text.lower():
                    links.append({"href": href, "title": "Minutes"})
        return links
