import re
from datetime import datetime
from zoneinfo import ZoneInfo

from city_scrapers_core.constants import BOARD, COMMITTEE, NOT_CLASSIFIED
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

    AGENDA_MINUTES_RE = re.compile(r"\s*Agenda\s+Minutes\s*$", re.I)
    LOCATION_PREFIX_RE = re.compile(r"^Location:\s*")

    LOCATION_EXCLUSION_KEYWORDS = [
        "email",
        "please",
        "meeting will be",
        "board of trustees is",
        "closed session",
        "pursuant",
        "speak during",
    ]

    # Length limits prevent false positives in location extraction:
    # - Inline locations (same paragraph as date/title) are typically short venue names
    # - Longer text is likely descriptive content, not a location
    MAX_INLINE_LOCATION_LENGTH = 80

    # Sibling paragraph locations can include full addresses, so allow more characters
    # - Includes venue name + street address + city, state, zip
    # - Example: "Library Administration Center, 510 Stitt Road, Charlotte, NC 28213"
    MAX_SIBLING_LOCATION_LENGTH = 120

    # Limit description extraction to 5 paragraphs after the meeting header
    # - Prevents pulling unrelated content from distant paragraphs
    # - Most meeting descriptions are 1-3 paragraphs
    MAX_SIBLING_PARAGRAPHS_TO_CHECK = 5

    # Check up to 4 paragraphs for agenda/minutes links
    # - Links are usually within 2-3 paragraphs of the meeting header
    # - Prevents false matches from unrelated meetings further down the page
    MAX_LINK_CANDIDATES = 4

    def _normalize_whitespace(self, text):
        """Normalize whitespace and non-breaking spaces to single spaces."""
        return re.sub(r"[\s\xa0]+", " ", text).strip()

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
            try:
                strong_text = self._normalize_whitespace(
                    " ".join(p.css("strong::text").getall())
                )
                if not re.search(r"\b\w+ \d{1,2}, \d{4}\b", strong_text):
                    continue

                start = self._parse_dt(strong_text, "start")
                if not start:
                    continue

                # Convert to timezone-aware for comparison with cutoff
                start_aware = start.replace(tzinfo=ZoneInfo(self.timezone))
                if start_aware < cutoff:
                    continue

                end = self._parse_dt(strong_text, "end")
                raw_title = self._get_raw_title(p)
                title = self._parse_title(raw_title)
                location = self._parse_location(p)

                meeting_data = {
                    "title": title,
                    "description": self._parse_description(
                        p, strong_text, location["name"]
                    ),
                    "classification": self._parse_classification(
                        "{} {}".format(title, self.agency)
                    ),
                    "start": start,
                    "end": end,
                    "all_day": False,
                    "time_notes": (
                        "For more accurate meeting location, please refer to the "
                        "meeting attachments."
                    ),
                    "location": location,
                    "links": self._parse_links(p),
                    "source": response.url,
                }
                meeting = Meeting(**meeting_data)
                meeting["status"] = self._get_status(meeting, text=strong_text)
                meeting["id"] = self._get_id(meeting)
                yield meeting
            except Exception as e:
                self.logger.error(
                    f"Failed to parse meeting from paragraph: {e}", exc_info=True
                )

    def _parse_description(self, p, strong_text, location_name=""):
        parts = []

        # Extract text with emails from the main paragraph
        main_text = self._extract_text_with_emails(p)

        # Remove the strong text (date/time and title) from main_text
        if strong_text and main_text.startswith(strong_text):
            main_text = main_text[len(strong_text) :].strip()

        # Also remove the raw title if present
        p_texts = p.xpath("./text()").getall()
        if p_texts:
            title_text = self._normalize_whitespace(p_texts[0])
            if title_text and main_text.startswith(title_text):
                main_text = main_text[len(title_text) :].strip()

        # Add remaining text if it's not a location
        if main_text and main_text != location_name:
            if not self._is_location_string(main_text, location_name):
                main_text = self.AGENDA_MINUTES_RE.sub("", main_text).strip()
                if main_text:
                    parts.append(main_text)

        # Sibling paragraphs: non-location, non-agenda/minutes-only content
        for i in range(1, self.MAX_SIBLING_PARAGRAPHS_TO_CHECK):
            try:
                sib = p.xpath("following-sibling::p[{}]".format(i))
                if not sib:
                    break
                if sib.css("strong"):
                    break

                # Extract text with decoded emails
                sib_clean = self._extract_text_with_emails(sib)
                sib_clean = self.LOCATION_PREFIX_RE.sub("", sib_clean).strip()

                if not sib_clean:
                    continue
                if location_name and sib_clean == location_name:
                    continue
                if self._is_location_string(sib_clean, location_name):
                    continue
                direct = self._normalize_whitespace(
                    " ".join(
                        t.strip()
                        for t in sib.xpath(".//text()[not(ancestor::a)]").getall()
                        if t.strip() and t.strip() != "\xa0"
                    )
                )
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
                sib_clean = self.AGENDA_MINUTES_RE.sub("", sib_clean).strip()
                if sib_clean:
                    parts.append(sib_clean)
            except Exception as e:
                self.logger.warning(f"Failed to process sibling paragraph {i}: {e}")
                continue

        return " ".join(part for part in parts if part)

    def _extract_text_with_emails(self, selector):
        """Extract text from selector, decoding Cloudflare-protected emails."""
        result = []

        # Process all child nodes in order
        for child in selector.xpath("./node()"):
            try:
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
            except Exception as e:
                self.logger.warning(f"Failed to process child node: {e}")
                continue

        return self._normalize_whitespace(" ".join(result))

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

    def _parse_dt(self, text, which):
        text = self._normalize_whitespace(text)
        for typo, fix in self.MONTH_TYPO_MAP.items():
            text = re.sub(typo, fix, text, flags=re.I)
        # Fix "apm" typo before regex matching
        text = text.replace("apm", "am").replace("APM", "AM")
        m = self.DT_RE.search(text)
        if not m:
            return None
        date_str = m.group(1)
        start_time = m.group(2).replace(" ", "")
        end_time = m.group(3).replace(" ", "") if m.group(3) else None
        try:
            if which == "start":
                naive_dt = dt_parse("{} {}".format(date_str, start_time), ignoretz=True)
                return naive_dt
            elif which == "end" and end_time:
                naive_dt = dt_parse("{} {}".format(date_str, end_time), ignoretz=True)
                return naive_dt
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
        text = self._normalize_whitespace(
            " ".join(
                t.strip()
                for t in selector.css("::text").getall()
                if t.strip() and t.strip() != "\xa0"
            )
        )
        if strip_location_prefix:
            text = self.LOCATION_PREFIX_RE.sub("", text).strip()
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
        # First check sibling paragraph for location
        # (prioritize virtual/online meetings)
        sib = p.xpath("following-sibling::p[1]")
        if sib:
            # Check if sibling contains a date/time pattern
            # (indicates another meeting)
            sib_strong_text = " ".join(sib.css("strong::text").getall())
            if not re.search(r"\b\w+ \d{1,2}, \d{4}\b", sib_strong_text):
                sib_text = self._clean_text(sib, strip_location_prefix=True)
                if (
                    sib_text
                    and len(sib_text) < self.MAX_SIBLING_LOCATION_LENGTH
                    and not any(
                        kw in sib_text.lower()
                        for kw in self.LOCATION_EXCLUSION_KEYWORDS
                    )
                ):
                    # Prioritize sibling if it contains "virtual"
                    if "virtual" in sib_text.lower():
                        return self._split_location(sib_text)

                    has_links = bool(sib.css("a"))
                    if not has_links:
                        return self._split_location(sib_text)

                    # If sibling has links, check if they're agenda/minutes
                    has_agenda = any(
                        "agenda" in t.lower() for t in sib.css("a::text").getall()
                    )
                    has_minutes = any(
                        "minutes" in t.lower() for t in sib.css("a::text").getall()
                    )
                    if has_agenda or has_minutes:
                        loc_text = self._normalize_whitespace(
                            " ".join(
                                t.strip()
                                for t in sib.xpath(
                                    ".//text()[not(ancestor::a)]"
                                ).getall()
                                if t.strip() and t.strip() != "\xa0"
                            )
                        )
                        if loc_text:
                            return self._split_location(loc_text)

        # Fallback: check for inline location in text nodes
        texts = [
            t.strip("\xa0").strip()
            for t in p.xpath("./text()").getall()
            if t.strip() and t.strip() != "\xa0"
        ]
        candidate = None
        if len(texts) > 1:
            candidate = texts[1]
        elif len(texts) == 1:
            candidate = texts[0]

        if candidate:
            if len(candidate) <= self.MAX_INLINE_LOCATION_LENGTH and not any(
                kw in candidate.lower() for kw in self.LOCATION_EXCLUSION_KEYWORDS
            ):
                loc = self.LOCATION_PREFIX_RE.sub("", candidate).strip()
                if loc:
                    return self._split_location(loc)

        return {"name": "", "address": ""}

    def _parse_links(self, p):
        links = []
        candidates = [p]
        for i in range(1, self.MAX_LINK_CANDIDATES):
            try:
                sib = p.xpath("following-sibling::p[{}]".format(i))
                if sib:
                    candidates.append(sib)
            except Exception as e:
                self.logger.warning(f"Failed to get sibling paragraph {i}: {e}")
                continue

        for candidate in candidates:
            try:
                if candidate is not p and candidate.css("strong"):
                    break
                for a in candidate.css("a"):
                    try:
                        href = a.attrib.get("href", "").strip()
                        if not href:
                            continue

                        # Fix URLs missing www subdomain to prevent redirects
                        if href.startswith("https://cmlibrary.org/"):
                            href = href.replace(
                                "https://cmlibrary.org/",
                                "https://www.cmlibrary.org/",
                                1,
                            )

                        text = self._normalize_whitespace(
                            " ".join(a.css("::text").getall())
                        )
                        if "agenda" in text.lower():
                            links.append({"href": href, "title": "Agenda"})
                        elif "minutes" in text.lower():
                            links.append({"href": href, "title": "Minutes"})
                    except Exception as e:
                        self.logger.warning(f"Failed to process link: {e}")
                        continue
            except Exception as e:
                self.logger.warning(f"Failed to process candidate paragraph: {e}")
                continue
        return links
