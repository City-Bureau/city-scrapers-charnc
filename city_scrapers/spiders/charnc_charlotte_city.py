from city_scrapers_core.constants import BOARD, CITY_COUNCIL, COMMITTEE, NOT_CLASSIFIED

from city_scrapers.mixins.charnc_charlotte_city import CharncCharlotteCitySpiderMixin

spider_configs = [
    {
        "class_name": "CharlotteCityCouncilBusinessMeetingsSpider",
        "name": "charlotte_city_council_business_meetings",
        "agency": "Charlotte City Council Business Meetings",
        "category_label": "City Council Business Meeting",
        "classification": CITY_COUNCIL,
        "legistar_bodies": [
            "City Council Business Meeting",
        ],
    },
    {
        "class_name": "CharlotteCityCouncilCommitteeMeetingsSpider",
        "name": "charlotte_city_council_committee_meetings",
        "agency": "Charlotte City Council Committee Meetings",
        "category_label": "City Council Committee Meeting",
        "classification": COMMITTEE,
        "legistar_bodies": [
            "Budget, Governance, and Intergovernmental Relations Council Committee",
            "Economic Development and Workforce Council Committee",
            "Housing Council Committee",
            "Safety Council Committee",
            "Transportation, Planning, and Development Council Committee",
            "Council Committee Discussions",
            "Housing, Safety and Community Council Committee",
            "Jobs and Economic Development Council Committee",
        ],
    },
    {
        "class_name": "CharlotteCityCouncilStrategySessionSpider",
        "name": "charlotte_city_council_strategy_session",
        "agency": "Charlotte City Council Strategy Session",
        "category_label": "City Council Strategy Session",
        "classification": CITY_COUNCIL,
        "legistar_bodies": [
            "City Council Strategy Session",
            "City Council Annual Strategy Meeting",
        ],
    },
    {
        "class_name": "CharlotteCityZoningMeetingsSpider",
        "name": "charlotte_city_zoning_meetings",
        "agency": "Charlotte City Zoning Meeting",
        "category_label": "City Council Zoning Meeting",
        "classification": CITY_COUNCIL,
        "legistar_bodies": [
            "City Council Zoning Meeting",
        ],
    },
    {
        "class_name": "CharlotteAdvisoryBoardSpider",
        "name": "charlotte_advisory_board",
        "agency": "Charlotte Advisory Board Meetings",
        "category_label": "Advisory Board",
        "classification": BOARD,
        "legistar_bodies": [],
    },
    {
        "class_name": "CharlotteMajorEventsSpider",
        "name": "charlotte_major_events",
        "agency": "Charlotte Major Events",
        "category_label": "Major events",
        "classification": NOT_CLASSIFIED,
        "legistar_bodies": [],
    },
    {
        "class_name": "CharlotteTalksAndWorkshopsSpider",
        "name": "charlotte_talks_and_workshops",
        "agency": "Charlotte Talks and Workshops",
        "category_label": "Talks & workshops",
        "classification": NOT_CLASSIFIED,
        "legistar_bodies": [],
    },
    {
        "class_name": "CharlotteProjectPublicMeetingsSpider",
        "name": "charlotte_project_public_meetings",
        "agency": "Charlotte Project Public Meetings",
        "category_label": "Project Public Meeting",
        "classification": NOT_CLASSIFIED,
        "legistar_bodies": [],
    },
    {
        "class_name": "CharlotteTownHallSpider",
        "name": "charlotte_town_hall",
        "agency": "Charlotte Town Hall Meetings",
        "category_label": "Town Hall",
        "classification": NOT_CLASSIFIED,
        "legistar_bodies": [],
    },
]


def create_spiders():
    """
    Dynamically create spider classes using the spider_configs list
    and register them in the global namespace.
    """
    for config in spider_configs:
        class_name = config["class_name"]

        if class_name not in globals():
            # Build attributes dict without class_name to avoid duplication.
            # We make sure that the class_name is not already in the global namespace
            # Because some scrapy CLI commands like `scrapy list` will inadvertently
            # declare the spider class more than once otherwise
            attrs = {k: v for k, v in config.items() if k != "class_name"}

            # Dynamically create the spider class
            spider_class = type(
                class_name,
                (CharncCharlotteCitySpiderMixin,),
                attrs,
            )

            globals()[class_name] = spider_class


# Create all spider classes at module load
create_spiders()
