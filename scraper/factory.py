from .ssense import SsenseScraper
from .gravitypope import GravitypopeScraper

class ScraperFactory:
    _scrapers = {
        'SsenseScraper': SsenseScraper,
        'GravitypopeScraper': GravitypopeScraper,
    }

    @classmethod
    def get(cls, scraper_class: str):
        return cls._scrapers.get(scraper_class)