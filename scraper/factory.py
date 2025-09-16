from .ssense import SsenseScraper

class ScraperFactory:
    _scrapers = {
        'SsenseScraper': SsenseScraper,
    }

    @classmethod
    def get(cls, scraper_name: str):
        if scraper_name in cls._scrapers:
            return cls._scrapers[scraper_name]
        raise ValueError(f"Scraper {scraper_name} not registered.")