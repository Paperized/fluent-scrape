from typing import Any

import cloudscraper
from lxml import html
from lxml.html import HtmlElement

from . import XScraper, XScraperElement, XScraperGroup


class XCloudScraperElement(XScraperElement['XCloudScraper', HtmlElement]):
    def __init__(self, scraper: 'XCloudScraper', native_element: HtmlElement | None):
        super().__init__(scraper, native_element)

    def get_attribute(self, attribute: str) -> str | None:
        attr = self.native_element.get(attribute)
        return attr.strip() if attr else None

    def get_text(self) -> str:
        return self.native_element.text_content().strip()

    def get_html(self) -> str:
        return html.tostring(self.native_element).strip()

    def get_multiple_by_xpath(self, xpath: str, timeout: float = 0):
        return [XCloudScraperElement(self.__scraper__, e) for e in self.native_element.xpath(xpath)]

    def get_single_by_xpath(self, xpath: str, timeout: float = 0):
        res = self.native_element.xpath(xpath)
        return None if len(res) == 0 else XCloudScraperElement(self.__scraper__, res[0])


class XCloudScraper(XScraper[XCloudScraperElement]):

    def __init__(self):
        self.scraper: cloudscraper.CloudScraper | None = None
        self.tree: Any = None
        super().__init__()

    def __initialize_impl__(self):
        self.scraper = cloudscraper.create_scraper()

    def __prepare_document__(self, url: str, x_group: XScraperGroup = None) -> 'XCloudScraper':
        merged_headers = {**self.headers, **x_group.headers}
        r = self.scraper.get(url, headers=merged_headers)
        self.tree = html.fromstring(r.content)
        return self

    def get_elements(self, xpath: str, timeout: float = 0) -> list[XCloudScraperElement]:
        elements: list[HtmlElement] = self.tree.xpath(xpath)
        return [XCloudScraperElement(self, e) for e in elements]

    def get_element(self, xpath: str, timeout: float = 0) -> XCloudScraperElement:
        elements: list[HtmlElement] = self.tree.xpath(xpath)
        if len(elements) == 0 or not elements:
            print(f"Could not find element with xpath: {xpath}")
            return XCloudScraperElement(self, None)

        return XCloudScraperElement(self, elements[0])

