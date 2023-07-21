import abc
from typing import Any, Callable, Tuple, Literal, TypeVar, Generic
import time
import threading

from deepmerge import always_merger
from multiprocessing.pool import ThreadPool

from . import XValueConverter


def xmerge_list(*x_scraper: 'XScraper') -> dict[str, Any]:
    pool = ThreadPool(processes=len(x_scraper))

    result: dict[str, Any] = {}
    start = time.perf_counter()
    scraper_results = pool.map(lambda s: s.get_results(), x_scraper)
    for curr_res in scraper_results:
        result = always_merger.merge(result, curr_res)
    end = time.perf_counter()
    print(f"scrapers execution and merge took {end - start:0.4f} seconds")
    return result


def xmerge_fixed(scraper_maker: Callable[[], 'XScraper'], scraper_number: int) -> dict[str, Any]:
    x_scraper = [scraper_maker() for _ in range(scraper_number)]
    pool = ThreadPool(processes=len(x_scraper))

    result: dict[str, Any] = {}
    start = time.perf_counter()
    scraper_results = pool.map(lambda s: s.get_results(), x_scraper)
    for curr_res in scraper_results:
        result = always_merger.merge(result, curr_res)
    end = time.perf_counter()
    print(f"scrapers execution and merge took {end - start:0.4f} seconds")
    return result


def xsingle(x_element: 'XScraper | XScraperElement | None', xpath: str, timeout: float = 0) -> 'XScraperElement | None':
    if x_element is None:
        return None

    if isinstance(x_element, XScraper):
        return x_element.get_element(xpath, timeout)
    return x_element.get_single_by_xpath(xpath, timeout)


def xmulti(x_element: 'XScraper | XScraperElement | None', xpath: str, timeout: float = 0) -> 'list[XScraperElement]':
    if x_element is None:
        return []

    if isinstance(x_element, XScraper):
        return x_element.get_elements(xpath, timeout)
    return x_element.get_multiple_by_xpath(xpath, timeout)


def xtext(x_element: 'XScraperElement | None', type_name: str = None):
    if x_element is None:
        return None

    return x_element.get_text_as(type_name) if type_name else x_element.get_text()


def xattr(x_element: 'XScraperElement | None', attr: str, type_name: str = None, *args):
    if x_element is None:
        return None

    return x_element.get_attribute_as(attr, type_name, *args) if type_name else x_element.get_attribute(attr)


url_type = str | Callable[[], str | None]
X_NATIVE_ELEMENT = TypeVar('X_NATIVE_ELEMENT')
X_SCRAPER = TypeVar('X_SCRAPER', bound='XScraper')
X_ELEMENT = TypeVar('X_ELEMENT', bound='XScraperElement')
X_GROUP = TypeVar('X_GROUP', bound='XScraperGroup')


class XScraper(Generic[X_ELEMENT], metaclass=abc.ABCMeta):
    """
    Abstract class for all scrapers
    """

    def __init__(self: X_SCRAPER):
        self.__initialize_impl__()
        self.result: dict[str, Any] = {}
        self.headers: dict[str, str] = {}
        self.groups: list[XScraperGroup[X_SCRAPER, X_ELEMENT]] = []
        self.custom_data = {}

    @abc.abstractmethod
    def __initialize_impl__(self):
        """
        Abstract method for initializing the scraper
        """
        pass

    def global_headers(self: X_SCRAPER, headers: dict[str, str]):
        """
        Set global headers
        """
        self.headers = headers
        return self

    def from_website(self: X_SCRAPER, id_group: str, url: url_type,
                     headers: dict[str, str] = None) -> 'XScraperGroup[X_SCRAPER, X_ELEMENT]':
        new_group = XScraperGroup[X_SCRAPER, X_ELEMENT](self, id_group, url, headers)
        self.groups.append(new_group)
        return new_group

    @abc.abstractmethod
    def get_elements(self: X_SCRAPER, xpath: str, timeout: float = 0) -> list[X_ELEMENT]:
        """
        Abstract method for getting elements from a page
        """
        return []

    @abc.abstractmethod
    def get_element(self: X_SCRAPER, xpath: str, timeout: float = 0) -> X_ELEMENT:
        """
        Abstract method for getting an element from a page
        """
        return XScraperElement(self, None)

    def __append_result__(self: X_SCRAPER, key: str, value: Any):
        """
        Append a result
        """
        if key in self.result:
            if isinstance(self.result[key], list):
                if isinstance(value, list):
                    self.result[key].extend(value)
                else:
                    self.result[key].append(value)
            elif isinstance(self.result[key], dict):
                self.result[key].update(value)
            else:
                raise Exception("Cannot append to a non-list or non-dict value")
        else:
            self.result[key] = value
        return self

    def __set_result__(self: X_SCRAPER, key: str, value: Any):
        """
        Set a result
        """
        self.result[key] = value
        return self

    def get_results(self: X_SCRAPER):
        """
        Get the result
        """
        for group in self.groups:
            group.compute_result()
        return self.result

    @abc.abstractmethod
    def __prepare_document__(self: X_SCRAPER, url: str, x_group: X_GROUP = None) -> X_SCRAPER:
        return self


class XScraperElement(Generic[X_SCRAPER, X_NATIVE_ELEMENT], metaclass=abc.ABCMeta):
    def __init__(self: X_ELEMENT, scraper: X_SCRAPER, native_element: X_NATIVE_ELEMENT | None):
        self.native_element = native_element
        self.__scraper__ = scraper

    @abc.abstractmethod
    def get_attribute(self: X_ELEMENT, attribute: str) -> str | None:
        pass

    def get_attribute_as(self: X_ELEMENT, attribute: str, type_name: str = "str", *args) -> Any:
        return XValueConverter.convert(self.get_attribute(attribute), type_name, *args)

    @abc.abstractmethod
    def get_text(self: X_ELEMENT) -> str:
        pass

    def get_text_as(self: X_ELEMENT, type_name: str = "str", *args) -> Any:
        return XValueConverter.convert(self.get_text(), type_name, *args)

    @abc.abstractmethod
    def get_html(self: X_ELEMENT) -> str:
        pass

    @abc.abstractmethod
    def get_multiple_by_xpath(self: X_ELEMENT, xpath: str, timeout: float = 0) -> list[X_ELEMENT]:
        pass

    @abc.abstractmethod
    def get_single_by_xpath(self: X_ELEMENT, xpath: str, timeout: float = 0) -> X_ELEMENT:
        pass


class XScraperGroup(Generic[X_SCRAPER, X_ELEMENT]):

    def __init__(self: X_GROUP, owner: X_SCRAPER, id_group: str, url: url_type, headers: dict[str, str] = None):
        self.id_group = id_group
        self.owner = owner
        self.url = url
        if headers is None:
            headers = {}
        self.headers = headers
        self.scrape_fns: list[Callable[['X_GROUP'], Any | None]] = []

    def scrape(self: X_GROUP, scrape_fn: Callable[['X_GROUP'], Any | None]):
        self.scrape_fns.append(scrape_fn)
        return self

    def __compute_blocks__(self):
        for scrape_fn in self.scrape_fns:
            res = scrape_fn(self)
            if res is None:
                continue

            self.owner.result = always_merger.merge(self.owner.result, res)

    def compute_result(self):
        if isinstance(self.url, str):
            self.owner.__prepare_document__(self.url, self)
            print("[INFO] Extracting data from url: " + self.url + " | thread_id: " + str(threading.get_ident()))
            self.__compute_blocks__()
            return None

        next_url = self.url()
        while next_url is not None:
            self.owner.__prepare_document__(next_url, self)
            print("[INFO] Extracting data from url: " + next_url + " | thread_id: " + str(threading.get_ident()))
            self.__compute_blocks__()
            prev_url = next_url
            next_url = self.url()
            if next_url == prev_url:
                print("[WARNING] Current url is the same as the next one... id: " + self.id_group + " | thread_id: " + str(threading.get_ident()))

    def then(self: X_SCRAPER):
        return self.owner
