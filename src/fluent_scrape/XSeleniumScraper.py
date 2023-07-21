from typing import Literal

from selenium.common import TimeoutException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from seleniumwire.webdriver import Chrome, Firefox

from . import XScraper, XScraperElement, XScraperGroup

supported_browser = Literal["chrome", "firefox"]


class XBrowserOptions(object):
    def __init__(self, browser_type: supported_browser = "chrome", headless: bool = True,
                 binary_location: str | None = None, driver_path: str | None = None, user_agent: str | None = None,
                 proxy: str | None = None, additional_options: dict[str, str] = None):
        self.browser_type: supported_browser = browser_type
        self.headless: bool = headless
        self.binary_location: str | None = binary_location
        self.driver_path: str | None = driver_path
        self.user_agent: str | None = user_agent
        self.proxy: str | None = proxy
        self.additional_options: dict[str, str] = additional_options if additional_options is not None else {}


def __find_elements__(driver: WebDriver, find_from: WebDriver | WebElement, by: By,
                      value: str, single_result: bool = False, timeout: float = 0) -> list[WebElement] | WebElement:
    def get_elements_by_timeout():
        if not single_result:
            return WebDriverWait(driver, timeout).until(lambda _: find_from.find_elements(by, value))
        else:
            return WebDriverWait(driver, timeout).until(lambda _: find_from.find_element(by, value))

    try:
        if timeout <= 0:
            return find_from.find_elements(by, value) if not single_result else find_from.find_element(by, value)
        else:
            return get_elements_by_timeout()
    except:
        return [] if not single_result else None


class XSeleniumScraperElement(XScraperElement['XSeleniumScraper', WebElement]):
    def __init__(self, scraper: 'XSeleniumScraper', native_element: WebElement | None):
        super().__init__(scraper, native_element)

    def get_attribute(self, attribute: str) -> str:
        return self.native_element.get_attribute(attribute).strip()

    def get_text(self) -> str:
        return self.native_element.get_attribute("innerText").strip()

    def get_html(self) -> str:
        return self.get_attribute("outerHTML")

    def get_multiple_by_xpath(self, xpath: str, timeout: float = 0):
        res = []
        for e in __find_elements__(self.__scraper__.driver, self.native_element, By.XPATH, xpath, timeout=timeout):
            res.append(XSeleniumScraperElement(self.__scraper__, e))
        return res

    def get_single_by_xpath(self, xpath: str, timeout: float = 0):
        el = __find_elements__(self.__scraper__.driver, self.native_element, By.XPATH, xpath, True, timeout=timeout)
        return XSeleniumScraperElement(self.__scraper__, el) if el is not None else None


class XSeleniumScraper(XScraper[XSeleniumScraperElement]):

    def __init__(self, options: XBrowserOptions):
        self.driver: WebDriver | None = None
        self.options: XBrowserOptions = options
        super().__init__()

    def __initialize_impl__(self):
        match self.options.browser_type:
            case "chrome":
                chrome_options = ChromeOptions()
                chrome_options.headless = self.options.headless
                if self.options.binary_location:
                    chrome_options.binary_location = self.options.binary_location
                chrome_options.add_argument(f"user-agent={self.options.user_agent}")
                if self.options.headless:
                    chrome_options.add_argument("--no-sandbox")
                    chrome_options.add_argument("--disable-dev-shm-usage")
                    chrome_options.add_argument("--disable-gpu")

                for key, value in self.options.additional_options.items():
                    chrome_options.add_argument(f"{key}={value}")

                options_proxy: dict | None = None
                if self.options.proxy:
                    options_proxy = {
                        'proxy': {
                            'https': f'https://{self.options.proxy}',
                            'http': f'http://{self.options.proxy}'
                        }
                    }

                service: ChromeService | None = None
                if self.options.driver_path:
                    service = ChromeService(self.options.driver_path)

                self.driver = Chrome(service=service, options=chrome_options, seleniumwire_options=options_proxy)
            case "firefox":
                firefox_options = FirefoxOptions()
                firefox_options.headless = self.options.headless
                if self.options.binary_location:
                    firefox_options.binary_location = self.options.binary_location
                firefox_options.add_argument(f"user-agent={self.options.user_agent}")
                if self.options.headless:
                    firefox_options.add_argument("--no-sandbox")
                    firefox_options.add_argument("--disable-dev-shm-usage")
                    firefox_options.add_argument("--single-process")
                    firefox_options.add_argument("--disable-gpu")
                    firefox_options.add_argument("--window-size=1920x1080")
                    firefox_options.add_argument("--start-maximized")

                for key, value in self.options.additional_options.items():
                    firefox_options.add_argument(f"{key}={value}")

                options_proxy: dict | None = None
                if self.options.proxy:
                    options_proxy = {
                        'proxy': {
                            'https': f'https://{self.options.proxy}',
                            'http': f'http://{self.options.proxy}'
                        }
                    }

                service: FirefoxService | None = None
                if self.options.driver_path:
                    service = FirefoxService(self.options.driver_path)

                self.driver = Firefox(service=service, options=firefox_options, seleniumwire_options=options_proxy)
            case _:
                raise Exception("Unsupported browser type")

    def global_headers(self, headers: dict[str, str]):
        print("Warning: Headers not supported in selenium scraper")
        return self

    def __prepare_document__(self, url: str, x_group: XScraperGroup = None) -> 'XSeleniumScraper':
        self.driver.get(url)
        return self

    def get_elements(self, xpath: str, timeout: float = 0) -> list[XSeleniumScraperElement]:
        elements: list[WebElement] = __find_elements__(self.driver, self.driver, By.XPATH, xpath, timeout=timeout)
        return [XSeleniumScraperElement(self, e) for e in elements]

    def get_element(self, xpath: str, timeout: float = 0) -> XSeleniumScraperElement:
        element: WebElement = __find_elements__(self.driver, self.driver, By.XPATH, xpath, True, timeout=timeout)
        if not element:
            print(f"Could not find element with xpath: {xpath}")
            return XSeleniumScraperElement(self, None)

        return XSeleniumScraperElement(self, element)

    def click_element(self, xpath: str):
        try:
            element: WebElement = WebDriverWait(self.driver, 2).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            if not element:
                print(f"Could not find element with xpath: {xpath}")
                return False

            self.driver.execute_script("arguments[0].click();", element)
            return True
        except TimeoutException:
            print(f"Could not find element with xpath: {xpath}")
            return False

    def get_cookies(self) -> dict[str, str]:
        cookies = self.driver.get_cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}
