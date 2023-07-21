import json
import queue

from fluent_scrape import *

companies = queue.Queue()
for c in ["apple-computer-inc", "microsoft-corp", "google-inc", "facebook-inc", "amazon-com-inc"]:
    companies.put(c)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Accept": "*/*",
}


def scraper_investing():
    curr_company = ""

    def company_info(grp: XScraperGroup):
        el_infos = xmulti(grp.owner.get_element("//div[@data-test='key-info']"), "./dl/div")
        infos = []

        for el in el_infos:
            name = xtext(xsingle(el, "./dt"))
            value = xtext(xsingle(el, "./dd"))
            infos.append({"name": name, "value": value})

        return {
            "companies": [
                {
                    "company_name": curr_company,
                    "infos": infos
                }
            ]
        }

    base_url_investing = 'https://www.investing.com/equities/'

    def first_pop_company():
        try:
            nonlocal curr_company
            curr_company = companies.get_nowait()
            print("Extracting company: " + curr_company + " by thread_id: " + str(threading.get_ident()))
            return base_url_investing + curr_company
        except queue.Empty:
            return None

    g: XScraperGroup
    return (
        XCloudScraper()
        .global_headers(headers)
        .from_website("company_profiles", first_pop_company)
        .scrape(company_info)
        .then()
    )


results = xmerge_list(*[scraper_investing() for _ in range(len(companies.queue))])
print(json.dumps(results, indent=4, default=str))
