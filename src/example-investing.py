import csv
import json
import queue

from fluent_scrape import *


def float_or_none(v: str):
    try:
        return float(v)
    except:
        return None


XValueConverter.register_type("float_or_none", float_or_none)

companies = queue.Queue()
for c in ["apple-computer-inc"]:#, "microsoft-corp", "google-inc", "facebook-inc", "amazon-com-inc"]:
    companies.put(c)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Accept": "*/*",
}


def scraper_investing():
    internal_state = {
        "companies": [],
        "curr_company": None,
        "next_company_index": 0
    }

    def add_to_company_result(key: str):
        def add_to_company_info(v: dict, r: dict):
            curr_company = internal_state["curr_company"]
            v["company_name"] = curr_company
            infos: list
            if key not in r:
                r[key] = infos = []
            else:
                infos = r[key]
            infos.append(v)
        return add_to_company_info

    def company_info(el: XScraperElement):
        data = xmulti(el, "./dl/div")
        if not data:
            return None
        res = {"infos": []}
        for d in data:
            name = xtext(xsingle(d, "./dt"))
            value = xtext(xsingle(d, "./dd"))
            res["infos"].append({"name": name, "value": value})
        return res

    def balance_sheet_data_header(el: XScraperElement):
        if el is None:
            return ""
        year = xtext(xsingle(el, "./span"))
        other = xtext(xsingle(el, "./div"))
        return f'{other}/{year}'

    def balance_sheet_data(el: XScraperElement):
        if el is None:
            return None

        balance_sheet = {
            "sections": [],
            "headers": []
        }

        headers_el = xmulti(el, ".//tr[@id='header_row']/th[position()>1]")
        periods = len(headers_el)
        for header_el in headers_el:
            balance_sheet["headers"].append(balance_sheet_data_header(header_el))

        sections_els = xmulti(el, "./tbody/tr[@id='parentTr' or not(@id)]")
        for section_el in sections_els:
            if section_el is None:
                continue
            current_section = {
                "name": xtext(xsingle(section_el, "./td[1]/span")),
                "values": [xtext(xsingle(section_el, f"./td[{i}]"), "float_or_none") for i in range(2, 2 + periods)],
                "inner_data": []
            }
            balance_sheet["sections"].append(current_section)
            if not section_el.get_attribute("id"):
                continue

            inner_data = section_el.get_multiple_by_xpath("./following-sibling::tr[1]//tr[contains(@class, 'child') and not(contains(@class, 'grand'))]")
            for data in inner_data:
                inner_tr = {
                    "name": xtext(xsingle(data, "./td[1]/span")),
                    "values": [xtext(xsingle(data, f"./td[{i}]"), "float_or_none") for i in range(2, 2 + periods)],
                    "inner_data": []
                }
                current_section["inner_data"].append(inner_tr)

                next_children = data.get_single_by_xpath("./following-sibling::tr[contains(@class, 'grand')]")
                while next_children:
                    inner_tr["inner_data"].append({
                        "name": xtext(xsingle(next_children, "./td[1]/span")),
                        "values": [xtext(xsingle(next_children, f"./td[{i}]"), "float_or_none") for i in range(2, 2 + periods)],
                    })
                    next_children = next_children.get_single_by_xpath("./following-sibling::tr[contains(@class, 'grand')]")

        return balance_sheet

    base_url_investing = 'https://www.investing.com/equities/'

    def first_pop_company():
        extracted_companies = internal_state["companies"]
        try:
            internal_state["curr_company"] = company = companies.get_nowait()
            extracted_companies.append(company)
            print("Extracting company: " + company + " by thread_id: " + str(threading.get_ident()))
            return base_url_investing + company
        except queue.Empty:
            return None

    def make_url(suffix: str):
        def __next_url():
            curr_company_index = internal_state["next_company_index"]
            extracted_companies = internal_state["companies"]
            if not extracted_companies or curr_company_index >= len(extracted_companies):
                internal_state["next_company_index"] = 0
                return None

            company = extracted_companies[curr_company_index]
            internal_state["curr_company"] = company
            internal_state["next_company_index"] = curr_company_index + 1
            return base_url_investing + company + suffix
        return __next_url

    g: XScraperGroup
    return (
        XCloudScraper()
        .global_headers(headers)
        .from_website("inv", first_pop_company)
        .register_block(lambda g: (
            g.make_block("//div[@data-test='key-info']", False, False, add_to_company_result("company_info"))
            .append_maps(company_info)
        ))
        .then()
        .from_website("inv", make_url("-balance-sheet"))
        .register_block(lambda g: (
            g
            .make_block("//div[@id='rrtable']/table", False, False, add_to_company_result("balance_sheet_data"))
            .append_maps(balance_sheet_data)
        ))
        .then()
        .from_website("inv", make_url("-cash-flow"))
        .register_block(lambda g: (
            g
            .make_block("//div[@id='rrtable']/table", False, False, add_to_company_result("cash_flow_data"))
            .append_maps(balance_sheet_data)
        ))
        .then()
        .from_website("inv", make_url("-income-statement"))
        .register_block(lambda g: (
            g
            .make_block("//div[@id='rrtable']/table", False, False, add_to_company_result("income-statement_data"))
            .append_maps(balance_sheet_data)
        ))
        .then()
    )


results = xmerge(*[scraper_investing() for _ in range(len(companies.queue))])
print(json.dumps(results, indent=4, default=str))
# for balance_sheet_data in results["balance_sheet_data"]:
#     with open(f'./exported/balance_{balance_sheet_data["company_name"]}.csv', 'w') as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames = employee_info)
#         writer.writeheader()
#         writer.writerows(new_dict)
#     print(balance_sheet_data["company_name"])

