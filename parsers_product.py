import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def parse_loans():
    """
    Parse consumer and refinancing loans of ПСБ from aggregator pages
    and return a list of product dictionaries.
    """
    products = []
    # Example: parse "Кредит на любые цели" from topbanki.ru
    loan_url = "https://www.topbanki.ru/credits/promsvjazbank/kredit-nalichnymi"
    resp = requests.get(loan_url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.content, "html.parser")

    # Extract min/max sum, term and rate (illustrative pattern)
    # Note: Each aggregator page has its own HTML structure. You must inspect
    # the specific page and adjust selectors accordingly.
    def get_text(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else None

    products.append({
        "product_name": "Кредит «На любые цели»",
        "product_type": "loan",
        "currency": "RUB",
        "rate": get_text(".product-rate"),
        "amount_min": get_text(".product-sum .min"),
        "amount_max": get_text(".product-sum .max"),
        "term": get_text(".product-term"),
        "fees": None,
        "bonuses": None,
        "requirements": None,
        "additional_terms": None,
        "extra_features": [],
        "data_actuality": datetime.now().strftime("%d.%m.%Y"),
        "source_url": loan_url
    })
    # Repeat the pattern for other loan pages...
    return products

def parse_cards():
    """
    Parse debit and credit card offers of ПСБ from aggregator pages
    and return a list of product dictionaries.
    """
    products = []

    # Example: parse "Твой Cashback" card from bank-rank.ru
    url = "https://bank-rank.ru/product/psb-tvoi-cashback"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.content, "html.parser")

    # Extract cashback percentage, fees, limits
    rate = None
    fees = None
    # (Selectors need to be tailored to the page structure.)
    products.append({
        "product_name": "Дебетовая карта «Твой Cashback»",
        "product_type": "debit_card",
        "currency": "RUB",
        "rate": rate,
        "amount_min": None,
        "amount_max": None,
        "term": None,
        "fees": fees,
        "bonuses": "Кэшбэк до 5% + 1.5% на все покупки",
        "requirements": "Возраст от 18 лет; паспорт",
        "additional_terms": "Снятие наличных бесплатно в банкоматах ПСБ и партнёров",
        "extra_features": [],
        "data_actuality": datetime.now().strftime("%d.%m.%Y"),
        "source_url": url
    })
    # Repeat for each card on different aggregators...
    return products

def parse_deposits():
    """
    Parse deposit products of ПСБ from aggregator pages (e.g. sravni.ru)
    and return a list of product dictionaries.
    """
    deposits = []

    url = "https://www.sravni.ru/bank/promsvjazbank/vklady/"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.content, "html.parser")
    text = soup.get_text(separator="\n")

    # Example parsing using regular expressions for two deposits:
    for match in re.finditer(
        r"(Моя\\s+выгода|Безлимитный)\\s*От\\s*(\\d[\\d\\s]*)\\s*До\\s*([\\d,\\.]+)\\s*(\\d+\\s*-\\s*\\d+)",
        text
    ):
        name, min_amount, rate, term = match.groups()
        deposits.append({
            "product_name": f"Вклад «{name}»",
            "product_type": "deposit",
            "currency": "RUB",
            "rate": f"до {rate.replace(',', '.')}%",
            "amount_min": min_amount.replace(" ", ""),
            "amount_max": None,
            "term": term.replace(" ", "") + " дней",
            "fees": None,
            "bonuses": None,
            "requirements": None,
            "additional_terms": None,
            "extra_features": [],
            "data_actuality": datetime.now().strftime("%d.%m.%Y"),
            "source_url": url
        })
    return deposits



def build_psb_products():
    """
    Build the full list of ПСБ products by combining data parsed from
    multiple aggregator sources. Where information is unavailable, leave
    fields as None.
    """
    products = []

    # Parse each category
    products.extend(parse_loans())
    products.extend(parse_cards())
    products.extend(parse_deposits())

    # ... Add calls to other parsing functions (mortgages, insurance, etc.)

    return products

def save_products(products: list[dict], json_path: str) -> None:
    """
    Save the list of products to a JSON file.
    """
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    products = build_psb_products()
    save_products(products, "psb_products.json")
    print(f"Saved {len(products)} products to psb_products.json")
