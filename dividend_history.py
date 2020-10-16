from datetime import datetime, timedelta
import time, requests, pandas, lxml
from lxml import html

def format_date(date_datetime):
    date_timetuple = date_datetime.timetuple()
    date_mktime = time.mktime(date_timetuple)
    date_int = int(date_mktime)
    date_str = str(date_int)
    return date_str

class dividend_history:
    base_url = "https://finance.yahoo.com/quote/"

    def __init__(self, symbol, start, end):
        self.subdomain = "{0}/history?period1={1}&period2={2}&interval=div%7Csplit&filter=div&frequency=1d".format(symbol, start, end)
        self.url = self.base_url + self.subdomain
        self.hdrs = {"authority": "finance.yahoo.com",
                "method": "GET",
                "path": self.subdomain,
                "scheme": "https",
                "accept": "text/html,application/xhtml+xml",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "dnt": "1",
                "pragma": "no-cache",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0"}

    def scrape_page(self):
        page = requests.get(url, headers=header)
        element_html = html.fromstring(page.content)
        table = element_html.xpath('//table')
        table_tree = lxml.etree.tostring(table[0], method='xml')
        panda = pandas.read_html(table_tree)
        return panda

    def clean_dividends(self, dividends):
        index = len(dividends)
        dividends = dividends.drop(index - 1)
        dividends = dividends.set_index('Date')
        dividends = dividends['Dividends']
        dividends = dividends.str.replace(r'\Dividend', '')
        dividends = dividends.astype(float)
        dividends.name = symbol
        return dividends

if __name__ == '__main__':
    symbol = "AAPL"
    # create datetime objects
    start = format_date(datetime.today() - timedelta(days=9125))
    end = format_date(datetime.today())

    # scrape the dividend history table from Yahoo Finance
    aapl = dividend_history(symbol, start, end)

    dividends = aapl.scrape_page()
    # clean the dividend history table
    clean_div = aapl.clean_dividends(dividends[0])