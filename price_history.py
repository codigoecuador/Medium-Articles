from datetime import datetime, timedelta
import time
from multiprocessing import Pool
import numpy as np, pandas as pd
import math, requests, html
import lxml, lxml.html
import concurrent.futures

class price_history:

    def __init__(self, symbol, start, end):
        '''

        :param symbol: example "AAPL, or TRI.TO"
        :param start: Start date as datetime object
        :param end: End date as datetime object
        '''
        self.symbol = symbol.upper()
        self.start = start
        self.end = end
        self.hdrs = {"authority": "finance.yahoo.com",
                "method": "GET",
                "scheme": "https",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "dnt": "1",
                "pragma": "no-cache",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"}
        self.urls = self.__urls__()

    def scrape_history(self, url):
        '''

        :param url: URL location of stock price history
        :return: price history
        '''
        symbol = self.symbol
        hdrs = self.hdrs
        price_history = self.__table__(url, hdrs)
        price_history = self.__clean_history__(price_history)
        return price_history

    def __getStarts__(self):
        response = self.__check__(self.start, self.end)
        pages = self.__calc_pages__(response)
        starts = self.__starts__(pages, self.start, self.end)
        return starts

    def __format_date__(self, date_datetime):
        date_timetuple = date_datetime.timetuple()
        date_mktime = time.mktime(date_timetuple)
        date_int = int(date_mktime)
        date_str = str(date_int)
        return date_str

    def __clean_history__(self, price_history):
        history = price_history.drop(len(price_history) - 1)
        history = history.set_index('Date')
        return history

    def __table__(self, url, hdrs):
        page = requests.get(url, headers=hdrs)
        tree = lxml.html.fromstring(page.content)
        table = tree.xpath('//table')
        string = lxml.etree.tostring(table[0], method='xml')
        data = pd.read_html(string)[0]
        return data

    def __check__(self, s, e):
        if np.busday_count(np.datetime64(s, "D"), np.datetime64(e, "D")) > 100:
            response = True
        else:
            response = False
        return response

    def __calc_pages__(self, response):
        s, e = [self.start, self.end]
        if response:
            pages = math.ceil(np.busday_count(np.datetime64(s, "D"), np.datetime64(e, "D")) / 100)
        else:
            pages = 1
        return pages

    def __calc_start__(self, pages, s, e):
        calendar_days = (e - s) / pages
        while pages > 0:
            s = s + calendar_days
            yield s
            pages -= 1

    def __starts__(self, pages, s, e):
        starts = []
        for s in self.__calc_start__(pages, s, e):
            if pages == 0:
                break
            starts.append(s)
        starts.append(e)
        return starts

    def __urls__(self):
        '''

        Returns
        -------
        urls : a list of urls complete with start and end dates for each 100 trading day block

        '''
        starts = self.__getStarts__()
        symbol = self.symbol
        urls = []
        for d in range(len(starts) - 1):
            start = str(self.__format_date__(starts[d]))
            end = str(self.__format_date__(starts[d + 1]))
            url = "HTTP://finance.yahoo.com/quote/{0}/history?period1={1}&period2={2}&interval=1d&filter=history&frequency=1d"
            url = url.format(symbol, start, end)
            urls.append(url)
        return urls

if __name__ == "__main__":
    start = datetime.today() - timedelta(days=365*10)
    end = datetime.today()
    aapl = price_history('aapl', start, end)
    urls = aapl.urls


    t0 = time.time()
    p = Pool()
    history_pool = p.map(aapl.scrape_history, urls)
    t1 = time.time()
    print(f"Multiprocessing: {t1 - t0} seconds to download {len(urls)} urls.")


    t0 = time.time()
    threads = min(100, len(urls))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        history = list(executor.map(aapl.scrape_history, urls))

    t1 = time.time()
    print(f"Multithreading: {t1 - t0} seconds to download {len(urls)} urls.")

    history_concat = pd.concat(history)
    history_concat = history_concat[~history_concat.Open.str.contains("Dividend")]