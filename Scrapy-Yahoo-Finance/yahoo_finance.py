import html

import lxml
import scrapy
import pandas
from itertools import islice

from scrapy.http import HtmlResponse, XmlResponse

from ..items import YahooItem

class YahooSpider(scrapy.Spider):
    name = 'Yahoo'
    symbols = ["ADSK","BA","CAT","EBAY","GS","HSY","IBM","JPM","WMT","SHOP",
               "T", "F", "TRI", "AMZN", "C", "A", "O", "B","MSFT", "NVDA",
               "DIS", "AAL", "NFLX", "JNJ","BAC","GOOGL", "WFC"]
    start_urls = ['https://finance.yahoo.com/quote/{0}/history?p={0}'.format(x) for x in symbols]

    def parse(self, response):
        items = YahooItem()
        data = response.xpath('//table//text()').extract()
        title = response.xpath('//title//text()').extract()
        num_cols = 7
        output = [data[i:i + num_cols] for i in range(0, len(data), num_cols)]
        dictionary = pandas.DataFrame(output[1:], columns=output[0]).set_index('Date').to_dict()
        items['title'] = title
        items['data'] = dictionary
        yield items