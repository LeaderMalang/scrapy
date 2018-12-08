# -*- coding: utf-8 -*-
import scrapy
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import json
from datetime import datetime

class CoinmarketcapSpider(scrapy.Spider):
    name = 'coinmarketcap'
    download_delay = 2
    allowed_domains = ['coinmarketcap.com']
    start_urls = ['https://coinmarketcap.com/']
    def parse(self, response):

        cointable=response.css('table#currencies').xpath('//tbody/tr')
        for coins in cointable:
            #name=coins.css('a.currency-name-container::text').extract_first().strip()
            single_url=coins.css('a.currency-name-container::attr(href)').extract_first();
            url = response.urljoin(single_url)
            yield scrapy.Request(url=url, callback=self.single_parse)


        ul = response.css('ul.top-paginator>li')
        nexturl=None
        for li in ul:
            title=li.css('a::text').extract_first()
            if 'Next' in title:
                nexturl=li.css('a::attr(href)').extract_first()
        if nexturl:
            nextPage = response.urljoin(nexturl)
            yield scrapy.Request(url=nextPage, callback=self.parse)



    def single_parse(self,response):
        items=dict()
        name = response.css('.details-panel-item--name::text').extract()[1].strip()
        url = response.url
        slug = url.split('/')[-2]
        logo = response.css('.details-panel-item--name>img::attr(src)').extract()
        symbol = response.css('.details-panel-item--name>span::text').extract_first()
        symbol=symbol.replace('(','')
        symbol=symbol.replace(')','')
        price = response.css('#quote_price>span:nth-child(1)::text').extract_first().strip()
        priceBtc = response.css('.bottom-margin-1x>span.text-gray>span::text').extract_first().strip()

        priceNames = response.css('.bottom-margin-1x>span.text-gray::text').extract()
        priceNames = map(lambda s: s.strip(), priceNames)
        priceNames = list(filter(None, priceNames))

        priceValues = response.css('.bottom-margin-1x>span.text-gray>span::text').extract()
        priceValues = list(map(lambda s: s.strip(), priceValues))

        stats_detail = response.css('div.details-panel-item--marketcap-stats>div.coin-summary-item')
        market_cap=0
        volume_24h=0
        circulating_supply=0
        total_supply=0
        max_supply=0
        for stat in stats_detail:
            title = stat.css('.coin-summary-item-header::text').extract_first()
            if 'Market Cap' in title:
                market_cap=stat.css('span>span::text').extract_first().strip()
            if 'Volume (24h)' in title:
                volume_24h=stat.css('span>span::text').extract_first().strip()
            if 'Circulating Supply' in title:
                circulating_supply=stat.css('span::text').extract_first().strip()
            if 'Total Supply' in title:
                total_supply=stat.css('span::text').extract_first().strip()
            if 'Max Supply' in title:
                max_supply=stat.css('span::text').extract_first().strip()



        ul_details = response.css('ul.details-panel-item--links>li')
        explorer=[]
        tags=[]
        website=[]
        rank=None
        message_board=None
        chat=None
        source_cdoe=None
        tech_doc=None
        announcement=None
        for li in ul_details:
            title=li.css('span.details-list-item-icon::attr(title)').extract_first()
            if 'Rank' in title:
                rank=li.css('span::text').extract_first()
            if 'Website' in title:
                website.append(li.css('a::attr(href)').extract_first())
            if 'Explorer' in title:
                explorer.append(li.css('a::attr(href)').extract_first())
            if 'Message Board' in title:
                message_board=li.css('a::attr(href)').extract_first()
            if 'Chat' in title:
                chat=li.css('a::attr(href)').extract_first()
            if 'Source Code' in title:
                source_cdoe=li.css('a::attr(href)').extract_first()
            if 'Technical Documentation' in title:
                tech_doc=li.css('a::attr(href)').extract_first()
            if 'Tags' in title:
                tagSpan=li.css('span.label')
                for label in tagSpan:
                    tags.append(label.css('::text').extract_first())

            if 'Announcement' in title:
                announcement=li.css('a::attr(href)').extract_first()
        historygraph=scrapy.Request(url="https://graphs2.coinmarketcap.com/currencies/" + slug + "/", callback=self.singleCurrency,meta={'items': items})
        yield historygraph
        historydata = scrapy.Request(url="https://coinmarketcap.com/currencies/"+slug+"/historical-data/?start=20130428&end=20181205",
                                      callback=self.singleCurrencyHistoryData, meta={'items': items})
        yield historydata
        items.update({
            'name':name,
            'slug':slug,
            'image_url':logo,
            'symbol':symbol,
            'price':price,
            'priceBtc':priceBtc,
            'priceNames':priceNames,
            'priceValues':priceValues,
            'market_cap':market_cap,
            'volumne_24h':volume_24h,
            'circulating_supply':circulating_supply,
            'total_supply':total_supply,
            'max_supply':max_supply,
            'explorer':explorer,
            'tags':tags,
            'website':website,
            'rank':rank,
            'message_board':message_board,
            'chat':chat,
            'source_cdoe':source_cdoe,
            'tech_doc':tech_doc,
            'announcement':announcement
        })
        yield items

    def singleCurrency(self, response):
        items = response.meta['items']
        jsonresponse = json.loads(response.text)
        marketcapLists = jsonresponse['market_cap_by_available_supply']
        pricebtcList = jsonresponse['price_btc']
        priceusdLists = jsonresponse['price_usd']
        volumeusdLists = jsonresponse['volume_usd']
        historygraph = []
        for marketcap, pricebtc, priceusd, vloumeusd in zip(marketcapLists, pricebtcList, priceusdLists,
                                                            volumeusdLists):
            ts = marketcap[0] / 1000

            dt = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
            historygraph.append([marketcap[1], pricebtc[1], priceusd[1], vloumeusd[1], dt, ts])

        items.update({'historygraph':historygraph})
        return items


    def singleCurrencyHistoryData(self,response):
        items = response.meta['items']
        historyTable=response.css('table.table').xpath('//tbody/tr')
        historyData=[]
        for tr in historyTable:
            data = tr.css('td::text').extract()
            print('historyData',data)
            historyData.append(data)
        items.update({'historyData': historyData})
        return items


