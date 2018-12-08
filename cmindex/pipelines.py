# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy,csv
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
import mysql.connector
from mysql.connector import errorcode
from elasticsearch import Elasticsearch
from decimal import Decimal
import datetime


class CmindexPipeline(ImagesPipeline):




    def get_media_requests(self, item, info):

        for image_url in item['image_url']:
            yield scrapy.Request(image_url)

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        item['image_paths'] = image_paths
        return item






class MysqlPipline(object):

    def open_spider(self, spider):
        self.cnx = None
        self._es = None

    def process_item(self, item, spider):
        if 'priceNames' in item and 'priceValues' in item:
            if len(item['priceNames'])>1 and len(item['priceValues'])>1:
                self.csvWriter(item)


        lastID=self.insert_mysql(item)
        es_res=self.insert_es(item,lastID)
        self.insert_detail(item,lastID)
        if 'historygraph' in item:
            self.insert_historygraph(item,lastID)
        print(lastID,es_res)
        return item


    def connect_mysql(self):
        try:
            self.cnx = mysql.connector.connect(user='root', password='7436253',
                              host='192.168.1.82',
                              database='cmindex_scraping')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        return self.cnx

    def csvWriter(self,item):
        #csv_columns=list(item.keys())
        csv_columns=['name','priceNames','priceValues']
        csv_file = "MultiplePrices.csv"
        try:
            with open(csv_file, 'w',newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns,extrasaction='ignore')
                writer.writeheader()
                writer.writerow({"name":item['name'],"priceNames":item['priceNames'],"priceValues":item['priceValues']})
        except IOError:
            print("I/O error")


    def connect_elasticsearch(self):

        self._es = Elasticsearch([{'host': '192.168.1.82', 'port': 9200}])
        if self._es.ping():
            print('Yay Connect')
        else:
            print('Awww it could not connect!')
        return self._es

    def insert_historygraph(self,item,lastID):
        es = self.connect_elasticsearch();
        for historygraph in item['historygraph']:
            doc = {
                "market_cap_by_available_supply": historygraph[0],
                "price_btc": historygraph[1],
                "price_usd": historygraph[2],
                "volume_usd": historygraph[3],
                "currency_id": lastID,
                "timestamp": historygraph[5],
                "datestamp": historygraph[4]
            }
            res = es.index(index="live_price_single", doc_type='_doc', body=doc)





    def insert_detail(self,item,lastID):
        if len(item['explorer'])>0:
            cnx = self.connect_mysql()
            cr = cnx.cursor()
            for explorer in item['explorer']:
                detail_data_ex=(lastID,'Explorer',explorer,'')
                detail_query_ex=("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
                cr.execute(detail_query_ex, detail_data_ex)
                cnx.commit()
        if len(item['website'])>0:
            for website in item['website']:
                detail_data_web=(lastID,'Website',website,'')
                detail_query_web = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
                cr.execute(detail_query_web, detail_data_web)
                cnx.commit()

        if len(item['tags'])>0:
            for tag in item['tags']:
                detail_data_tag = (lastID, 'Tags', '', tag)
                detail_query_tag = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
                cr.execute(detail_query_tag, detail_data_tag)
                cnx.commit()
        if item['message_board'] !=None:
            detail_data_msg=(lastID, 'Message Board', item['message_board'], '')
            detail_query_msg = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
            cr.execute(detail_query_msg, detail_data_msg)
            cnx.commit()
        if item['chat'] !=None:
            detail_data_ch = (lastID, 'Chat', item['chat'], '')
            detail_query_ch = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
            cr.execute(detail_query_ch, detail_data_ch)
            cnx.commit()
        if item['source_cdoe'] !=None:
            detail_data_sc = (lastID, 'Source Code', item['source_cdoe'], '')
            detail_query_sc = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
            cr.execute(detail_query_sc, detail_data_sc)
            cnx.commit()
        if item['tech_doc'] !=None:
            detail_data_td = (lastID, 'Technical Documentation', item['tech_doc'], '')
            detail_query_td = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
            cr.execute(detail_query_td, detail_data_td)
            cnx.commit()
        if item['announcement'] !=None:
            detail_data_an = (lastID, 'Announcement', item['announcement'], '')
            detail_query_an = ("INSERT INTO `currency_details`(`cid`, `type`, `url`, `text`) VALUES (%s,%s,%s,%s)")
            cr.execute(detail_query_an, detail_data_an)
            cnx.commit()



    def insert_mysql(self,item):
        cnx = self.connect_mysql()
        cr = cnx.cursor()
        circulating_supply = Decimal(item['circulating_supply'].replace(',', '')) if item[
                                                                                         'circulating_supply'] != 0 else 0.0
        max_supply = Decimal(item['max_supply'].replace(',', '')) if item['max_supply'] != 0 else 0.0
        total_supply = Decimal(item['total_supply'].replace(',', '')) if item['total_supply'] != 0 else 0.0
        now = datetime.datetime.now()
        if "Coin" in item['tags']:
            if "Mineable" in item['tags']:

                currency_data = (
                item['name'], item['slug'], "Coin", item['symbol'], item['image_paths'][0], circulating_supply,
                max_supply, total_supply, 0.0, 1, item['rank'], 1, now, now)
            else:
                currency_data = (
                item['name'], item['slug'], "Coin", item['symbol'], item['image_paths'][0], circulating_supply,
                max_supply, total_supply, 0.0, 0, item['rank'], 1, now, now)
        elif "Token" in item['tags']:
            if "Mineable" in item['tags']:
                currency_data = (
                item['name'], item['slug'], "Token", item['symbol'], item['image_paths'][0], circulating_supply,
                max_supply, total_supply, 0.0, 1, item['rank'], 1, now, now)
            else:
                currency_data = (
                item['name'], item['slug'], "Coin", item['symbol'], item['image_paths'][0], circulating_supply,
                max_supply, total_supply, 0.0, 0, item['rank'], 1, now, now)
        currency = (
            "INSERT INTO `currencies`(`name`, `slug`, `currency_type`, `symbol`, `logo`, `circulating_supply`, `max_supply`, `total_supply`, `exchange_rate`, `mineable`, `ranking`, `status`, `created_at`, `updated_at`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        cr.execute(currency, currency_data)
        lastID = cr.lastrowid
        cnx.commit()
        return lastID
    def insert_es(self,item,lastID):
        es = self.connect_elasticsearch();
        ts = datetime.datetime.now().timestamp()
        currentDT = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nwmarket_cap = float(item['market_cap'].replace(',', ''))
        nwvolume_24h = float(item['volumne_24h'].replace(',', ''))

        doc = {
            "market_cap_by_available_supply": nwmarket_cap,
            "price_btc": float(item['priceBtc']),
            "price_usd": float(item['price']),
            "volume_usd": nwvolume_24h,
            "currency_id": lastID,
            "timestamp": ts,
            "datestamp": str(currentDT)
        }
        res = es.index(index="live_price_single", doc_type='_doc', body=doc)
        return  res['result']

    def __del__(self):
        self.cnx.close()




