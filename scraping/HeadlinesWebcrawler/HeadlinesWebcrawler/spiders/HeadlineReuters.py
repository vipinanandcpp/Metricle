from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy import log
from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from HeadlinesWebcrawler.items import HeadlineswebcrawlerItem
from pyquery import PyQuery as pq
from dateutil import parser
import numpy as np
import datetime, pytz
import requests
import justext
import re

date_format = "%a %b %d %H:%M:%S %Y"
eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')

d = utc.localize(datetime.datetime.utcnow()).astimezone(eastern)

class HeadlineReuters(CrawlSpider):

	name = 'reutersdomain'
	allowed_domains = ['reuters.com']
	start_urls = ['http://www.reuters.com/news/archive/healthcareSector']
	rules = (Rule(SgmlLinkExtractor(allow=('?date=[0-9]+', )), follow=True,callback='parse_item'),)

	def __init__(self, name=None, **kwargs):
		super(HeadlineReuters, self).__init__(name, **kwargs)
		self.headline_list = []
		self.abstract_list = []
		self.url_list = []
		self.date_list = []
		self.author_name_list = []
		self.record_addition_date_list = []

	def parse_item(self,response):
		self.log('A response from %s just arrived!' % response.url)
		self.headline_list = []
		self.abstract_list = []
		self.url_list = []
		self.date_list = []
		self.author_name_list = []
		self.record_addition_date_list = []
		response = requests.get(response.url)
		


		