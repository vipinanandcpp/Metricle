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


class HeadlineFierceBio(CrawlSpider):
	name = 'fiercedomain'
	allowed_domains = ['fiercebiotech.com','fiercebiotechresearch.com','fiercebiotechit.com','fiercediagnostics.com','fiercedrugdelivery.com','fiercemedicaldevices.com','fiercepharma.com','fiercepharmamanufacturing.com','fiercevaccines.com','fiercehealthcare.com']
	start_urls = ['http://www.fiercebiotech.com/news?page=0','http://www.fiercebiotechresearch.com/news?page=0','http://www.fiercebiotechit.com/news?page=0','http://www.fiercediagnostics.com/news?page=0','http://www.fiercedrugdelivery.com/news?page=0','http://www.fiercemedicaldevices.com/news?page=0','http://www.fiercepharma.com/news?page=0','http://www.fiercepharmamanufacturing.com/news?page=0','http://www.fiercevaccines.com/news?page=0','http://www.fiercehealthcare.com/news?page=0']
	rules = (Rule(SgmlLinkExtractor(allow=('news\?page=[0-9]+', )), follow=True,callback='parse_item'),)

	def __init__(self, name=None, **kwargs):
		super(HeadlineFierceBio, self).__init__(name, **kwargs)
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
		source_url = response.url[0:re.search('/news',response.url).start()]
		pq_obj = pq(response.content)
		pq_obj = pq_obj('.panel-pane.pane-page-content')
		item = HeadlineswebcrawlerItem()
		for idx in xrange(0,len(pq_obj('.view-content').children('div'))):
			for child in pq_obj('.view-content').children('div').eq(idx).children():
					pq_child_obj = pq(child)
					try:
						self.headline_list.append(pq_child_obj.children('h2').text())
					except Exception as e:
						print e
						self.headline_list.append(None)
					try:
						self.url_list.append(source_url+''+pq_child_obj.children('h2').children('a').attr('href'))
					except Exception as e:
						print e
						self.url_list.append(None)
					try:
						self.author_name_list.append(pq_child_obj.children('div').children('div').children('a').text())		
					except Exception as e:
						print e
						self.author_name_list.append(None)
					try:	
						self.abstract_list.append(pq_child_obj.children('div').children('p').text())
					except Exception as e:
						print e
						self.abstract_list.append(None)
					try:
						date_string =  pq_child_obj.children('div').children('div').text()[0:re.search(',',pq_child_obj.children('div').children('div').text()).start()+1]
						date_string =  date_string + pq_child_obj.children('div').children('div').text()[re.search(',',pq_child_obj.children('div').children('div').text()).start()+1:re.search(',',pq_child_obj.children('div').children('div').text()).start()+6]#[0:-4]
						date_time = datetime.datetime.strptime(date_string,'%B %d, %Y')
						d = parser.parse(date_time.strftime('%Y-%m-%d %H:%M:%S'))
						if d.tzinfo is None:
							d = utc.localize(d)
						self.date_list.append(d)
					except Exception as e:
						print e
						self.date_list.append(None)
					self.record_addition_date_list.append(utc.localize(datetime.datetime.utcnow()).astimezone(eastern))
		item['newsHeadline'] = self.headline_list
		item['newsAbstract'] = self.abstract_list
		item['newsHeadlineDate'] = self.date_list
		item['newsHeadlineAuthor'] = self.author_name_list
		item['newsHeadline_url'] = self.url_list
		item['recordAdditionDate'] = self.record_addition_date_list
		return item		