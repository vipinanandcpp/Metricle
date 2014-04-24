# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html
import re,os, time, datetime, pytz
from dateutil import parser
import pandas as pd
import numpy as np
import time

date_format = "%a %b %d %H:%M:%S %Y"
eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')

d = utc.localize(datetime.datetime.utcnow()).astimezone(eastern)

working_directory = os.path.abspath(os.path.dirname(__file__))
data_root_directory = os.path.join(working_directory,'metricle','data','daily', d.date().strftime('%Y%m%d'))
data_directory = None

headline_list = []
abstract_list = []
date_list = []
author_list = []
url_list = []
insertion_date_list = [] 

class HeadlineswebcrawlerPipeline(object):
	def process_item(self, item, spider):
		global data_directory,headline_list,abstract_list,date_list,author_list,url_list,insertion_date_list
		data_directory = os.path.join(working_directory,'metricle','data','daily',d.date().strftime('%Y%m%d'),spider.name)
		if not os.path.exists(data_directory):
			os.makedirs(data_directory)	
		headline_list.extend(item['newsHeadline'])
		abstract_list.extend(item['newsAbstract'])
		date_list.extend(item['newsHeadlineDate'])
		author_list.extend(item['newsHeadlineAuthor'])
		insertion_date_list.extend(item['recordAdditionDate'])
		url_list.extend(item['newsHeadline_url'])
		time.sleep(0)
			
	def close_spider(self,spider):
		global headline_list,abstract_list,date_list,author_list,url_list,insertion_date_list
		col = ['created_at','insertion_date','headline','abstract','author','URL']
		glbl_df = pd.DataFrame(columns = col,index = [idx for idx in xrange(0,len(headline_list))])
		glbl_df['created_at'] = date_list
		glbl_df['insertion_date'] = insertion_date_list
		glbl_df['headline'] = headline_list
		glbl_df['abstract'] = abstract_list
		glbl_df['author'] = author_list
		glbl_df['URL'] = url_list
		glbl_df.to_json(os.path.join(data_directory,spider.name+'.json'))
		