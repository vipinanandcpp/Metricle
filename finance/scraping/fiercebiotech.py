import pandas as pd
import numpy as np
from pyquery import PyQuery as pq
import re
import time
import datetime


def extract_historic_fierce_biotech_headlines(snooze=1):
	headline_list = []
	abstract_list = []
	url_list = []
	date_list = []
	author_name_list = []
	author_twitter_id_list = []
	source_url = 'http://www.fiercebiotech.com'
	for page_number in xrange(0,830):
		pq_object = pq(source_url+'/news?page='+str(page_number))
		pq_object = pq_object('.panel-pane.pane-page-content')
		for idx in xrange(0,len(pq_object('.view-content').children('div'))):
			try:
				for child in pq_object('.view-content').children('div').eq(idx).children():
					pq_child_obj = pq(child)
					headline_list.append(pq_child_obj.children('h2').text())
					url_list.append(source_url+''+pq_child_obj.children('h2').children('a').attr('href'))
					author_name_list.append(pq_child_obj.children('div').children('div').children('a').text())		
					abstract_list.append(pq_child_obj.children('div').children('p').text())
					date_string =  pq_child_obj.children('div').children('div').text()[0:re.search(',',pq_child_obj.children('div').children('div').text()).start()+1]
					date_string =  date_string + pq_child_obj.children('div').children('div').text()[re.search(',',pq_child_obj.children('div').children('div').text()).start()+1:re.search(',',pq_child_obj.children('div').children('div').text()).start()+6]#[0:-4]
					date_time = datetime.datetime.strptime(date_string,'%B %d, %Y')
					date_list.append(date_time.strftime('%Y-%m-%d'))
			except Exception as e:
				print e
				pass
				#pq_child_obj = pq(source_url+''+pq_child_obj.children('div').children('div').children('a').attr('href'))
				#author_twitter_id_list.append(pq_child_obj('.panel-pane.pane-page-content').text()[re.search('follow @',pq_child_obj('.panel-pane.pane-page-content').text()).start()+len('follow @')-1:re.search('on Twitter',pq_child_obj('.panel-pane.pane-page-content').text()).start()])
	df = pd.DataFrame(columns=['date','headline','abstract','author','url'],index=[x for x in xrange(0,len(headline_list))])
	df['date'] = date_list
	df['headline'] = headline_list
	df['abstract'] = abstract_list
	df['author'] = author_name_list
	df['url'] = url_list				
	return df.to_json('fiercebiotech.json')	
		
	