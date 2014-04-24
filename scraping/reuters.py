import pandas as pd
import numpy as np
import re
from pyquery import PyQuery as pq
import datetime
import time,os,imp

root_directory = os.path.abspath(os.getcwd()+'\..')

def extract_company_description_reuters(ticker):
	source_url = 'http://www.reuters.com/finance/stocks/companyProfile?symbol='+ticker
	source_txt = ''
	pquery_object = pq(source_url)
	for txt in pquery_object('.moduleBody').children('p').items():
		source_txt = source_txt + txt.text()
	return source_txt

def extract_reuters_key_developments(ticker,date_ = None,snooze=1):
	import os
	i = 1
	headlines_list = []
	time_stamp_list = []
	abstract_list  = []
	anchor_list = []
	headline_text = ''
	abstract_text = ''
	anchor_text = ''
	time_stamp_text = ''

	while True:
		try:
			source_url = 'http://www.reuters.com/'
			pq_obj = pq(source_url+'/finance/stocks/'+ticker+'/key-developments?pn='+str(i))
			pq_ = pq_obj('.feature')
			if len(pq_obj('.pageNavigation')) == 0:
				break
			for item in pq_.items():
				headline_text = item.children('h2').text()
				if os.name == 'nt':
					date_time = datetime.datetime.strptime(item.children('h2').children('.timestamp').text()[:-4],'%A, %d %b %Y %I:%M%p')
				else:
					date_time = datetime.datetime.strptime(item.children('h2').children('.timestamp').text(),'%A, %d %b %Y %I:%M%p %Z')
				time_stamp_text = date_time.strftime('%Y-%m-%d %H:%M')
				abstract_text = item.children('p').text()
				anchor_text = source_url+item.children('h2').children('a').attr('href')
				if date_ != None:
					if (( (date_time.date() == (datetime.datetime.strptime(date_,'%Y-%m-%d') - datetime.timedelta(days=1)).date() and date_time.strftime('%H:%M') >= '16:00') or date_time.date() == datetime.datetime.strptime(date_,'%Y-%m-%d').date())):
						headlines_list.append(headline_text)
						abstract_list.append(abstract_text)
						time_stamp_list.append(time_stamp_text)
						anchor_list.append(anchor_text)
						
					elif date_time.date() < datetime.datetime.strptime(date_,'%Y-%m-%d').date():
						return headlines_list,abstract_list,time_stamp_list,anchor_list,n_grams_list
				else:
					headlines_list.append(headline_text)
					abstract_list.append(abstract_text)
					time_stamp_list.append(time_stamp_text)
					anchor_list.append(anchor_text)
					
		except Exception as e:
			pass
		finally:
			i = i + 1
			time.sleep(snooze)
	return headlines_list,abstract_list,time_stamp_list,anchor_list

def extract_reuters_news_headline(ticker,date_,snooze=1):
	import os
	headlines_list = []
	time_stamp_list = []
	abstract_list  = []
	anchor_list = []
	date_time = datetime.datetime.strptime(date_,'%Y-%m-%d')
	date_string = date_time.strftime('%m%d%Y')
	try:
		source_url = 'http://www.reuters.com/'
		pq_obj = pq(source_url+'finance/stocks/companyNews?symbol='+ticker+'&date='+date_string)
		pq_ = pq_obj('.feature')
		for item in pq_.items():
			headlines_list.append(item.children('h2').text())
			abstract_list.append(item.children('p').text())
			anchor_list.append(source_url+item.children('h2').children('a').attr('href'))
			pq_obj = pq(source_url+item.children('h2').children('a').attr('href'))
			if os.name == 'nt':
				date_time = datetime.datetime.strptime(pq_obj('.timestamp')[0].text[:-4],'%a %b %d, %Y %I:%M%p')
			else:
				date_time = datetime.datetime.strptime(pq_obj('.timestamp')[0].text,'%a %b %d, %Y %I:%M%p %Z')
			time_stamp_list.append(date_time.strftime('%Y-%m-%d %H:%M'))
			
	except Exception as e:
		pass
	return headlines_list,abstract_list,time_stamp_list,anchor_list 		

# extracts the headlines and keywords from Reuters. For effective working of this function, 
# please make sure the data frame must include atleast 2 columns --> date and ticker 
def extract_headlines_data_reuters(extreme_events_data_frame,columns_dict = {'date':'date','ticker':'symbol'}):
	time_stamp_list_list = []
	headlines_list_list  = []
	abstract_list_list   = []
	anchor_list_list = []
	extreme_events_data_frame.index = extreme_events_data_frame[columns_dict['date']]
	i = 0
	for date_ in extreme_events_data_frame[columns_dict['date']]:
		stock_ticker = extreme_events_data_frame[columns_dict['ticker']].ix[i]
		headlines_list,abstract_list,time_stamp_list,anchor_list = extract_reuters_key_developments(stock_ticker,n_grams_min,n_grams_max,str(date_))
		headlines_list_news,abstract_list_news,time_stamp_list_news,anchor_list_news = extract_reuters_news_headline(stock_ticker,n_grams_min,n_grams_max,str(date_))
		headlines_list.extend(headlines_list_news)
		abstract_list.extend(abstract_list_news)
		time_stamp_list.extend(time_stamp_list_news)
		anchor_list.extend(anchor_list_news)
		headlines_list_list.append(headlines_list)
		time_stamp_list_list.append(time_stamp_list)
		abstract_list_list.append(abstract_list)
		anchor_list_list.append(anchor_list)
		i = i+1
	extreme_events_data_frame['timestamps'] = time_stamp_list_list
	extreme_events_data_frame['headlines'] = headlines_list_list
	extreme_events_data_frame['abstracts'] = abstract_list_list
	extreme_events_data_frame['articles_urls'] = anchor_list_list
	
	return extreme_events_data_frame#.to_json(ticker+'.json')

def get_reuters_news_headlines(function_pointer,*args):
	#from nlp import nltk_metricle
	import finviz
	nltk_metricle = imp.load_source('nltk_metricle',os.path.join(root_directory,'nlp','nltk_metricle.py'))
	firm_name = finviz.read_finviz_data('http://finviz.com/export.ashx?v=111&t='+args[0])['Company'][0]
	tokenized_firm_name_set = set(nltk_metricle.create_tokens(firm_name))
	headlines_list,abstract_list,time_stamp_list,anchor_list = function_pointer(*args)
	tokens_list_set = [set(nltk_metricle.find_features_from_POS(nltk_metricle.clean_document_return_features(headline,False),'NN','NNS','NNP','JJ','VB')) for headline in headlines_list]
	max_ = 1
	headlines_clean_list = []
	abstract_clean_list  = []
	timestamp_clean_list = []
	keywords_list_list = []
	for i in xrange(0,len(tokens_list_set)-1):
		flag_1 = False
		for j in xrange(i+1,len(tokens_list_set)):
			if (len(tokens_list_set[i].intersection(tokens_list_set[j])) >= max_) and (len(tokenized_firm_name_set.intersection(tokens_list_set[j])) > 0):
				max_ = len(tokens_list_set[i].intersection(tokens_list_set[j]))
				headlines_clean_list.append(headlines_list[j])
				abstract_clean_list.append(abstract_list[j])
				timestamp_clean_list.append(time_stamp_list[j])
				keywords_list_list.append(nltk_metricle.find_features_from_POS(list(tokens_list_set[j])))
				flag_1 = True
		if flag_1==True:
			headlines_clean_list.append(headlines_list[i])
			abstract_clean_list.append(abstract_list[i])
			timestamp_clean_list.append(time_stamp_list[i])
			keywords_list_list.append(nltk_metricle.find_features_from_POS(list(tokens_list_set[i])))
	
	df = pd.DataFrame(columns=['ticker','date','headlines','abstract','timestamp'], index=[x for x in xrange(0,len(headlines_clean_list))])				
	df['ticker'] = args[0] #ticker
	df['date'] = args[1] #Date
	df['timestamp'] = timestamp_clean_list
	df['headlines'] = headlines_clean_list
	df['abstract'] = abstract_clean_list
	df['keywords'] = keywords_list_list
	return df

def get_reuters_symbol_headline_events(ticker,date_='2013-12-23',snooze=1):
	df_news = get_reuters_news_headlines(extract_reuters_news_headline,ticker,date_,snooze)
	df_key_developments = get_reuters_news_headlines(extract_reuters_key_developments,ticker,date_,snooze)
	return df_news.append(df_key_developments)