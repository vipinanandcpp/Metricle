from pyquery import PyQuery as pq
from bson import json_util
import bz2
import pandas as pd
import pandas.io.data as web
import numpy as np
import cPickle as pickle
import gzip
import requests
import pyquery
import re,os, time, datetime, pytz
from dateutil import parser



date_format = "%a %b %d %H:%M:%S %Y"
eastern = pytz.timezone('US/Eastern')
utc = pytz.timezone('UTC')

d = utc.localize(datetime.datetime.utcnow()).astimezone(eastern)

working_directory = os.path.abspath(os.path.dirname(__file__))
data_directory = os.path.join(working_directory, 'data', d.date().strftime('%Y%m%d'), 'document_archive')
data_root_directory = os.path.join(working_directory, 'data', d.date().strftime('%Y%m%d'))

if not os.path.exists(data_directory):
	os.makedirs(data_directory)

def get_Dow_Jones_Article_response_object(guid):
	url = 'http://betawebapi.dowjones.com/fintech/articles/api/v1/guid/'+str(guid)
	response = requests.get(url)
	return response

def get_Dow_Jones_Article_external_url(response_object):
    for item in pq(response_object.content).children():
        if item.tag == 'LOCATION':
            for child in item.getchildren():
                for key in child.keys():
                    if(key == 'URL'):
                        return child.get(key)
    return np.NaN

def get_Dow_Jones_Article_abstract(response_object):
	for item in pq(response_object.content).children():
		if item.tag == 'ABSTRACT':
			for child in item.getchildren():
				if child.tag == 'PARAGRAPH':
					return child.text
	return np.NaN

def download_Dow_Jones_article_information(ticker):
	with gzip.open(os.path.join(data_root_directory,'headlines_'+ticker+'.json.gz')) as f:
		json_data = json_util.loads(f.read())
	article_data_frame = pd.DataFrame(index = xrange(len(json_data)))
	timestamp_list = []
	headline_list = []
	GUID_list = []
	article_external_url_list = []
	article_abstract_list = []
	for result in json_data:
		try:
			timestamp_list.append(result['CreateTimestamp']['Value'])
			headline_list.append(result['Headline'])
			GUID_list.append(result['GUID'])
			response_object = get_Dow_Jones_Article_response_object(result['GUID'])
			article_external_url_list.append(get_Dow_Jones_Article_external_url(response_object))
			article_abstract_list.append(get_Dow_Jones_Article_abstract(response_object))
			
		except Exception as e:
			print e
	article_data_frame['create_timestamp'] = timestamp_list
	article_data_frame['headline'] = headline_list  
	article_data_frame['GUID'] = GUID_list
	article_data_frame['external_url'] = article_external_url_list
	article_data_frame['abstract'] = article_abstract_list
	fo = gzip.GzipFile(os.path.join(data_directory,ticker+'.json.gz'), 'wb')
	fo.write(pickle.dumps(article_data_frame.to_dict()))
	fo.close()
	return article_data_frame

def get_Dow_Jones_Ticker_Headlines_Data_Frame(ticker,date_input=d.date().strftime('%Y%m%d'),start_date=None,end_date=None,start_time=None,end_time=None):
	tmp_directory = os.path.join(working_directory, 'data',date_input,'document_archive')
	try:
		f = gzip.GzipFile(os.path.join(tmp_directory,ticker+'.json.gz'))
		data_buffer = ''
		while True:
			data = f.read()
			if data == "":
				break
			data_buffer += data
		data_dict = pickle.loads(data_buffer)
		headlines_frame_columns = data_dict.keys()	
		headlines_frame = pd.DataFrame(columns=headlines_frame_columns,index=xrange(0,len(data_dict['headline'].keys())))
		for column in headlines_frame_columns:
			headlines_frame[column] = map(lambda x:data_dict[column][x],data_dict[column].keys())  					
	except Exception as e:
		print e
		return None
	if start_date != None and end_date != None:
		headlines_frame['date'] = map(lambda x: datetime.datetime.strptime(x[:re.search('T',x).start()],'%Y-%m-%d'),headlines_frame['create_timestamp'])
		headlines_frame['time'] = map(lambda x: x[re.search('T',x).start()+1:],headlines_frame['create_timestamp'])
		if start_time != None and end_time != None:
			headlines_frame = headlines_frame[(headlines_frame['date'] >= start_date) & (headlines_frame['date'] <= end_date) & (headlines_frame['time'] >= start_time) & (headlines_frame['time'] <= end_time)]
		else:
			headlines_frame = headlines_frame[(headlines_frame['date'] >= start_date) & (headlines_frame['date'] <= end_date)]
		del headlines_frame['date']
		del headlines_frame['time']
	return headlines_frame

def get_extreme_tail_events_results_yahoo(ticker,left_quantile,right_quantile,date_input=d.date().strftime('%Y%m%d'),start_date=None,end_date=None):
	ticker_headlines_data_frame = get_Dow_Jones_Ticker_Headlines_Data_Frame(ticker,date_input)
	if start_date == None or end_date == None:
		max_date = ticker_headlines_data_frame['create_timestamp'][0][:re.search('T',ticker_headlines_data_frame['create_timestamp'][0]).start()]
		min_date = ticker_headlines_data_frame['create_timestamp'][len(ticker_headlines_data_frame['create_timestamp'])-1][:re.search('T',ticker_headlines_data_frame['create_timestamp'][len(ticker_headlines_data_frame['create_timestamp'])-1]).start()]
	else:
		min_date = start_date
		max_date = end_date
	yahoo_data = web.get_data_yahoo(ticker,min_date,max_date)
	yahoo_data['log_return'] = np.log(yahoo_data['Close']/(yahoo_data.shift())['Close'])*100
	left_quantile_data = yahoo_data['log_return'].quantile(left_quantile)
	right_quantile_data = yahoo_data['log_return'].quantile(right_quantile)
	return yahoo_data[(yahoo_data['log_return'] <= left_quantile_data) | (yahoo_data['log_return'] >= right_quantile_data)] 

def get_extreme_events_headlines_data_frame(extreme_events_historical_price_data_frame,ticker,date_input=d.date().strftime('%Y%m%d'),start_date=None,end_date=None,start_time=None,end_time=None):
	headlines_data_frame = get_Dow_Jones_Ticker_Headlines_Data_Frame(ticker,date_input,start_date,end_date,start_time,end_time)
	headlines_data_frame['date'] = map(lambda x: datetime.datetime.strptime(x[:re.search('T',x).start()],'%Y-%m-%d'),headlines_data_frame['create_timestamp'])
	headlines_data_frame['time'] = map(lambda x: x[re.search('T',x).start()+1:],headlines_data_frame['create_timestamp'])
	headlines_data_frame['date'] = map(lambda x,y: x + datetime.timedelta(days=1) if y >= '16:00:00' else x,headlines_data_frame['date'],headlines_data_frame['time']) 
	headlines_data_frame = headlines_data_frame[headlines_data_frame['date'].isin(extreme_events_historical_price_data_frame.index.tolist())]
	headlines_data_frame['log_returns'] = map(lambda x: extreme_events_historical_price_data_frame['log_return'].ix[x],headlines_data_frame['date'])
	del headlines_data_frame['date']
	del headlines_data_frame['time']
	return headlines_data_frame

def analyze_extreme_events_headlines(extreme_events_headlines_data_frame,headlines_data_frame_column,is_single_doc_corpus,is_stop_words_allowed,min_ngram,max_ngram,doc_features_quantile,*pos_filter_args):
	import imp
	metricle_nltk = imp.load_source('metricle_utility_package',os.path.abspath('../metricle_utility_package/metricle_nltk_utilities.py'))
	valid_columns_list = ['headline','abstract']
	is_valid = False
	for column in valid_columns_list:
		if column == headlines_data_frame_column:
			is_valid = True
			break
	if is_valid == False:
		raise Exception('Column values can only be either headline or abstract')
	column_data_list = extreme_events_headlines_data_frame[headlines_data_frame_column].tolist()
	tfidf_scored_featured_list = []
	if is_single_doc_corpus == True:
		for data in column_data_list:
		   try:
				tfidf_scored_featured_list.append(metricle_nltk.get_corpus_keywords([data],None,is_stop_words_allowed,min_ngram,max_ngram,doc_features_quantile,*pos_filter_args))
		   except Exception as e:
				#print e
				tfidf_scored_featured_list.append(np.NaN)
				continue
	else:
		   try:
				tfidf_scored_featured_list.append(metricle_nltk.get_corpus_keywords(column_data_list,None,is_stop_words_allowed,min_ngram,max_ngram,doc_features_quantile,*pos_filter_args))
		   except Exception as e:
				tfidf_scored_featured_list.append(np.NaN)
				#print e
	extreme_events_headlines_data_frame['key_ngrams_list'] = map(lambda x: x,tfidf_scored_featured_list)
	return extreme_events_headlines_data_frame

def generate_ngram_buckets(headlines_data_frame,minimum_percent,maximum_percent):
	max_bin_data_frame = headlines_data_frame[headlines_data_frame['log_returns'] >= maximum_percent]
	min_bin_data_frame = headlines_data_frame[headlines_data_frame['log_returns'] <= minimum_percent]
	mid_bin_data_frame = headlines_data_frame[(headlines_data_frame['log_returns'] < maximum_percent) & (headlines_data_frame['log_returns'] > minimum_percent)]
	max_bin_dict = get_words_bin_dict(max_bin_data_frame)
	min_bin_dict = get_words_bin_dict(min_bin_data_frame)
	mid_bin_dict = get_words_bin_dict(mid_bin_data_frame)
	max_bin_data_frame['key_ngrams_list'] = map(lambda x:delete_duplicate_keys(x,max_bin_dict,min_bin_dict,mid_bin_dict),max_bin_data_frame['key_ngrams_list'])
	min_bin_data_frame['key_ngrams_list'] = map(lambda x:delete_duplicate_keys(x,min_bin_dict,max_bin_dict,mid_bin_dict),min_bin_data_frame['key_ngrams_list'])
	mid_bin_data_frame['key_ngrams_list'] = map(lambda x:delete_duplicate_keys(x,mid_bin_dict,max_bin_dict,min_bin_dict),mid_bin_data_frame['key_ngrams_list'])
		
	return max_bin_data_frame,min_bin_data_frame,mid_bin_data_frame,max_bin_dict,min_bin_dict,mid_bin_dict



def delete_duplicate_keys(source_list,source_dict,*target_dicts):
	filtered_list = []
	try:
		for word in source_list:
			try:
				for dictionary in target_dicts:
					if dictionary.has_key(word):
						#if source_dict[word] > dictionary[word]:
							#filtered_list.append(word)
						del dictionary[word]
						del source_dict[word]
						#else:
							#del source_dict[word]    
					else:
						filtered_list.append(word)                        
			except Exception as e:
				print e
	except Exception as e:
		print e
		return None
	return filtered_list

def get_words_bin_dict(data_frame):
	words_bin_dict = {}
	for word_list in data_frame['key_ngrams_list']:
		try:
			for word in word_list:
				if words_bin_dict.has_key(word):
					words_bin_dict[word] = words_bin_dict[word] + 1
				else:
					words_bin_dict[word] = 1
		except Exception as e:
			print e
	return words_bin_dict