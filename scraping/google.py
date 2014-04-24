import pandas as pd
import numpy as np
import re
from pyquery import PyQuery as pq
import datetime
import time

# Extracts related firms for the target company
def get_related_firms_google_finance(source_url,ticker,snooze=3):
	print source_url
	request_object = requests.get(url = source_url)
	request_content = request_object.content
	filtered_related_string = request_content[re.search('streaming:',request_content).start() + len('streaming:'):re.search('],',request_content).start()]
	filtered_related_string = filtered_related_string + ']'
	related_symbols_list = []
	filtered_list = filtered_related_string.split('},')
	for filtered_str in filtered_list:
		target_str = filtered_str[re.search('s:"',filtered_str).start()+len('s:"'):re.search('",e',filtered_str).start()]
		if (target_str != ticker):
			related_symbols_list.append(target_str)
	time.sleep(snooze)
	return related_symbols_list

#Gets exchange name where the stock is traded.
def get_ticker_exchange_google_finance(ticker,snooze=3):
	source_url = 'https://www.google.com/finance?q=:'+ticker
	print source_url
	request_object = requests.get(url = source_url)
	request_content = request_object.content
	filtered_related_string = request_content[re.search('streaming:',request_content).start() + len('streaming:'):re.search('],',request_content).start()]
	filtered_related_string = filtered_related_string + ']'
	filtered_list = filtered_related_string.split('},')
	filtered_str = filtered_list[0]
	time.sleep(snooze)
	return filtered_str[re.search('e:"',filtered_str).start()+len('e:"'):][:-1]

def extract_company_description_google(ticker):
	source_url = 'https://www.google.com/finance?q=:'+ticker 
	pquery_object = pq(source_url)
	return pquery_object('div.companySummary').text()

# searches Google for facebook or twitter accounts of companies
def search_google_engine(search_key,website='google.com',snooze=3):
    search_url = 'http://www.google.com/search?q=%3A'+website+' '+search_key
    print search_url
    pq_object = pq(url=search_url)
    time.sleep(snooze)
    return [txt.text() for txt in pq_object('cite').items()]