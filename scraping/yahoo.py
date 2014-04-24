import pandas as pd
import numpy as np
import re
from pyquery import PyQuery as pq
import datetime
import time

#extracts CEOs/CFOs from Yahoo Finance
def extract_key_people_yahoo_finance(ticker,snooze=3):
	source_url = 'http://finance.yahoo.com/q/pr?s='+ticker
	print source_url
	pqobj = pq(source_url)
	time.sleep(snooze)
	return [txt.text() for txt in pqobj('.yfnc_tabledata1').children('b').items()]

def extract_company_description_yahoo(ticker):
	source_url = 'http://finance.yahoo.com/q/pr?s='+ticker
	pquery_object = pq(source_url)
	return pquery_object('.yfnc_modtitlew1').children('p').text()
