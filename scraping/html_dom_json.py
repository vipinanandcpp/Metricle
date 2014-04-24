from pyquery import PyQuery as pq
from bson import json_util
import urllib2
import requests
import time

def extract_url_text(key,value,snooze=3):
    pqobj = pq(key)
    if len(value) == 1:
        source_txt = pqobj(value[0]).text()
    else:
        source_txt = ''
        pq_obj = pqobj(value[0])
        for val in value[1:]:
            pq_obj = pq_obj.children(val)
        for txt in pq_obj.items():
            source_txt = source_txt + txt.text()
    time.sleep(snooze)
    return source_txt

def read_json_file(source_path):
    with open(source_path) as json_file:
        json_data = json.load(json_file)
    return json_data

def create_HTTP_Request(url): # creates HTTP Request
    r = urllib2.Request(url)
    return r

def create_request_header(request_object): #Creates request header 
    request_object.add_header("User-Agent","Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0")
    request_object.add_header("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")