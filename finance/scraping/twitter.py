from pyquery import PyQuery as pq
import urllib2
import requests
import time


# gets twitter screen names for firms
def get_twitter_screen_name(entity_twitter_url):
    twitter_screen_name = entity_twitter_url[re.search('.com/',entity_twitter_url).start()+len('.com/'):]
    #pq_object_new = pq(url=entity_twitter_url)
    #return [tmp.text()[re.search('@',tmp.text()).start():].replace(" ","") for tmp in pq_object_new('a').items() if '@' in tmp.text() and len(tmp.text()[re.search('@',tmp.text()).start()+1:]) > 0][0]
    twitter_screen_name = str('@'+twitter_screen_name)
    return twitter_screen_name