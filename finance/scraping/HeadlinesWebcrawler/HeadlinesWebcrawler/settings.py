# Scrapy settings for HeadlinesWebcrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'HeadlinesWebcrawler'

SPIDER_MODULES = ['HeadlinesWebcrawler.spiders']
NEWSPIDER_MODULE = 'HeadlinesWebcrawler.spiders'
ITEM_PIPELINES = {'HeadlinesWebcrawler.pipelines.HeadlineswebcrawlerPipeline'}
DEFAULT_ITEM_CLASS = 'HeadlinesWebcrawler.items.HeadlineswebcrawlerItem'
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'HeadlinesWebcrawler (+http://www.yourdomain.com)'
