import time
import dowjones as dj
import dowjones_utilities as dju


def run_dowjones_headlines_extractor(tickers,snooze=10):
	dj.runner(tickers,snooze)
	for ticker in tickers:
		try:
			dju.download_Dow_Jones_article_information(ticker)
		except Exception as   e:
			print e
		finally:
			time.sleep(snooze)	

