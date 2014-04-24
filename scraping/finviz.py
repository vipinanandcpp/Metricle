import pandas as pd

def read_finviz_data(source_path):
	stocks_data = pd.read_csv(source_path) # loads data from finviz.com
	stocks_data.index = stocks_data.Ticker # reindexing the data frame
	del stocks_data['Ticker'] #removing redundant column
	del stocks_data['No.'] # removing redundant column
	return stocks_data #data frame from finviz.com 
	
def get_Tickers_From_Finviz(source_query_url):
	return read_finviz_data(source_query_url).index.tolist()