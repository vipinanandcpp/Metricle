import nltk
from nltk.corpus import stopwords
from nltk import word_tokenize, pos_tag, WordPunctTokenizer
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim import corpora
from gensim.models.tfidfmodel import TfidfModel
from operator import itemgetter
from collections import OrderedDict
import pandas as pd
import numpy as np
import re

def get_corpus_keywords(source_corpus_list,source_url_dict,is_stop_words_allowed,n_gram_min,n_gram_max,quantile,*pos_taggs):
    corpus_list = create_Normalized_corpus(source_corpus_list,source_url_dict,is_stop_words_allowed,*pos_taggs)
    dictionary = corpora.Dictionary(corpus_list)
    bow_corpus = [dictionary.doc2bow(doc) for doc in corpus_list] 
    ordered_dict = keyword_extractor_tfidf(corpus_list,is_stop_words_allowed,n_gram_min,n_gram_max)
    word_list_dataFrame = pd.DataFrame(data = [idf_score for idf_score in ordered_dict.itervalues()],index=ordered_dict.keys(),columns=['tfidf_score'])
    quantile_item = word_list_dataFrame.quantile(quantile)[0]
    #if source_url_dict != None:
    #    time.sleep(snooze)
    return word_list_dataFrame[word_list_dataFrame['tfidf_score'] >= quantile_item].index.tolist()

def keyword_extractor_tfidf(corpus_list,is_stop_words_allowed,n_gram_min,n_gram_max):
    if n_gram_min > n_gram_max:
        raise Exception('Invalid input n_gram_min should be <= n_gram_max')
    corpus = []
    for doc in corpus_list:
        text = ''
        for word in doc:
            text = text +' '+ word
        corpus.append(text)
    if is_stop_words_allowed == False:     
        vectorizer = TfidfVectorizer(ngram_range=(n_gram_min, n_gram_max),stop_words='english')
    else:
        vectorizer = TfidfVectorizer(ngram_range=(n_gram_min, n_gram_max))
    analyzer = vectorizer.build_analyzer()
    analyzer(corpus[0])
    features_array = vectorizer.fit_transform(corpus).toarray()
    features_transform_list = features_array.tolist()[0]
    features_dictionary = dict(zip(vectorizer.get_feature_names(),features_transform_list))
    sorted_features_dictionary = OrderedDict(sorted(features_dictionary.items(),key=itemgetter(1)))
    return sorted_features_dictionary  

def create_Normalized_corpus(source_corpus_list,corpus_source_url_dict,is_stop_words_allowed,*pos_filter_args):
    corpus_list = []
    if corpus_source_url_dict != None:
        for key in corpus_source_url_dict:
            source_txt = extract_url_text(key,corpus_source_url_dict[key])
            features_list = clean_document_return_features(source_txt,is_stop_words_allowed)
            features_list = find_features_from_POS(features_list,*pos_filter_args)
            corpus_list.append(features_list)
    if source_corpus_list != None:
        for source_txt in source_corpus_list:
            features_list = clean_document_return_features(source_txt,is_stop_words_allowed)
            features_list = find_features_from_POS(features_list,*pos_filter_args)
            corpus_list.append(features_list)
    return corpus_list

def clean_document_return_features(doc,is_stop_words_allowed):
    stop_set = set(stopwords.words('english'))
    #stemmer = nltk.PorterStemmer()
    tokens = WordPunctTokenizer().tokenize(doc)
    if is_stop_words_allowed == False:
        clean = [token.lower() for token in tokens if token.lower() not in stop_set and len(token) > 2]
    else:
        clean = [token.lower() for token in tokens if len(token) > 2]
    return clean    

def find_features_from_POS(features_list,*pos_args):
    pos = nltk.pos_tag(features_list)
    pos_filter_dict = {}
    if(len(pos_args) > 0):
        for arg in pos_args:
            pos_filter_dict[arg] = 1
        filtered_features_list = []
        for i in xrange(len(pos)):
            if pos_filter_dict.has_key(pos[i][1]):
                filtered_features_list.append(pos[i][0])
    else:
        return features_list
    return filtered_features_list

def create_tokens(text):
    return re.findall('[a-z]+',text.lower())

def create_ngrams(n_gram_min,n_gram_max,text,is_stop_words_allowed,*pos_filter_args):
    tokenized_string = find_features_from_POS(clean_document_return_features(text,is_stop_words_allowed),*pos_filter_args)
    if (n_gram_min <= 0):
        raise Exception('Invalid input') 
    n_grams_set = set()
    for j in xrange(0,len(tokenized_string)):
        for i in xrange(min(n_gram_min,len(tokenized_string)),min(n_gram_max+1,len(tokenized_string)+1)):
            n_grams_set.add(' '.join(tokenized_string[j:i+j]))
    return n_grams_set    

