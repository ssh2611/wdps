from sklearn.feature_extraction.text import TfidfVectorizer
import gzip
import sys
from warcRecord import WarcRecord
from nltk import ne_chunk, pos_tag, word_tokenize, tokenize, sent_tokenize
from nltk.tree import Tree
from nltk.corpus import stopwords
from collections import defaultdict
import re
from elasticSearcher import ElasticSearcher
from sparqlSearcher import SparqlSearcher
from bs4 import BeautifulSoup
from bs4.element import Comment
import spacy
from collections import Counter
import en_core_web_md
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import logging


KEYNAME = "WARC-TREC-ID"

sparql = SparqlSearcher(
    "node059:9090",
    1,
)
es = ElasticSearcher(
    "node001:9200",
    5
)


input_lines = None;

def split_records():
    payload = ''
    for line in input_lines:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload


def process_page(row):
    nlp = en_core_web_md.load()
    warc_record = WarcRecord(row)
    warc_payload = warc_record.payload
    if warc_record.broken or not warc_payload or warc_payload is None or warc_record.id is None:
        return (yield "")
    
    ents = []
    soup = None
    try:
        soup = BeautifulSoup(warc_payload, "html.parser")
    except:
        raise Exception("SOUP FAILED:  %s %s\n" % (warc_payload, warc_record.id) )
    for script in soup(['style', 'script', 'head', 'title', 'meta', '[document]', 'code' 'blockquote', 'cite']):
        script.extract()
    text = " ".join(re.findall(r'\w+',  soup.get_text()))
    article = nlp(text)

    raw_words = []
    for ent in article.ents:
        if(ent.label_ not in ["CARDINAL", "DATE", "QUANTITY", "TIME", "ORDINAL", "MONEY", "PERCENT", "QUANTITY"]) and len(ent.text.split(" ")) < 4:
            raw_words.append(ent.text)
            ents.append(ent)

    processed_results = {}
    output = []
    for ent in ents:
        stripped_ent = ent.text.strip()
        if text == '':
            continue
        es_results = es.search(stripped_ent)
        sims = []
        abstracts = []
        processed_results = {}
        for es_result in es_results:
            if es_result.label in processed_results:
              continue
            else:
               processed_results[es_result.label] = True
               abstract = sparql.search(es_result.id, es_result.label, es_result.score)
               if abstract:
                    abstracts.append([es_result.id, abstract])
        if len(abstracts) > 0:
            output.append(stringify_reply(warc_record.id, stripped_ent, get_sim(text, abstracts)))
    yield output

def get_sim(text, corpus):
    text = text.lower()
    scores = {}
    for doc in corpus:
        vect = TfidfVectorizer(min_df=1, stop_words="english")
        tfidf = vect.fit_transform([text, doc[1].lower()])
        pairwise_similarity = tfidf * tfidf.T
        scores[doc[0]] = pairwise_similarity[0, 1]
    return max(scores, key=lambda x: (x[0]))

def stringify_reply(warc_id, word, freebase_id):
    return '%s\t%s\t%s' % (warc_id, word, freebase_id)


if __name__ == '__main__':
    sc = SparkContext(conf= SparkConf().set("spark.local.dir", "/var/scratch2/wdps1907/spark-temp").set("textinputformat.record.delimiter", "WARC/1.0"))
    input_lines  = sc.textFile("sample.warc.gz").collect()
    raw_records = sc.parallelize(split_records())
    rdd = raw_records.flatMap(process_page)
    rdd = rdd.saveAsTextFile("sample_predications_tmp78")
