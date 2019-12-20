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
from spacy import displacy
from collections import Counter
import en_core_web_md
nlp = en_core_web_md.load()

KEYNAME = "WARC-TREC-ID"

sparql = SparqlSearcher(
    "%s" % sys.argv[3],
    1,
)
es = ElasticSearcher(
    "%s" % sys.argv[4],
    5
)


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line


def process_page(row):
    warc_record = WarcRecord(row)
    warc_payload = warc_record.payload
    if warc_record.broken or not warc_payload:
        return []

    canonical_labels_of_ids = dict()
    related_ids_of_ids = dict()
    ids_of_words = defaultdict(list)
    types_of_words = defaultdict(list)
    ents = []
    soup = BeautifulSoup(warc_payload, "html.parser")
    for script in soup(['style', 'script', 'head', 'title', 'meta', '[document]', 'code' 'blockquote', 'cite']):
        script.extract()
    text = " ".join(re.findall(r'\w+',  soup.get_text()))
    article = nlp(text)

    raw_words = []
    for ent in article.ents:
        if(ent.label_ not in ["DATE", "TIME", "ORDINAL", "MONEY", "PERCENT", "QUANTITY", "QUANITY", "CARDINAL"]) and len(ent.text.split(" ")) < 4:
            raw_words.append(ent.text)
            ents.append(ent)
    output = []
    processed_results = {}
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
            output.append(stringify_reply(warc_record.id,
                                  stripped_ent, get_sim(text, abstracts)))
    return output

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
    return '<%s>\t%s\t%s' % (warc_id, word, freebase_id)


if __name__ == '__main__':
    warcfile = gzip.open(sys.argv[1], "rt", errors="ignore")
    output = []
    count = 0
    
    for row in split_records(warcfile):
        output.extend(process_page(row))
    outfile = open(sys.argv[2], 'w')
    outfile.write("\n".join(output))
    outfile.close()
