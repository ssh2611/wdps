import gzip
import sys
from warcRecord import WarcRecord
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree
from nltk.corpus import stopwords
from textExtractor import TextExtractor
from collections import defaultdict
import re
from elasticSearcher import ElasticSearcher
from sparqlSearcher import SparqlSearcher

KEYNAME = "WARC-TREC-ID"

sparql = SparqlSearcher(
    "node001",
    1,
    5,
    5,
    mock=False
)
es = ElasticSearcher(
    "node001",
    1,
    mock=False
)

def find_labels(payload, labels):
	key = None
	for line in payload.splitlines():
		if line.startswith(KEYNAME):
			key = line.split(': ')[1]
			break
	for label, freebase_id in labels.items():
		if key and (label in payload):
			yield key, label, freebase_id



def split_records(stream):
	payload = ''
	for line in stream:
		if line.strip() == "WARC/1.0":
			yield payload
			payload = ''
		else:
			payload += line

def get_continuous_chunks(text):
    chunked = ne_chunk(pos_tag(word_tokenize(text)))
    continuous_chunk = []
    current_chunk = []
    for i in chunked:
					if type(i) == Tree:
									current_chunk.append(" ".join([token for token, pos in i.leaves()]))
					elif current_chunk:
									named_entity = " ".join(current_chunk)
									if named_entity not in continuous_chunk:
													continuous_chunk.append(named_entity)
													current_chunk = []
					else:
									continue
    return continuous_chunk


def process_page(row):
	warc_record = WarcRecord(row)
	warc_payload = warc_record.payload
	if warc_record.broken or not warc_payload:
		return
	words = TextExtractor.get_all_words(warc_payload)
	words = [word for word in words if word not in stopwords.words('english')]
	ners = get_continuous_chunks(re.sub('<[^>]*>', ' ', warc_payload))

	canonical_labels_of_ids = dict()
	related_ids_of_ids = dict()
	ids_of_words = defaultdict(list)
	#print(words)

	for word in words:
		es_results = es.search(word)
		for es_result in es_results:
			#print(es_result)
			if canonical_labels_of_ids.get(es_result.id) is not None:
				# We are not interested in labels with repeating freebase ids,
				# so we just skip them
				continue
			print("%s %s", word, es_result.label)
			ids_of_words[word].append(es_result.id)
			canonical_labels_of_ids[es_result.id] = es_result.label
			related_ids_of_ids[es_result.id] = set(i for i in sparql.search(es_result.id))
			#print(related_ids_of_ids)
	#print(ids_of_words)
	print("PROCESSED RECORD")
	print(ids_of_words)
	print(canonical_labels_of_ids)
	#for d in ids_of_words:
	#	print(d)

if __name__ == '__main__':
	if len(sys.argv) < 1:
		print('Usage: python3 starter-code.py INPUT')
		sys.exit(0)

	cheats = dict((line.split('\t',2) for line in open('data/sample-labels-cheat.txt').read().splitlines()))
	warcfile = gzip.open(sys.argv[1], "rt", errors="ignore")
	count = 0 
	for row in split_records(warcfile):
		process_page(row)
		count = count + 1
		if count > 20:
			break
		
		# for key, label, freebase_id in find_labels(record, cheats):
		# 	print(key + '\t' + label + '\t' + freebase_id)
