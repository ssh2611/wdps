import gzip
import sys
from warcRecord import WarcRecord
from nltk import ne_chunk, pos_tag, word_tokenize, tokenize, sent_tokenize
from nltk.tree import Tree
from nltk.corpus import stopwords
#from textExtractor import TextExtractor
from collections import defaultdict
import re
from elasticSearcher import ElasticSearcher
from sparqlSearcher import SparqlSearcher
from bs4 import BeautifulSoup
from bs4.element import Comment
import spacy
from spacy import displacy
from collections import Counter
#print("Reading en code web md")
import en_core_web_md
nlp = en_core_web_md.load()
#import justext
from sklearn.feature_extraction.text import TfidfVectorizer

KEYNAME = "WARC-TREC-ID"

sparql = SparqlSearcher(
    "node059",
    1,
    1,
    1,
    mock=False
)
es = ElasticSearcher(
    "node001",
    10,
    mock=False
)


def split_records(stream):
	payload = ''
	for line in stream:
		if line.strip() == "WARC/1.0":
			yield payload
			payload = ''
		else:
			payload += line

def get_continuous_chunks(text):
    tokenized = word_tokenize(text)
    #filtered = [w for w in tokenized if not w in stop_words] 
    chunked = ne_chunk(pos_tag(tokenized))
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


def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]', 'code' 'blockquote', 'cite']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def process_page(row):
	warc_record = WarcRecord(row)
	warc_payload = warc_record.payload
	if warc_record.broken or not warc_payload:
		return
	# text = BeautifulSoup(warc_payload, "html.parser").findAll(text=True)
	#print(text)
	# visible_texts = filter(tag_visible, text)
	# text = u" ".join(t.strip() for t in visible_texts)
	# text =  BeautifulSoup(warc_payload, "lxml").text
	# print(text)
	# paragraphs = justext.justext(BeautifulSoup(warc_payload, "lxml").prettify(), justext.get_stoplist("English"))
	# print(paragraphs)
	# ners = []
	# text = ""
	# for paragraph in paragraphs:
	# 	# if not paragraph.is_boilerplate:
	# 	print(paragraph.text)
	# 	ners.extend(get_continuous_chunks(re.sub('<[^>]*>', ' ', paragraph.text)))
	# for ner in ners:
	# 	text = text + " " + ner

	canonical_labels_of_ids = dict()
	related_ids_of_ids = dict()
	ids_of_words = defaultdict(list)
	types_of_words = defaultdict(list)
	# print(ners)
	# print("====================")
	# spacy_nlp = spacy.load("en_core_web_md")
	# document = spacy_nlp(paragraphs)
	# for element in document.ents:
	# 	print(element)
	ents = []
	soup = BeautifulSoup(warc_payload, 'lxml')#.findAll(text=True)
	for script in soup(["script", "style", 'aside']):
		script.extract()
	#text = " ".join(re.split(r'[\n\t]+', soup.get_text()))
	#text = filter(tag_visible, soup)
	#text = " ".join(t.strip() for t in text)
	text = soup.get_text()
	#print(text)
	article = nlp(text)
	#ents = get_continuous_chunks(re.sub('<[^>]*>', ' ', text))
	#print(len(article.ents))
        
	# print([(X.text, X.label_) ])
	raw_words = []
	for ent in article.ents:
		if(ent.label_ not in ["CARDINAL", "DATE", "QUANTITY", "TIME", "ORDINAL", "MONEY", "PERCENT", "QUANTITY"]):
			raw_words.append(ent.text)
			ents.append(ent)

#	return ootramba
	#print(ents)	
	for ent in ents:
		stripped_ent = ent.text.strip()
		if text == '':
			continue
		es_results = es.search(stripped_ent)
		sims = []
		for es_result in es_results:
			#print(es_result)
			#if canonical_labels_of_ids.get(es_result.id) is not None:
			#	continue
			#ids_of_words[ent].append(es_result.id)
			#print(es_result)
			#types_of_words[ent].append(ent.label_)
			#canonical_labels_of_ids[es_result.id] = es_result.label
			abstract = sparql.search(es_result.id, es_result.label, es_result.score)
			#for abstract in abstracts:
				#{es_result, similar(text, abstract)}
			#print("Got abstract for %s %s" % (ent.text, es_result.label))
			if abstract:
                                sims.append({'score': get_jaccard_sim(abstract, text), 'result': es_result})
				#print("Similarity, %s %s " % (abstract[0:20], str(get_jaccard_sim(abstract, text))))
		if len(sims) > 0:
			max_common = max(sims, key=lambda item: item['score'])
			#print("===> %s %s" % (ent.text, max))
			print(stringify_reply(warc_record.id, stripped_ent, max_common.get('result').id))		
	#		related_ids_of_ids[es_result.id] = set(i for i in sparql.search(es_result.id, es_result.label, es_result.score))
	#for word, ids in ids_of_words.items():
	#	if word in raw_words:
	#		max_common = -1
	#		id_with_max_common = None
	#		for freebase_id in ids:
	#			for other_id, related_ids in related_ids_of_ids.items():
	#				if freebase_id == other_id:
	#					continue
	#				common = len(related_ids.intersection(related_ids_of_ids[freebase_id]))
	#				if common > max_common:
	#					max_common = common
	#					id_with_max_common = freebase_id
	#		if id_with_max_common is None:
	#			id_with_max_common = ids[0]
	#		label_with_max_common = canonical_labels_of_ids[id_with_max_common]
	#		print(stringify_reply(warc_record.id, word, id_with_max_common))
	#print("PROCESSED RECORD")

def get_jaccard_sim(str1, str2): 
    a = set(str1.split()) 
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))

def stringify_reply(warc_id, word, freebase_id):
	return '%s\t%s\t%s' % (warc_id, word, freebase_id)

if __name__ == '__main__':
	if len(sys.argv) < 1:
		print('Usage: python3 starter-code.py INPUT')
		sys.exit(0)
	#print("Starting reading warc file")

	cheats = dict((line.split('\t',2) for line in open('data/sample-labels-cheat.txt').read().splitlines()))
	warcfile = gzip.open(sys.argv[1], "rt", errors="ignore")
	count = 0 
	for row in split_records(warcfile):
		#if count > 32:
	#		continue
		process_page(row)
		count = count + 1
		#if count > 50:
		#	break
		
		# for key, label, freebase_id in find_labels(record, cheats):
		# 	print(key + '\t' + label + '\t' + freebase_id)
