from time import sleep
import requests
from SPARQLWrapper import SPARQLWrapper, JSON

prefixes = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX fbase: <http://rdf.freebase.com/ns/>
PREFIX dbo: <http://dbpedia.org/ontology/>
"""
same_as_template = prefixes + """
SELECT DISTINCT ?same ?abstract WHERE {
    ?s owl:sameAs <http://rdf.freebase.com/ns/m.%s> .
    { ?s owl:sameAs ?same .} UNION { ?same owl:sameAs ?s .}
    ?s dbo:abstract ?abstract .
    FILTER (langMatches(lang(?abstract),"en"))
}
limit 1
"""

sparql = SPARQLWrapper("http://lod.openlinksw.com/sparql/")


class SparqlSearcher:
    def __init__(self, address: str, count: int):
        self._address = address
        self._count = count


    def search(self, freebase_id, label, score) -> map:
        url = 'http://%s:9090/sparql' % self._address
        data = []
        key = freebase_id.split("/")[2]
        #response2 = requests.post(url, data={'print': False, 'query': same_as_template % (key)    }).json()
        sparql.setQuery(same_as_template % key)
        sparql.setReturnFormat(JSON)
        response2 = sparql.query().convert()
        #print(same_as_template % key)
        results = response2.get('results').get('bindings')
        if results and len(results) > 0:
            return results[0].get('abstract').get('value')
        else:
            return False
