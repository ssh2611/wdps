from time import sleep
from requests import post, RequestException

from SPARQLWrapper import SPARQLWrapper, JSON


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
po_template = prefixes + """
SELECT DISTINCT * WHERE {
 <http://rdf.freebase.com/ns/m.%s> ?p ?o.
}

limit 10

"""

my_template = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?abstract WHERE {

  {
    ?altName rdfs:label "%s"@en ;
             dbo:abstract ?abstract .
            

  }
}
"""
sparql = SPARQLWrapper("http://dbpedia.org/sparql")


class SparqlSearcher:
    def __init__(self, address: str, count: int, retry_attempts: int, retry_delay: float, mock: bool = False):
        self._address = address
        self._count = count
        self._attempts = retry_attempts
        self._delay = retry_delay
        self._post_request = post if not mock else post_mock

    #def search(self, freebase_id: str, label):
    #    return self._search_retry(freebase_id, 1)

    def search(self, freebase_id, label, score) -> map:
        try:
            url = 'http://%s:9090/sparql' % self._address
            #url = "http://vmdbpedia.informatik.uni-leipzig.de:8080/"
            #class_type = "common.topic"
            #if(type == "PERSON"):
            #    class_type = "people.person"
            #elif(type == "GPE" or type == "LOC"):
            #    class_type = "location.location"
            #elif(type == "ORG"):
            #    class_type = "organization.organization"
            #query = 'select * where {<http://rdf.freebase.com/ns/%s> ?p ?o} limit %d' % (freebase_id.split("/")[2],  self._count)
            #print(query)
            #print(label)
            #response = requests.post(url, data={'print': False, 'query': po_template % (freebase_id.split("/")[2])}).json()

            #response = requests.post(url, data={'print': True, 'query': query}).json()
            #print(response.get('stats'))
            #results = response.get('results')
            #print(results)
            #if results:
               # bindings = results.get('bindings')
		#
            data = []
                #return map(lambda binding: binding.get('p').get('value'), bindings)
            key = freebase_id.split("/")[2]
            response2 = requests.post(url, data={'print': False, 'query': same_as_template % (key)    }).json()
            #sparql.setQuery(same_as_template % key)
            #sparql.setReturnFormat(JSON)
            #response2 = sparql.query().convert()
            #print("RESPONSE")
            #print(response2)
            #print(same_as_template % key)
            results = response2.get('results').get('bindings')
            if results and len(results) > 0:
                   #print(binding.get('p').get('value'))
                return results[0].get('abstract').get('value')
            else:
                return False
            #else:
            #    return []
        except RequestException:
            if attempt == self._attempts:
                raise
            sleep(self._delay)
            return self._search_retry(freebase_id, attempt + 1)


class PostMock:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def post_mock(_, data):
    freebase_id = data['query'].split('/')[1].split('>')[0]
    mock = {'results': {'bindings': [{
        'o': {'value': 'fb-id-%s->%d' % (freebase_id, x)}
    } for x in range(3)]}}
    return PostMock(mock)
