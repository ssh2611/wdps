from collections import namedtuple
from requests import get
import json

ElasticSearchResult = namedtuple('ElasticSearchResult', ['id', 'label', 'score', 'type'])


class ElasticSearcher:
    def __init__(self, address: str, default_count: int, mock: bool = False):
        self._address = address
        self._default_count = default_count
        self._request_get = get if not mock else get_mock

    def search(self, query: str, count: int = None) -> list:
        if count is None:
            count = self._default_count
        url = 'http://%s:9200/freebase/label/_search' % self._address
        response = self._request_get(url, params={'q': query, 'size': count, 'sort': {'_score': {'order': 'desc'}}}).json()
        results = []
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            resource = hit.get('_source', {}).get('resource')
            freebase_id = resource
            score = hit.get('_score')
            results.append(ElasticSearchResult(freebase_id, freebase_label, score, ""))
        return results

