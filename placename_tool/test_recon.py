import requests
import json

def test_wikidata():
    print("Testing Wikidata Recon...")
    resp = requests.get('https://wikidata.reconci.link/en/api', params={
        'queries': json.dumps({'q': {'query': 'Zürich', 'limit': 3}})
    })
    print(resp.status_code)
    print(resp.text[:500])

test_wikidata()

try:
    print("\nTesting GeoNames Recon via OpenRefine test instance...")
    resp = requests.get('https://geonames.reconci.link/en/api', params={
        'queries': json.dumps({'q': {'query': 'Zürich', 'limit': 3}})
    })
    print(resp.status_code)
except Exception as e:
    print(e)
