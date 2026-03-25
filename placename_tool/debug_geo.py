import requests, json, re

name = "Zurich"
resp = requests.post(
    'https://fornpunkt.se/apis/reconciliation/geonames',
    data={'queries': json.dumps({'q': {'query': name, 'limit': 3}})},
    timeout=10
)
print(resp.status_code)
data = resp.json().get('q', {}).get('result', [])
print("Data:", data)

for item in data:
    gid = item.get('id')
    print("GID:", gid)
    rdf_url = f"http://sws.geonames.org/{gid}/about.rdf"
    rdf_resp = requests.get(rdf_url, timeout=5)
    print("RDF Status:", rdf_resp.status_code)
    lat_m = re.search(r'<wgs84_pos:lat>([-\d.]+)</', rdf_resp.text)
    lng_m = re.search(r'<wgs84_pos:long>([-\d.]+)</', rdf_resp.text)
    print("Lat/Lng:", lat_m, lng_m)

