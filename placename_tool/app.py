#!/usr/bin/env python3
"""
PlaceName Reconciliation Tool
Scans TEI XML files for LOC_Lat_Long placeholders and resolves them
using Wikidata, ortsnamen.ch, GeoAdmin.ch, and GeoNames APIs.
"""

import os
import re
import math
import json
import glob
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, jsonify, request
import requests
from lxml import etree

app = Flask(__name__)

# Configuration
XML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'quellenstuecke')
PLACEHOLDER = 'LOC_Lat_Long'

# In-memory state for resolved/skipped place names
resolved_places = {}  # {place_name_text: {"lat": ..., "lng": ..., "source": ...}}
skipped_places = set()
state_lock = threading.Lock()

# ─── Coordinate conversion helpers ───────────────────────────────────────────

def ch1903_to_wgs84(east, north):
    """Convert Swiss CH1903/LV03 coordinates to WGS84 (lat, lng)."""
    # Convert to auxiliary values (shift Bern)
    y_aux = (east - 600000) / 1000000
    x_aux = (north - 200000) / 1000000

    lat = (16.9023892 +
           3.238272 * x_aux -
           0.270978 * y_aux ** 2 -
           0.002528 * x_aux ** 2 -
           0.0447 * y_aux ** 2 * x_aux -
           0.0140 * x_aux ** 3)

    lng = (2.6779094 +
           4.728982 * y_aux +
           0.791484 * y_aux * x_aux +
           0.1306 * y_aux * x_aux ** 2 -
           0.0436 * y_aux ** 3)

    lat = lat * 100 / 36
    lng = lng * 100 / 36

    return round(lat, 5), round(lng, 5)


def ch1903plus_to_wgs84(east, north):
    """Convert Swiss CH1903+/LV95 coordinates to WGS84 (lat, lng)."""
    # LV95 to LV03
    east_lv03 = east - 2000000
    north_lv03 = north - 1000000
    return ch1903_to_wgs84(east_lv03, north_lv03)


def parse_ewkt_coords(ewkt_str):
    """Parse EWKT string and return WGS84 (lat, lng)."""
    if not ewkt_str:
        return None
    # e.g. "SRID=21781;POINT (658286 241635)"
    # or "SRID=2056;POINT (2658286 1241635)"
    match = re.search(r'SRID=(\d+);POINT\s*\(\s*([\d.]+)\s+([\d.]+)\s*\)', ewkt_str)
    if not match:
        # Try MULTIPOINT or other geometry types - take first point
        match = re.search(r'SRID=(\d+);(?:MULTI)?POINT\s*\(\s*\(?\s*([\d.]+)\s+([\d.]+)', ewkt_str)
    if not match:
        return None
    srid = int(match.group(1))
    x = float(match.group(2))
    y = float(match.group(3))

    if srid == 21781:  # LV03
        return ch1903_to_wgs84(x, y)
    elif srid == 2056:  # LV95
        return ch1903plus_to_wgs84(x, y)
    elif srid == 4326:  # Already WGS84
        return round(y, 5), round(x, 5)  # EWKT is (lng, lat)
    else:
        return None


# ─── XML scanning ────────────────────────────────────────────────────────────

def scan_xml_files():
    """Scan all XML files and return place names with LOC_Lat_Long."""
    pattern = os.path.join(XML_DIR, '*.xml')
    files = sorted(glob.glob(pattern))
    place_names = {}  # {text: [{"file": ..., "line": ...}, ...]}

    for filepath in files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    # Find all placeName tags with LOC_Lat_Long in this line
                    for m in re.finditer(
                        r'<placeName\s+ref="LOC_Lat_Long">([^<]+)</placeName>',
                        line
                    ):
                        name = m.group(1)
                        if name not in place_names:
                            place_names[name] = []
                        place_names[name].append({
                            'file': os.path.basename(filepath),
                            'line': line_num
                        })
        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    return place_names


# ─── API search functions ────────────────────────────────────────────────────

def search_wikidata(name):
    """Search Wikidata for a place name and return candidates with coordinates."""
    results = []
    try:
        # Use Wikidata SPARQL endpoint
        sparql = f"""
        SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
          ?item rdfs:label "{name}"@de .
          ?item wdt:P625 ?coord .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
        }}
        LIMIT 10
        """
        resp = requests.get(
            'https://query.wikidata.org/sparql',
            params={'query': sparql, 'format': 'json'},
            headers={'User-Agent': 'PlaceNameReconciler/1.0'},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            for binding in data.get('results', {}).get('bindings', []):
                coord_str = binding.get('coord', {}).get('value', '')
                # Parse Point(lng lat)
                coord_match = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', coord_str)
                if coord_match:
                    lng = float(coord_match.group(1))
                    lat = float(coord_match.group(2))
                    qid = binding.get('item', {}).get('value', '').split('/')[-1]
                    results.append({
                        'source': 'Wikidata',
                        'name': binding.get('itemLabel', {}).get('value', name),
                        'description': binding.get('itemDescription', {}).get('value', ''),
                        'lat': round(lat, 5),
                        'lng': round(lng, 5),
                        'url': f'https://www.wikidata.org/wiki/{qid}',
                        'id': qid
                    })

        # Also try with broader search using wbsearchentities
        if not results:
            resp2 = requests.get(
                'https://www.wikidata.org/w/api.php',
                params={
                    'action': 'wbsearchentities',
                    'search': name,
                    'language': 'de',
                    'type': 'item',
                    'limit': 10,
                    'format': 'json'
                },
                headers={'User-Agent': 'PlaceNameReconciler/1.0'},
                timeout=10
            )
            if resp2.status_code == 200:
                entities = resp2.json().get('search', [])
                qids = [e['id'] for e in entities[:5]]
                if qids:
                    # Get coordinates for these entities
                    qid_values = ' '.join(f'wd:{q}' for q in qids)
                    sparql2 = f"""
                    SELECT ?item ?itemLabel ?itemDescription ?coord WHERE {{
                      VALUES ?item {{ {qid_values} }}
                      ?item wdt:P625 ?coord .
                      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
                    }}
                    """
                    resp3 = requests.get(
                        'https://query.wikidata.org/sparql',
                        params={'query': sparql2, 'format': 'json'},
                        headers={'User-Agent': 'PlaceNameReconciler/1.0'},
                        timeout=15
                    )
                    if resp3.status_code == 200:
                        data3 = resp3.json()
                        for binding in data3.get('results', {}).get('bindings', []):
                            coord_str = binding.get('coord', {}).get('value', '')
                            coord_match = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', coord_str)
                            if coord_match:
                                lng = float(coord_match.group(1))
                                lat = float(coord_match.group(2))
                                qid = binding.get('item', {}).get('value', '').split('/')[-1]
                                results.append({
                                    'source': 'Wikidata',
                                    'name': binding.get('itemLabel', {}).get('value', name),
                                    'description': binding.get('itemDescription', {}).get('value', ''),
                                    'lat': round(lat, 5),
                                    'lng': round(lng, 5),
                                    'url': f'https://www.wikidata.org/wiki/{qid}',
                                    'id': qid
                                })
    except Exception as e:
        print(f"Wikidata search error for '{name}': {e}")
    return results


def search_ortsnamen(name):
    """Search ortsnamen.ch for a place name."""
    results = []
    try:
        resp = requests.get(
            'https://search.ortsnamen.ch/de/api/search',
            params={'q': name, 'limit': 10},
            headers={'User-Agent': 'PlaceNameReconciler/1.0'},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('results', []):
                # Get detail for coordinates
                toponym_id = item.get('id')
                detail_url = item.get('url', '')
                if not detail_url and toponym_id:
                    detail_url = f'https://search.ortsnamen.ch/de/api/toponyms/{toponym_id}'

                localisation_text = item.get('localisation', '')
                municipalities = ', '.join(item.get('municipalities', []))
                cantons = ', '.join(item.get('cantons', []))
                description_parts = item.get('description', [])
                desc = ', '.join(description_parts) if description_parts else ''
                if municipalities:
                    desc = f"{municipalities}" + (f" ({cantons})" if cantons else '')
                elif cantons:
                    desc = cantons

                # Try to get coordinates from detail endpoint
                coords = None
                if toponym_id:
                    try:
                        detail_resp = requests.get(
                            f'https://search.ortsnamen.ch/de/api/toponyms/{toponym_id}',
                            headers={'User-Agent': 'PlaceNameReconciler/1.0'},
                            timeout=10
                        )
                        if detail_resp.status_code == 200:
                            detail = detail_resp.json()
                            loc = detail.get('localisation')
                            if loc and loc.get('data'):
                                ewkt = loc['data'].get('ewkt', '')
                                coords = parse_ewkt_coords(ewkt)
                    except Exception as e2:
                        print(f"ortsnamen.ch detail error for id {toponym_id}: {e2}")

                if coords:
                    results.append({
                        'source': 'ortsnamen.ch',
                        'name': item.get('name', name),
                        'description': desc,
                        'lat': coords[0],
                        'lng': coords[1],
                        'url': f'https://search.ortsnamen.ch/de/record/{toponym_id}' if toponym_id else '',
                        'id': str(toponym_id) if toponym_id else ''
                    })
    except Exception as e:
        print(f"ortsnamen.ch search error for '{name}': {e}")
    return results


def search_geoadmin(name):
    """Search GeoAdmin.ch (Swiss federal geo API) for place names."""
    results = []
    try:
        resp = requests.get(
            'https://api3.geo.admin.ch/rest/services/api/SearchServer',
            params={
                'searchText': name,
                'type': 'locations',
                'sr': '4326',
                'limit': 5
            },
            headers={'User-Agent': 'PlaceNameReconciler/1.0'},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            for feature in data.get('results', []):
                attrs = feature.get('attrs', {})
                lat = attrs.get('lat')
                lng = attrs.get('lon')
                label = attrs.get('label', name)
                # Clean HTML from label
                label = re.sub(r'<[^>]+>', '', label)
                detail = attrs.get('detail', '')
                detail = re.sub(r'<[^>]+>', '', detail)

                if lat and lng:
                    results.append({
                        'source': 'GeoAdmin',
                        'name': label,
                        'description': detail,
                        'lat': round(float(lat), 5),
                        'lng': round(float(lng), 5),
                        'url': f'https://map.geo.admin.ch/?swisssearch={name}',
                        'id': str(attrs.get('featureId', ''))
                    })
    except Exception as e:
        print(f"GeoAdmin search error for '{name}': {e}")
    return results


def search_geonames(name):
    """Search GeoNames for a place name."""
    results = []
    try:
        resp = requests.get(
            'http://api.geonames.org/searchJSON',
            params={
                'q': name,
                'maxRows': 5,
                'username': 'demo',
                'style': 'FULL',
                'lang': 'de'
            },
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('geonames', []):
                lat = item.get('lat')
                lng = item.get('lng')
                if lat and lng:
                    country = item.get('countryName', '')
                    admin1 = item.get('adminName1', '')
                    desc_parts = [p for p in [admin1, country] if p]
                    results.append({
                        'source': 'GeoNames',
                        'name': item.get('toponymName', name),
                        'description': ', '.join(desc_parts),
                        'lat': round(float(lat), 5),
                        'lng': round(float(lng), 5),
                        'url': f'https://www.geonames.org/{item.get("geonameId", "")}',
                        'id': str(item.get('geonameId', ''))
                    })
    except Exception as e:
        print(f"GeoNames search error for '{name}': {e}")
    return results


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/placenames')
def get_placenames():
    """Return all unique place names with LOC_Lat_Long and their occurrences."""
    place_names = scan_xml_files()
    result = []
    with state_lock:
        for name, occurrences in sorted(place_names.items()):
            status = 'pending'
            coords = None
            if name in resolved_places:
                status = 'resolved'
                coords = resolved_places[name]
            elif name in skipped_places:
                status = 'skipped'
            result.append({
                'name': name,
                'count': len(occurrences),
                'occurrences': occurrences,
                'status': status,
                'coords': coords
            })
    return jsonify({
        'placenames': result,
        'total': len(result),
        'resolved': sum(1 for r in result if r['status'] == 'resolved'),
        'skipped': sum(1 for r in result if r['status'] == 'skipped'),
        'pending': sum(1 for r in result if r['status'] == 'pending')
    })


@app.route('/api/search')
def search_place():
    """Search all sources for a place name."""
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Missing name parameter'}), 400

    all_results = []

    # Search all sources in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(search_wikidata, name): 'Wikidata',
            executor.submit(search_ortsnamen, name): 'ortsnamen.ch',
            executor.submit(search_geoadmin, name): 'GeoAdmin',
            executor.submit(search_geonames, name): 'GeoNames',
        }
        for future in as_completed(futures):
            source = futures[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                print(f"Error from {source}: {e}")

    # Deduplicate results with very similar coordinates
    deduped = []
    seen_coords = []
    for r in all_results:
        is_dup = False
        for sc in seen_coords:
            if (sc['source'] == r['source'] and
                abs(sc['lat'] - r['lat']) < 0.001 and
                abs(sc['lng'] - r['lng']) < 0.001):
                is_dup = True
                break
        if not is_dup:
            deduped.append(r)
            seen_coords.append(r)

    return jsonify({'results': deduped, 'query': name})


@app.route('/api/resolve', methods=['POST'])
def resolve_place():
    """Mark a place name as resolved with specific coordinates."""
    data = request.get_json()
    name = data.get('name', '').strip()
    lat = data.get('lat')
    lng = data.get('lng')
    source = data.get('source', '')

    if not name or lat is None or lng is None:
        return jsonify({'error': 'Missing required fields'}), 400

    with state_lock:
        resolved_places[name] = {
            'lat': round(float(lat), 5),
            'lng': round(float(lng), 5),
            'source': source
        }
        skipped_places.discard(name)

    return jsonify({'status': 'ok', 'name': name})


@app.route('/api/skip', methods=['POST'])
def skip_place():
    """Mark a place name as skipped."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Missing name'}), 400

    with state_lock:
        skipped_places.add(name)
        resolved_places.pop(name, None)

    return jsonify({'status': 'ok', 'name': name})


@app.route('/api/apply', methods=['POST'])
def apply_changes():
    """Apply all resolved coordinates to the XML files."""
    with state_lock:
        current_resolved = dict(resolved_places)

    if not current_resolved:
        return jsonify({'error': 'No resolved place names to apply'}), 400

    modified_files = []
    errors = []
    pattern = os.path.join(XML_DIR, '*.xml')

    for filepath in sorted(glob.glob(pattern)):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            original = content
            for name, coords in current_resolved.items():
                ref_value = f'LOC_{coords["lat"]}_{coords["lng"]}'
                # Replace LOC_Lat_Long in placeName tags with this specific text
                pattern_re = re.compile(
                    rf'(<placeName\s+ref=")LOC_Lat_Long(">{re.escape(name)}</placeName>)'
                )
                content = pattern_re.sub(rf'\g<1>{ref_value}\g<2>', content)

            if content != original:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                modified_files.append(os.path.basename(filepath))
        except Exception as e:
            errors.append({'file': os.path.basename(filepath), 'error': str(e)})

    return jsonify({
        'status': 'ok',
        'modified_files': modified_files,
        'modified_count': len(modified_files),
        'errors': errors
    })


@app.route('/api/unresolve', methods=['POST'])
def unresolve_place():
    """Remove a place name from resolved state."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Missing name'}), 400

    with state_lock:
        resolved_places.pop(name, None)
        skipped_places.discard(name)

    return jsonify({'status': 'ok', 'name': name})


if __name__ == '__main__':
    print(f"📍 PlaceName Reconciliation Tool")
    print(f"   XML directory: {os.path.abspath(XML_DIR)}")
    print(f"   Open http://localhost:5000 in your browser")
    app.run(debug=True, port=5000)
