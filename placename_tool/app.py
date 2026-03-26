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

try:
    from .xml_scan import PLACEHOLDER, replace_placeholder_refs, scan_xml_files
except ImportError:
    from xml_scan import PLACEHOLDER, replace_placeholder_refs, scan_xml_files

app = Flask(__name__)

# Configuration
XML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'quellenstuecke')

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
        # Try POLYGON / MULTIPOLYGON - extract first coordinate pair
        match = re.search(r'SRID=(\d+);(?:MULTI)?POLYGON\s*[ZM ]*\(+\s*([\d.]+)\s+([\d.]+)', ewkt_str)
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


# ─── API search functions ────────────────────────────────────────────────────

def search_wikidata(name):
    """Search Wikidata using the W3C Entity Reconciliation API format."""
    results = []
    try:
        # Search via Reconciliation API with fuzzy match ~ (German endpoint)
        resp = requests.get('https://wikidata.reconci.link/de/api', params={
            'queries': json.dumps({'q': {'query': f"{name}~", 'limit': 10}})
        }, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json().get('q', {}).get('result', [])
            qids = [item['id'] for item in data if item['id'].startswith('Q')]
            
            if qids:
                # Then fetch coords for these IDs using the standard Wikidata API
                qids_str = '|'.join(qids)
                ents_resp = requests.get('https://www.wikidata.org/w/api.php', params={
                    'action': 'wbgetentities',
                    'ids': qids_str,
                    'props': 'claims',
                    'format': 'json'
                }, headers={'User-Agent': 'PlaceNameReconciler/1.0'}, timeout=15)
                
                if ents_resp.status_code == 200:
                    entities = ents_resp.json().get('entities', {})
                    for r in data:
                        qid = r['id']
                        if qid in entities:
                            ent = entities[qid]
                            claims = ent.get('claims', {})
                            if 'P625' in claims: # Coordinate location
                                try:
                                    coord = claims['P625'][0]['mainsnak']['datavalue']['value']
                                    lat = coord.get('latitude')
                                    lng = coord.get('longitude')
                                    
                                    results.append({
                                        'source': 'Wikidata',
                                        'name': r.get('name', name),
                                        'description': r.get('description', ''),
                                        'lat': round(lat, 5),
                                        'lng': round(lng, 5),
                                        'url': f'https://www.wikidata.org/wiki/{qid}',
                                        'id': qid
                                    })
                                except (KeyError, IndexError, TypeError) as e:
                                    print(f"Error parsing P625 for {qid}: {e}")
    except Exception as e:
        print(f"Wikidata Recon error for '{name}': {e}")
    return results


def search_ortsnamen(name):
    """Search ortsnamen.ch for a place name."""
    results = []
    try:
        # Append ~ for Lucene fuzzy search
        resp = requests.get(
            'https://search.ortsnamen.ch/de/api/search',
            params={'q': f"{name}~", 'limit': 10},
            headers={'User-Agent': 'PlaceNameReconciler/1.0'},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('results', []):
                toponym_id = item.get('id')
                municipalities = ', '.join(item.get('municipalities', []))
                cantons = ', '.join(item.get('cantons', []))
                description_parts = item.get('description', [])
                desc = ', '.join(description_parts) if isinstance(description_parts, list) else str(description_parts)
                if municipalities:
                    desc = municipalities + (f" ({cantons})" if cantons else '')
                elif cantons:
                    desc = cantons

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
    """Search GeoNames using the FornPunkt Reconciliation API."""
    results = []
    try:
        resp = requests.post(
            'https://fornpunkt.se/apis/reconciliation/geonames',
            data={'queries': json.dumps({'q': {'query': name, 'limit': 3}})},
            timeout=10
        )
        if resp.status_code == 200:
            try:
                resp_data = resp.json()
                # The reconciliation API mirrors the query key (e.g. 'q' or 'q0')
                data = next(iter(resp_data.values()), {}).get('result', [])
            except (json.JSONDecodeError, AttributeError):
                data = []

            for item in data:
                gid = item.get('id')
                if not gid:
                    continue
                # Fetch Coordinates from GeoNames RDF snippet
                rdf_url = f"https://sws.geonames.org/{gid}/about.rdf"
                try:
                    rdf_resp = requests.get(rdf_url, timeout=5)
                    if rdf_resp.status_code == 200:
                        # Use lxml or simple regex to extract coordinates
                        # The RDF uses <wgs84_pos:lat> and <wgs84_pos:long>
                        lat_m = re.search(r'<wgs84_pos:lat>([-\d.]+)</', rdf_resp.text)
                        lng_m = re.search(r'<wgs84_pos:long>([-\d.]+)</', rdf_resp.text)
                        if lat_m and lng_m:
                            results.append({
                                'source': 'GeoNames (Recon)',
                                'name': item.get('name', name),
                                'description': item.get('description', ''),
                                'lat': round(float(lat_m.group(1)), 5),
                                'lng': round(float(lng_m.group(1)), 5),
                                'url': f'https://www.geonames.org/{gid}',
                                'id': str(gid)
                            })
                except Exception as e:
                    print(f"Error fetching GeoNames RDF for {gid}: {e}")
    except Exception as e:
        print(f"GeoNames Recon search error for '{name}': {e}")
    return results


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/placenames')
def get_placenames():
    """Return all unique place names with LOC_Lat_Long and their occurrences."""
    place_names = scan_xml_files(XML_DIR)
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
            content = replace_placeholder_refs(content, current_resolved)

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
