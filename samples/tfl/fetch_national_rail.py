#!/usr/bin/env python3
"""
Fetch National Rail station and connection data from the TfL Unified API
and merge into the existing TFL sample CSVs.

Usage:
    python3 fetch_national_rail.py

Fetches data for National Rail operators serving Greater London,
normalizes station names to match existing data, computes travel times
from haversine distance, and appends to stations.csv / connections.csv.
"""

import csv
import json
import math
import os
import sys
import time
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIONS_CSV = os.path.join(SCRIPT_DIR, "stations.csv")
CONNECTIONS_CSV = os.path.join(SCRIPT_DIR, "connections.csv")

# TfL API base
API_BASE = "https://api.tfl.gov.uk"

# National Rail operators to include (TfL line IDs)
OPERATORS = [
    ("southeastern", "Southeastern"),
    ("southern", "Southern"),
    ("thameslink", "Thameslink"),
    ("south-western-railway", "South Western Railway"),
    ("great-northern", "Great Northern"),
    ("c2c", "c2c"),
    ("greater-anglia", "Greater Anglia"),
    ("great-western-railway", "Great Western Railway"),
    ("chiltern-railways", "Chiltern Railways"),
    ("heathrow-express", "Heathrow Express"),
]

# Greater London bounding box (lat/lon)
LAT_MIN, LAT_MAX = 51.28, 51.72
LON_MIN, LON_MAX = -0.52, 0.34

# Average suburban rail speed for travel time estimation
AVG_SPEED_KMH = 40.0

# Explicit name mappings: TfL API name -> existing station name
EXPLICIT_MAP = {
    "St Pancras International": "King's Cross St. Pancras",
    "King's Cross": "King's Cross St. Pancras",
    "Heathrow Terminals 2 & 3": "Heathrow Terminals 1-2-3",
    "Heathrow Terminals 1, 2 & 3": "Heathrow Terminals 1-2-3",
    "Heathrow Terminal 2 & 3": "Heathrow Terminals 1-2-3",
}

# Additional interchange edges (NR terminus <-> nearby tube station)
INTERCHANGE_EDGES = [
    ("Fenchurch Street", "Tower Hill", "Walk", 5.0),
]


def fetch_json(url):
    """Fetch JSON from URL with retry."""
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "TuringDB-Sample/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < 2:
                print(f"  Retry {attempt + 1} for {url}: {e}")
                time.sleep(2)
            else:
                raise


def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def latlon_to_osgrid(lat, lon):
    """Convert WGS84 lat/lon to OS National Grid easting/northing.

    Uses a simplified Helmert + transverse Mercator approach,
    accurate to ~5m which is sufficient for station positions.
    """
    # Airy 1830 ellipsoid
    a = 6377563.396
    b = 6356256.909
    F0 = 0.9996012717
    lat0 = math.radians(49.0)
    lon0 = math.radians(-2.0)
    N0 = -100000.0
    E0 = 400000.0
    e2 = 1 - (b * b) / (a * a)
    n = (a - b) / (a + b)

    lat_r = math.radians(lat)
    lon_r = math.radians(lon)

    sinlat = math.sin(lat_r)
    coslat = math.cos(lat_r)
    tanlat = math.tan(lat_r)

    nu = a * F0 / math.sqrt(1 - e2 * sinlat * sinlat)
    rho = (a * F0 * (1 - e2)
           / (1 - e2 * sinlat * sinlat) ** 1.5)
    eta2 = nu / rho - 1

    # Meridional arc
    n2 = n * n
    n3 = n * n2
    Ma = (1 + n + 5.0 / 4 * n2 + 5.0 / 4 * n3) * (lat_r - lat0)
    Mb = (3 * n + 3 * n2 + 21.0 / 8 * n3) * math.sin(
        lat_r - lat0) * math.cos(lat_r + lat0)
    Mc = (15.0 / 8 * n2 + 15.0 / 8 * n3) * math.sin(
        2 * (lat_r - lat0)) * math.cos(2 * (lat_r + lat0))
    Md = 35.0 / 24 * n3 * math.sin(
        3 * (lat_r - lat0)) * math.cos(3 * (lat_r + lat0))
    M = b * F0 * (Ma - Mb + Mc - Md)

    dlambda = lon_r - lon0

    I = M + N0
    II = nu / 2 * sinlat * coslat
    III = nu / 24 * sinlat * coslat ** 3 * (5 - tanlat ** 2 + 9 * eta2)
    IIIA = nu / 720 * sinlat * coslat ** 5 * (
        61 - 58 * tanlat ** 2 + tanlat ** 4)
    IV = nu * coslat
    V = nu / 6 * coslat ** 3 * (nu / rho - tanlat ** 2)
    VI = nu / 120 * coslat ** 5 * (
        5 - 18 * tanlat ** 2 + tanlat ** 4
        + 14 * eta2 - 58 * tanlat ** 2 * eta2)

    northing = (I + II * dlambda ** 2 + III * dlambda ** 4
                + IIIA * dlambda ** 6)
    easting = (E0 + IV * dlambda + V * dlambda ** 3
               + VI * dlambda ** 5)

    return int(round(easting)), int(round(northing))


def normalize_name(api_name, existing_names):
    """Normalize a TfL API station name to match existing conventions."""
    name = api_name

    # Strip common suffixes
    for suffix in [" Rail Station", " Railway Station",
                   " (London)", " Station"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]

    # Strip "London " prefix unless the full name exists already
    # (e.g. keep "London Bridge", "London Fields"; strip "London Waterloo")
    if name.startswith("London "):
        if name not in existing_names:
            name = name[len("London "):]

    # Explicit mappings
    if name in EXPLICIT_MAP:
        name = EXPLICIT_MAP[name]

    # Normalize "&" / "and" to match existing
    if " and " in name:
        alt = name.replace(" and ", " & ")
        if alt in existing_names:
            name = alt

    return name


def load_existing_stations():
    """Load existing stations.csv into a dict keyed by name."""
    stations = {}
    max_fid = -1
    max_objectid = -1
    with open(STATIONS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stations[row["NAME"]] = row
            fid = int(row["FID"])
            oid = int(row["OBJECTID"])
            if fid > max_fid:
                max_fid = fid
            if oid > max_objectid:
                max_objectid = oid
    return stations, max_fid, max_objectid


def load_existing_connections():
    """Load existing connections.csv into a set of (s1, s2, line)."""
    conns = set()
    rows = []
    with open(CONNECTIONS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conns.add((row["station1"], row["station2"], row["line"]))
            rows.append(row)
    return conns, rows


def fetch_operator_data(line_id, display_name):
    """Fetch stations and route data for one operator."""
    print(f"Fetching {display_name} ({line_id})...")

    # Fetch stop points
    url = f"{API_BASE}/Line/{line_id}/StopPoints"
    stop_data = fetch_json(url)

    stations = {}
    for s in stop_data:
        lat = s.get("lat")
        lon = s.get("lon")
        if lat is None or lon is None:
            continue
        # Filter to Greater London bounding box
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            continue
        naptan = s.get("naptanId", "")
        name = s.get("commonName", "")
        zone = ""
        for prop in s.get("additionalProperties", []):
            if prop.get("key") == "Zone":
                zone = prop.get("value", "")
                break
        stations[naptan] = {
            "name": name,
            "lat": lat,
            "lon": lon,
            "zone": zone,
            "naptan": naptan,
        }

    print(f"  {len(stations)} stations in Greater London")

    # Fetch route sequences
    url = f"{API_BASE}/Line/{line_id}/Route/Sequence/outbound"
    route_data = fetch_json(url)

    connections = []
    for route in route_data.get("orderedLineRoutes", []):
        naptan_ids = route.get("naptanIds", [])
        for i in range(len(naptan_ids) - 1):
            n1, n2 = naptan_ids[i], naptan_ids[i + 1]
            # Both stations must be in our filtered set
            if n1 in stations and n2 in stations:
                connections.append((n1, n2))

    # Deduplicate connections
    seen = set()
    unique_conns = []
    for c in connections:
        key = tuple(sorted(c))
        if key not in seen:
            seen.add(key)
            unique_conns.append(c)

    print(f"  {len(unique_conns)} connections")
    return stations, unique_conns


def main():
    # Load existing data
    existing_stations, max_fid, max_objectid = load_existing_stations()
    existing_conns, existing_rows = load_existing_connections()
    existing_names = set(existing_stations.keys())

    print(f"Existing: {len(existing_stations)} stations, "
          f"{len(existing_conns)} connections")
    print(f"Max FID={max_fid}, Max OBJECTID={max_objectid}\n")

    # Collect all new data
    # naptan -> station info (with normalized name)
    all_stations = {}
    # (name1, name2, operator_display) tuples
    all_connections = []
    # station_name -> set of operator display names
    station_operators = {}

    for line_id, display_name in OPERATORS:
        stations, connections = fetch_operator_data(line_id, display_name)

        # Normalize names and record operator
        for naptan, info in stations.items():
            norm_name = normalize_name(info["name"], existing_names)
            info["norm_name"] = norm_name
            all_stations[naptan] = info
            if norm_name not in station_operators:
                station_operators[norm_name] = set()
            station_operators[norm_name].add(display_name)

        # Convert naptan connections to name connections
        for n1, n2 in connections:
            if n1 in all_stations and n2 in all_stations:
                name1 = all_stations[n1]["norm_name"]
                name2 = all_stations[n2]["norm_name"]
                if name1 != name2:
                    all_connections.append(
                        (name1, name2, display_name))

        time.sleep(0.5)  # Be polite to the API

    # Build name -> best station info (prefer the one with coords)
    name_to_info = {}
    for naptan, info in all_stations.items():
        name = info["norm_name"]
        if name not in name_to_info:
            name_to_info[name] = info
        elif info.get("zone") and not name_to_info[name].get("zone"):
            name_to_info[name] = info

    # Compute travel times for connections
    new_conn_rows = []
    conn_seen = set()
    for name1, name2, operator in all_connections:
        # Find station info by name
        info1 = name_to_info.get(name1)
        info2 = name_to_info.get(name2)
        if not info1 or not info2:
            continue

        key = (name1, name2, operator)
        rev_key = (name2, name1, operator)
        if key in conn_seen or rev_key in conn_seen:
            continue
        conn_seen.add(key)

        # Skip if already exists
        if key in existing_conns or rev_key in existing_conns:
            continue

        dist_km = haversine_km(
            info1["lat"], info1["lon"],
            info2["lat"], info2["lon"])
        travel_time = max(1.0, round(dist_km / AVG_SPEED_KMH * 60.0))

        new_conn_rows.append({
            "station1": name1,
            "station2": name2,
            "line": operator,
            "time": f"{travel_time:.1f}",
        })

    # Add interchange edges
    for s1, s2, line, t in INTERCHANGE_EDGES:
        key = (s1, s2, line)
        rev_key = (s2, s1, line)
        if (key not in existing_conns and rev_key not in existing_conns
                and key not in conn_seen and rev_key not in conn_seen):
            new_conn_rows.append({
                "station1": s1,
                "station2": s2,
                "line": line,
                "time": f"{t:.1f}",
            })

    # Determine new stations (not in existing)
    new_station_names = sorted(
        set(name_to_info.keys()) - existing_names)

    print(f"\nNew stations to add: {len(new_station_names)}")
    print(f"New connections to add: {len(new_conn_rows)}")

    # Make sure all connection endpoints exist (either existing or new)
    all_known = existing_names | set(new_station_names)
    dropped = 0
    filtered_conns = []
    for row in new_conn_rows:
        if row["station1"] in all_known and row["station2"] in all_known:
            filtered_conns.append(row)
        else:
            dropped += 1
    if dropped:
        print(f"Dropped {dropped} connections with unknown endpoints")
    new_conn_rows = filtered_conns

    # Append to stations.csv
    next_fid = max_fid + 1
    next_objectid = max_objectid + 1
    new_station_rows = []
    for name in new_station_names:
        info = name_to_info[name]
        easting, northing = latlon_to_osgrid(info["lat"], info["lon"])
        operators = ", ".join(sorted(station_operators.get(name, set())))
        zone = info.get("zone", "")
        if not zone:
            zone = "0"
        new_station_rows.append({
            "FID": str(next_fid),
            "OBJECTID": str(next_objectid),
            "NAME": name,
            "EASTING": str(easting),
            "NORTHING": str(northing),
            "LINES": operators,
            "NETWORK": "National Rail",
            "Zone": zone,
            "x": f"{info['lon']:.9f}" if info["lon"] else "",
            "y": f"{info['lat']:.8f}" if info["lat"] else "",
        })
        next_fid += 1
        next_objectid += 1

    # Write updated stations.csv
    with open(STATIONS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["FID", "OBJECTID", "NAME", "EASTING",
                         "NORTHING", "LINES", "NETWORK", "Zone",
                         "x", "y"])
        for row in new_station_rows:
            writer.writerow(row)

    # Write updated connections.csv
    with open(CONNECTIONS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["station1", "station2", "line", "time"])
        for row in new_conn_rows:
            writer.writerow(row)

    print(f"\nDone!")
    print(f"  stations.csv: {len(existing_stations)} existing "
          f"+ {len(new_station_rows)} new "
          f"= {len(existing_stations) + len(new_station_rows)} total")
    print(f"  connections.csv: {len(existing_conns)} existing "
          f"+ {len(new_conn_rows)} new "
          f"= {len(existing_conns) + len(new_conn_rows)} total")

    # Print some stats
    all_conn_stations = set()
    for row in new_conn_rows:
        all_conn_stations.add(row["station1"])
        all_conn_stations.add(row["station2"])
    shared = all_conn_stations & existing_names
    print(f"\n  Shared stations (auto-interchange): {len(shared)}")
    if shared:
        for s in sorted(shared)[:20]:
            print(f"    {s}")
        if len(shared) > 20:
            print(f"    ... and {len(shared) - 20} more")


if __name__ == "__main__":
    main()
