#!/usr/bin/env python3
"""
Fetch the UK public transport network from official open data and write it
as CSV files: every stop, every operator, and every route with its stops.

Sources:
    National Rail Data Portal  rail stations, train operators, rail timetable
    Bus Open Data Service      bus, coach, tram, metro and ferry timetables (GTFS)
    NaPTAN                     every public transport stop in Great Britain

Usage:
    python3 fetch_uk_transport.py [--credentials ~/uktransport.env] [--output data]

The credentials file holds NRE_USERNAME and NRE_PASSWORD for the National Rail
Data Portal, one KEY=VALUE per line. Environment variables take precedence.

Outputs, in the output directory:
    stops.csv        NaPTAN stops, keyed by ATCO code
    stations.csv     National Rail stations, keyed by CRS code
    operators.csv    train operating companies and bus, coach and ferry operators
    routes.csv       one row per distinct stop sequence of a line
    route_stops.csv  the stops of each route in order, timed from the first stop
    transfers.csv    two-way links between stops: National Rail interchange links,
                     and a walk between any two served stops within 200 m
    raw/             the downloaded feeds, reused by the next run
"""

import argparse
import collections
import csv
import dataclasses
import io
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ElementTree
import zipfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = "TuringDB-Sample/1.0"

NRE_BASE = "https://opendata.nationalrail.co.uk"
NRE_FEEDS = {
    "nre_stations.xml": "/api/staticfeeds/4.0/stations",
    "nre_tocs.xml": "/api/staticfeeds/4.0/tocs",
    "nre_timetable.zip": "/api/staticfeeds/3.0/timetable",
}
BODS_GTFS_URL = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/all/"
NAPTAN_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"

STATION_NAMESPACE = {"s": "http://nationalrail.co.uk/xml/station"}
TOC_NAMESPACE = {"t": "http://nationalrail.co.uk/xml/toc"}

PASSENGER_STOP_ACTIVITIES = {"T ", "U ", "D ", "R "}
SUBSIDIARY_TIPLOC_CATEGORY = "9"
NAPTAN_RAIL_PREFIX = "9100"
RAIL_LINK_PATTERN = re.compile(r"ADDITIONAL LINK: (\w+) BETWEEN (\w+) AND (\w+) IN +(\d+) MINUTES")

WALKING_LINK_METRES = 200
WALKING_METRES_PER_MINUTE = 80
METRES_PER_DEGREE = 111_195

GTFS_ROUTE_MODES = {
    "0": "tram",
    "1": "metro",
    "2": "rail",
    "3": "bus",
    "4": "ferry",
    "5": "cable_tram",
    "6": "aerial_lift",
    "7": "funicular",
    "11": "trolleybus",
    "12": "monorail",
    "200": "coach",
}
GTFS_NO_PICKUP_OR_DROP_OFF = "1"

STOP_COLUMNS = ["atco_code", "naptan_code", "name", "indicator", "locality",
                "stop_type", "latitude", "longitude", "status"]
STOP_STATUS = STOP_COLUMNS.index("status")
STOP_LATITUDE = STOP_COLUMNS.index("latitude")
STOP_LONGITUDE = STOP_COLUMNS.index("longitude")


@dataclasses.dataclass
class Route:
    mode: str
    operator: str
    line: str
    stops: tuple
    times: list
    services: int = 1


def load_credentials(path, keys):
    credentials = {}
    if os.path.exists(path):
        with open(path) as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    credentials[key.strip()] = value.strip()

    for key in keys:
        if key in os.environ:
            credentials[key] = os.environ[key]
        if not credentials.get(key):
            sys.exit(f"Missing {key}: set it in {path} or in the environment")

    return credentials


def authenticate_nre(credentials):
    body = urllib.parse.urlencode({"username": credentials["NRE_USERNAME"],
                                   "password": credentials["NRE_PASSWORD"]}).encode()
    request = urllib.request.Request(f"{NRE_BASE}/authenticate",
                                     data=body,
                                     headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)["token"]


def download(url, path, headers):
    print(f"Downloading {url}")
    partial_path = path + ".part"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **headers})
    with urllib.request.urlopen(request, timeout=600) as response:
        with open(partial_path, "wb") as file:
            while chunk := response.read(1 << 20):
                file.write(chunk)
    os.replace(partial_path, path)
    print(f"  {os.path.getsize(path) / 1e6:.1f} MB")


def download_feeds(raw_dir, credentials):
    token = None
    for name, path in NRE_FEEDS.items():
        target = os.path.join(raw_dir, name)
        if not os.path.exists(target):
            token = token or authenticate_nre(credentials)
            download(NRE_BASE + path, target, {"X-Auth-Token": token})

    for name, url in (("bods_gtfs.zip", BODS_GTFS_URL), ("naptan.csv", NAPTAN_URL)):
        target = os.path.join(raw_dir, name)
        if not os.path.exists(target):
            download(url, target, {})


def write_csv(path, header, rows):
    with open(path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


def open_text(archive, name):
    return io.TextIOWrapper(archive.open(name), encoding="utf-8-sig", newline="")


def find_member(archive, extension):
    for name in archive.namelist():
        if name.upper().endswith(extension):
            return name
    sys.exit(f"No {extension} file in the rail timetable archive")


def read_naptan(path):
    stops = {}
    with open(path, newline="", encoding="utf-8-sig") as file:
        for row in csv.DictReader(file):
            stops[row["ATCOCode"]] = [
                row["ATCOCode"],
                row["NaptanCode"],
                row["CommonName"],
                row["Indicator"],
                row["LocalityName"],
                row["StopType"],
                row["Latitude"],
                row["Longitude"],
                row["Status"],
            ]
    return stops


def read_master_station_names(archive):
    """Map each TIPLOC of the rail timetable to its MSN record:
    (CRS, interchange category, minimum change minutes)."""
    tiplocs = {}
    for line in io.TextIOWrapper(archive.open(find_member(archive, ".MSN")),
                                 encoding="latin-1"):
        tiploc = line[36:43].strip()
        if line.startswith("A ") and tiploc:
            tiplocs[tiploc] = (line[49:52], line[35], line[63:65].strip())
    return tiplocs


def map_stations_to_naptan(msn, naptan):
    principal = {}
    candidates = {}
    for tiploc, (crs, category, change_minutes) in msn.items():
        if category != SUBSIDIARY_TIPLOC_CATEGORY and crs not in principal:
            principal[crs] = (tiploc, category, change_minutes)
        candidates.setdefault(crs, []).append(tiploc)

    station_stops = {}
    for crs, tiplocs in candidates.items():
        if crs in principal:
            tiplocs = [principal[crs][0]] + tiplocs
        for tiploc in tiplocs:
            if NAPTAN_RAIL_PREFIX + tiploc in naptan:
                station_stops[crs] = NAPTAN_RAIL_PREFIX + tiploc
                break
    return principal, station_stops


def read_stations(path, principal, station_stops):
    rows = []
    root = ElementTree.parse(path).getroot()
    for station in root.findall("s:Station", STATION_NAMESPACE):
        crs = station.findtext("s:CrsCode", "", STATION_NAMESPACE)
        tiploc, category, change_minutes = principal.get(crs, ("", "", ""))
        rows.append([
            crs,
            station.findtext("s:Name", "", STATION_NAMESPACE),
            station_stops.get(crs, ""),
            tiploc,
            station.findtext("s:AlternativeIdentifiers/s:NationalLocationCode",
                             "", STATION_NAMESPACE),
            station.findtext("s:StationOperator", "", STATION_NAMESPACE),
            station.findtext("s:Latitude", "", STATION_NAMESPACE),
            station.findtext("s:Longitude", "", STATION_NAMESPACE),
            category,
            change_minutes,
        ])
    return rows


def read_train_operators(path):
    rows = []
    root = ElementTree.parse(path).getroot()
    for operator in root.findall("t:TrainOperatingCompany", TOC_NAMESPACE):
        rows.append([
            operator.findtext("t:AtocCode", "", TOC_NAMESPACE),
            operator.findtext("t:Name", "", TOC_NAMESPACE),
            "",
            operator.findtext("t:CompanyWebsite", "", TOC_NAMESPACE),
        ])
    return rows


def read_gtfs_operators(archive):
    return [[row["agency_id"], row["agency_name"], row["agency_noc"], row["agency_url"]]
            for row in csv.DictReader(open_text(archive, "agency.txt"))]


def parse_cif_time(field):
    field = field.strip()
    if not field:
        return None
    minutes = int(field[0:2]) * 60 + int(field[2:4])
    return minutes + 0.5 if field[4:5] == "H" else minutes


def parse_gtfs_time(field):
    if not field:
        return None
    hours, minutes, seconds = field.split(":")
    return int(hours) * 60 + int(minutes) + int(seconds) / 60


def is_passenger_stop(activity):
    return any(activity[index:index + 2] in PASSENGER_STOP_ACTIVITIES
               for index in range(0, len(activity), 2))


def add_trip(routes, key, mode, operator, line, stops, times):
    route = routes.get(key)
    if route:
        route.services += 1
    else:
        routes[key] = Route(mode, operator, line, stops, times())


def read_rail_routes(archive, msn, station_stops, routes):
    """Add the permanent passenger trains of the CIF timetable. A train whose
    calling points are not all National Rail stations is skipped."""
    skipped = {}
    calls = None
    operator = ""

    def finish_train():
        stops = []
        for tiploc, _, _ in calls:
            stop = station_stops.get(msn.get(tiploc, ("",))[0])
            if stop is None:
                skipped[operator] = skipped.get(operator, 0) + 1
                return
            stops.append(stop)

        stops = tuple(stops)
        add_trip(routes, ("rail", operator, stops), "rail", operator, "", stops,
                 lambda: [(arrival, departure) for _, arrival, departure in calls])

    for line in io.TextIOWrapper(archive.open(find_member(archive, ".MCA")),
                                 encoding="latin-1"):
        record = line[0:2]
        if record == "BS":
            is_permanent_passenger = line[79] == "P" and line[29] == "P"
            calls = [] if is_permanent_passenger else None
        elif calls is None:
            continue
        elif record == "BX":
            operator = line[11:13]
        elif record == "LO":
            calls.append((line[2:9].strip(), None, parse_cif_time(line[10:15])))
        elif record == "LI" and is_passenger_stop(line[42:54]):
            calls.append((line[2:9].strip(),
                          parse_cif_time(line[10:15]),
                          parse_cif_time(line[15:20])))
        elif record == "LT":
            calls.append((line[2:9].strip(), parse_cif_time(line[10:15]), None))
            finish_train()
            calls = None

    for operator, count in sorted(skipped.items()):
        print(f"Skipped {count} {operator} trains calling outside National Rail stations")


def read_gtfs_routes(archive, routes):
    """Add every GTFS trip. GTFS rows of a trip are contiguous but not
    always in stop_sequence order."""
    lines = {}
    for row in csv.DictReader(open_text(archive, "routes.txt")):
        mode = GTFS_ROUTE_MODES.get(row["route_type"], row["route_type"])
        line = row["route_short_name"] or row["route_long_name"]
        lines[row["route_id"]] = (mode, row["agency_id"], line)

    trip_lines = {}
    for row in csv.DictReader(open_text(archive, "trips.txt")):
        trip_lines[row["trip_id"]] = row["route_id"]

    reader = csv.reader(open_text(archive, "stop_times.txt"))
    header = next(reader)
    trip_column = header.index("trip_id")
    sequence_column = header.index("stop_sequence")
    stop_column = header.index("stop_id")
    arrival_column = header.index("arrival_time")
    departure_column = header.index("departure_time")
    pickup_column = header.index("pickup_type")
    drop_off_column = header.index("drop_off_type")

    def finish_trip():
        if len(calls) < 2:
            return
        calls.sort()
        route_id = trip_lines[trip]
        mode, operator, line = lines[route_id]
        stops = tuple(sys.intern(call[1]) for call in calls)
        add_trip(routes, ("gtfs", route_id, stops), mode, operator, line, stops,
                 lambda: [(parse_gtfs_time(arrival), parse_gtfs_time(departure))
                          for _, _, arrival, departure in calls])

    trip = None
    calls = []
    for row in reader:
        if row[trip_column] != trip:
            if trip is not None:
                finish_trip()
            trip = row[trip_column]
            calls = []

        is_passing_point = (row[pickup_column] == GTFS_NO_PICKUP_OR_DROP_OFF
                            and row[drop_off_column] == GTFS_NO_PICKUP_OR_DROP_OFF)
        if not is_passing_point:
            calls.append((int(row[sequence_column]),
                          row[stop_column],
                          row[arrival_column],
                          row[departure_column]))
    if trip is not None:
        finish_trip()


def read_gtfs_stops(archive, stop_ids):
    rows = []
    for row in csv.DictReader(open_text(archive, "stops.txt")):
        if row["stop_id"] in stop_ids:
            rows.append([row["stop_id"], row["stop_code"], row["stop_name"], "", "", "",
                         row["stop_lat"], row["stop_lon"], ""])
    return rows


def read_rail_links(archive, station_stops):
    links = []
    for line in io.TextIOWrapper(archive.open(find_member(archive, ".FLF")),
                                 encoding="latin-1"):
        match = RAIL_LINK_PATTERN.match(line)
        if match:
            mode, first_crs, second_crs, minutes = match.groups()
            links.append((station_stops.get(first_crs),
                          station_stops.get(second_crs),
                          mode.lower(),
                          int(minutes)))
    return links


def distance_metres(first, second):
    north = (second[0] - first[0]) * METRES_PER_DEGREE
    east = (second[1] - first[1]) * METRES_PER_DEGREE * math.cos(math.radians(first[0]))
    return math.hypot(north, east)


def build_transfer_rows(rail_links, coordinates):
    rows = []
    linked_pairs = set()
    for first, second, mode, minutes in rail_links:
        if first in coordinates and second in coordinates:
            metres = round(distance_metres(coordinates[first], coordinates[second]))
            rows.append([first, second, mode, minutes, metres])
            linked_pairs.add(tuple(sorted((first, second))))
    print(f"Kept {len(rows)} of {len(rail_links)} National Rail links, "
          f"the others have an end no route calls at")

    # Grid cells are at least WALKING_LINK_METRES wide everywhere, the
    # northernmost stop included, so every pair in range is in adjacent cells.
    latitude_cell = WALKING_LINK_METRES / METRES_PER_DEGREE
    northernmost = max(latitude for latitude, _ in coordinates.values())
    longitude_cell = latitude_cell / math.cos(math.radians(northernmost))
    cells = collections.defaultdict(list)
    for stop, (latitude, longitude) in coordinates.items():
        cell = (math.floor(latitude / latitude_cell), math.floor(longitude / longitude_cell))
        cells[cell].append(stop)

    for (row, column), stops in cells.items():
        for row_offset in (-1, 0, 1):
            for column_offset in (-1, 0, 1):
                for other in cells.get((row + row_offset, column + column_offset), ()):
                    for stop in stops:
                        if stop >= other or (stop, other) in linked_pairs:
                            continue
                        metres = distance_metres(coordinates[stop], coordinates[other])
                        if metres <= WALKING_LINK_METRES:
                            rows.append([stop,
                                         other,
                                         "walk",
                                         round(metres / WALKING_METRES_PER_MINUTE, 1),
                                         round(metres)])
    return rows


def minutes_since(origin, time):
    if time is None:
        return ""
    elapsed = time - origin
    if elapsed < 0:
        elapsed += 24 * 60
    return f"{round(elapsed, 2):g}"


def build_route_rows(routes):
    route_rows = []
    stop_rows = []
    for route_id, route in enumerate(routes.values(), 1):
        stops = route.stops
        route_rows.append([
            route_id,
            route.mode,
            route.operator,
            route.line,
            stops[0],
            stops[-1],
            len(stops),
            route.services,
        ])

        first_arrival, first_departure = route.times[0]
        origin = first_departure if first_departure is not None else first_arrival
        for sequence, (stop, (arrival, departure)) in enumerate(zip(stops, route.times), 1):
            stop_rows.append([
                route_id,
                sequence,
                stop,
                minutes_since(origin, arrival),
                minutes_since(origin, departure),
            ])
    return route_rows, stop_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--credentials", default=os.path.expanduser("~/uktransport.env"))
    parser.add_argument("--output", default=os.path.join(SCRIPT_DIR, "data"))
    args = parser.parse_args()

    raw_dir = os.path.join(args.output, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    credentials = load_credentials(args.credentials, ["NRE_USERNAME", "NRE_PASSWORD"])
    download_feeds(raw_dir, credentials)

    naptan = read_naptan(os.path.join(raw_dir, "naptan.csv"))
    routes = {}

    with zipfile.ZipFile(os.path.join(raw_dir, "nre_timetable.zip")) as archive:
        msn = read_master_station_names(archive)
        principal, station_stops = map_stations_to_naptan(msn, naptan)
        write_csv(os.path.join(args.output, "stations.csv"),
                  ["crs", "name", "atco_code", "tiploc", "nlc", "operator", "latitude",
                   "longitude", "interchange", "change_minutes"],
                  read_stations(os.path.join(raw_dir, "nre_stations.xml"),
                                principal,
                                station_stops))
        read_rail_routes(archive, msn, station_stops, routes)
        rail_links = read_rail_links(archive, station_stops)

    with zipfile.ZipFile(os.path.join(raw_dir, "bods_gtfs.zip")) as archive:
        operators = read_train_operators(os.path.join(raw_dir, "nre_tocs.xml"))
        operators += read_gtfs_operators(archive)
        write_csv(os.path.join(args.output, "operators.csv"),
                  ["operator_id", "name", "noc", "website"],
                  operators)

        read_gtfs_routes(archive, routes)

        served = {stop for route in routes.values() for stop in route.stops}
        stops = [row for atco_code, row in naptan.items()
                 if row[STOP_STATUS] == "active" or atco_code in served]
        stops += read_gtfs_stops(archive, served - naptan.keys())
        write_csv(os.path.join(args.output, "stops.csv"), STOP_COLUMNS, stops)

    coordinates = {row[0]: (float(row[STOP_LATITUDE]), float(row[STOP_LONGITUDE]))
                   for row in stops
                   if row[0] in served and row[STOP_LATITUDE] and row[STOP_LONGITUDE]}
    write_csv(os.path.join(args.output, "transfers.csv"),
              ["atco_code1", "atco_code2", "mode", "minutes", "metres"],
              build_transfer_rows(rail_links, coordinates))

    route_rows, stop_rows = build_route_rows(routes)
    write_csv(os.path.join(args.output, "routes.csv"),
              ["route_id", "mode", "operator_id", "line", "origin", "destination",
               "stops", "services"],
              route_rows)
    write_csv(os.path.join(args.output, "route_stops.csv"),
              ["route_id", "sequence", "atco_code", "arrival_minutes", "departure_minutes"],
              stop_rows)


if __name__ == "__main__":
    main()
