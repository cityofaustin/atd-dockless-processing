'''
Download new data from an MDS provider and load it to db.
'''
from datetime import datetime
import pytz
import logging
import pdb

from mds_provider_client import *
import requests
from pypgrest import Postgrest
from tdutils import argutil

from config import secrets
from config import config


def get_token(url, data):
    res = requests.post(url, data=data)
    res.raise_for_status()
    return res.json()["access_token"]


def most_recent(client, provider_id, key="end_time"):
    """
    Return the most recent trip record for the given provider
    """
    results = client.select(
        {
            "select": f"{key}",
            "provider_id": f"eq.{provider_id}",
            "limit": 1,
            "order": f"{key}.desc",
        }
    )

    return results[0].get(key)


def cli_args():
    parser = argutil.get_parser(
        "mds.py",
        "Extract data from MDS endpoint and load to staging database.",
        "provider_name",
        "--start",
        "--end",
        "--replace",
    )

    args = parser.parse_args()

    return args


def get_data(client, end_time, interval, paging):

    data = client.get_trips(
        start_time=end_time - interval,
        end_time=end_time, paging=paging
    )

    return data


def get_coords(feature):
    if feature["geometry"]["coordinates"]:
        return feature["geometry"]["coordinates"]
    else:
        return [None, None]


def parse_routes(trips):
    for trip in trips:
        trip["start_longitude"], trip["start_latitude"] = get_coords(
            trip["route"]["features"][0]
        )

        trip["end_longitude"], trip["end_latitude"] = get_coords(
            trip["route"]["features"][-1]
        )

        trip.pop("route")
    return trips


def to_unix(iso_string, tz_string="+00:00"):
    # handle format YYYY-MM-DDTHH:MM:SS+00:00 (timezone ignored)
    iso_string = iso_string.replace(tz_string, "")
    dt = datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.UTC)
    return int(dt.timestamp())


def to_iso(unix):
    if unix < 4_100_264_520:
        return datetime.utcfromtimestamp(int(unix)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        # try milliseconds instead
        return datetime.utcfromtimestamp(int(unix / 1000)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )


def floats_to_iso(data, keys):
    for row in data:
        for key in keys:
            row[key] = to_iso(row[key])
    return data


def drop_dupes(trips, key="trip_id"):
    ids = []
    new_trips = []

    for trip in trips:
        if trip[key] not in ids:
            ids.append(trip[key])
            new_trips.append(trip)
        else:
            print(trip[key])

    return new_trips


def post_data(client, data):
    print("Post {} trips...".format(len(data)))
    return client.upsert(data)


def main():

    args = cli_args()

    provider_name = args.provider_name

    cfg = secrets.PROVIDERS[provider_name]

    pgrest = Postgrest(secrets.PG["url"], auth=secrets.PG["token"])

    start = args.start
    end = args.end

    if not start:
        # use the most recent record as the start date
        start = most_recent(pgrest, cfg["provider_id"])
        start = to_unix(start)

    if not end:
        # use current time as the end date
        end = int(datetime.today().timestamp())

    interval = cfg["interval"]

    if cfg.pop("time_format") == "mills":
        # mills to unix
        start, end, interval = int(start * 1000), int(end * 1000), int(interval * 1000)

    if not cfg["client_params"].get("token"):
        auth_info = cfg.get("oauth")
        url = auth_info.pop("url")
        cfg["client_params"]["token"] = get_token(url, auth_info)

    client = ProviderClient(**cfg["client_params"])

    total = 0

    for i in range(start, end, interval):
        data = get_data(client, i, interval, cfg["paging"])
        
        data = parse_routes(data)

        data = drop_dupes(data)
        
        data = [{field['name']: row[field['name']] for field in config.FIELDS if field.get('upload_mds') } for row in data]
        
        data = floats_to_iso(data, [ field['name'] for field in config.FIELDS if field.get('datetime') ] )

        if data:
            results = post_data(pgrest, data)

        total += len(data)

    return total


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG)

    main()
