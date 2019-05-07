'''
Get MDS data async'ly. For loading backlog data.
'''
from datetime import datetime
import logging
import time
from multiprocessing.dummy import Pool as ThreadPool
import pdb

from mds_provider_client import *
import pytz
import requests
from tdutils import datautil, argutil
from pypgrest import Postgrest

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


def min_max(trips):
    min_ = 9_999_999_999_000
    max_ = 0

    for i, trip in enumerate(trips):
        s = to_iso(trip["start_time"])
        e = to_iso(trip["end_time"])
        print("TRIP START {} END {}".format(s, e))
        if trip["start_time"] < min_:
            min_ = trip["start_time"]

        if trip["end_time"] > max_:
            max_ = trip["end_time"]

    return min_, max_


def drop_dupes(trips, key="trip_id"):
    '''
    Make sure there are no duplicate records in the payload
    (upsert cannot handle dupes in a single payload)
    '''
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

    def async_wrapper(end_time):
        try:
            t0 = time.time()

            data = get_data(client, end_time, interval, paging)

            elapsed = time.time() - t0

            logging.info('Request Time: {} seconds for {} records'.format(elapsed, len(data)))

            min_max(data)

            data = parse_routes(data)

            data = drop_dupes(data)

            data = [{field: row[field] for field in config.FIELDS if field.get('upsert_mds') } for row in data]

            data = floats_to_iso(data, [ field for field in config.FIELDS if field.get('datetime') ] )

            if data:
                results = post_data(pgrest, data)
                return data

        except Exception as e:
            logging.error(e)
            logging.error("{} to {} FAILED".format(end_time - interval, end_time))

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
    paging = cfg["paging"]

    if cfg.pop("time_format") == "mills":
        # mills to unix
        start, end, interval = int(start * 1000), int(end * 1000), int(interval * 1000)

    if not cfg["client_params"].get("token"):
        auth_info = cfg.get("oauth")
        url = auth_info.pop("url")
        cfg["client_params"]["token"] = get_token(url, auth_info)

    client = ProviderClient(**cfg["client_params"])

    total = 0
    workers = 6
    pool = ThreadPool(workers)
    
    results = pool.map(async_wrapper, range(start, end, interval))

    pool.close()

    pool.join()

    for result in results:
        if result:
            total += len(result)

    logging.info(total)
    return total


if __name__ == "__main__":
    import logging
    logging.basicConfig(filename="async_get_data.log", level=logging.DEBUG)

    main()
