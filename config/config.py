FIELDS = [
    {
        "name": "provider_id",
        "upload_mds": True,
        "upload_postgrest": True,
        "datetime": False,
    },
    {
        "name": "provider_name",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "device_id",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "vehicle_type",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "trip_id",
        "upload_mds": True,
        "upload_postgrest": True,
        "datetime": False,
    },
    {
        "name": "trip_duration",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "trip_distance",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "accuracy",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "start_time",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": True,
    },
    {
        "name": "end_time",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": True,
    },
    {
        "name": "propulsion_type",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "start_latitude",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "start_longitude",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "end_latitude",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "end_longitude",
        "upload_mds": True,
        "upload_postgrest": False,
        "datetime": False,
    },
    {
        "name": "orig_cell_id",
        "upload_mds": False,
        "upload_postgrest": True,
        "datetime": False,
    },
    {
        "name": "dest_cell_id",
        "upload_mds": False,
        "upload_postgrest": True,
        "datetime": False,
    },
    {
        "name": "council_district_start",
        "upload_mds": False,
        "upload_postgrest": True,
        "datetime": False,
    },
    {
        "name": "council_district_end",
        "upload_mds": False,
        "upload_postgrest": True,
        "datetime": False,
    },
]

GRID_GEOJSON = "data/hex1000.json"

DISTRICTS_GEOJSON = "data/council_districts_simplified.json"