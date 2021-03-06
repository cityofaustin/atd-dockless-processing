"""
Download trips from db, extract attributes from overlapping poly, update records in db.
"""
import logging
import json
import os
import pdb
import time

import requests
from rtree import index
from shapely.geometry import shape, point
from pypgrest import Postgrest

import _setpath
from config import secrets
from config import config


def main():
    def split_trips(data, fields):
        """
        Split trip into separate rows (one for trip start and one fore trip end)
        """
        split = []
        for row in data:
            new_row = {
                "type": "start",
                "x": row[fields["trip_start_x_field"]],
                "y": row[fields["trip_start_y_field"]],
            }

            new_row.update(**row)

            split.append(new_row)

            new_row = {
                "type": "end",
                fields["trip_id_field"]: row[fields["trip_id_field"]],
                "x": row[fields["trip_end_x_field"]],
                "y": row[fields["trip_end_y_field"]],
            }

            new_row.update(**row)

            split.append(new_row)

        return split

    def create_points(data):
        """
        Create shapely geometry from list of dicts
        """
        for row in data:

            if row["x"] and row["y"]:
                try:
                    row["geometry"] = point.Point(float(row["x"]), float(row["y"]))
                except:
                    row["geometry"] = None
            else:
                row["geometry"] = None

        return data

    def read_json(f):
        """
        Load (geo)JSON into memory
        """
        with open(f, "r") as fin:
            return json.loads(fin.read())

    def point_in_poly(
        points=None,
        polys=None,
        row_property_key="cell_id",
        feature_property_key="id",
        geom_key="geometry",
        null_val=None,
    ):
        """
        Get property of polygon that intersects input point. Assumes input polygons
        do not overlap.
    
        points: list of dicts with shapely point geometries
        
        polys: geojson polygon feature collection
        
        row_property_key: the property name of the input point that will be assigned a
            value from intersecting the polygon 
        
        feature_property_key: the property name of the intersecting polygon that will
            be assigned to intersecting point(s)

        geom_key: the name of the feature proerty which contains the geometry of the
            feature. Applies to both input points and polygons

        null_val: the value which will be assigned to `<row_property_key>` if no
            intersecting polygon is found.

        Returns points with updated <row_property_key>
        """
        start_time = time.time()
        count = 0

        # create grid cell index of polygon *bounding boxes*
        idx = index.Index()
        for pos, feature in enumerate(polys["features"]):
            idx.insert(pos, shape(feature[geom_key]).bounds)

        # find intersecting polygon
        for i, pt in enumerate(points):

            if pt[geom_key]:

                matched = False

                # iterate through polygon *bounding boxes* that intersect with point
                for intersect_pos in idx.intersection(pt[geom_key].coords[0]):

                    poly = shape(polys["features"][intersect_pos][geom_key])

                    # check if point intersects actual polygon
                    if pt[geom_key].intersects(poly):
                        pt.update(
                            {
                                row_property_key: polys["features"][intersect_pos][
                                    "properties"
                                ][feature_property_key]
                            }
                        )
                        matched = True

                        # break because we assume there are no overlapping polygons
                        break

                if not matched:
                    #  add empty cell ID rows not contained by poly
                    count += 1
                    pt.update({row_property_key: null_val})
            else:
                count += 1
                pt.update({row_property_key: null_val})

        elapsed_time = time.time() - start_time
        print(f"{count} points outside the input poly(s)")
        print(f"{elapsed_time} seconds for point in poly")
        return points

    def merge_trips(trips, id_field="trip_id"):
        #  merge start and end trips to single trip with start and end cell
        new_trips = {}

        for trip in trips:
            trip_id = trip[id_field]
            trip_type = trip.get("type")

            geoid = trip.get("census_geoid")
            district = trip.get("council_district")

            if trip_type == "start":
                current_data = {
                    "census_geoid_start": geoid,
                    "council_district_start": district,
                }

            elif trip_type == "end":
                current_data = {
                    "census_geoid_end": geoid,
                    "council_district_end": district,
                }

            if trip_id in new_trips:
                new_trips[trip_id].update(current_data)

            else:
                new_trips[trip_id] = current_data
                new_trips[trip_id].update(**trip)

        return [new_trips[trip] for trip in new_trips.keys()]

    def reduce_fields(data, fieldnames):
        return [{key: row[key] for key in fieldnames} for row in data]

    def post_trips(client, trips):
        print("upsert!")
        print(len(trips))
        return client.upsert(trips)

    field_map = {
        "trip_id_field": "trip_id",
        "trip_start_y_field": "start_latitude",
        "trip_start_x_field": "start_longitude",
        "trip_end_y_field": "end_latitude",
        "trip_end_x_field": "end_longitude",
    }

    # move working directory to script location to
    # ensure relative paths work (in case script
    # is run by external launcher)
    if os.path.dirname(__file__):
        os.chdir(os.path.dirname(__file__))

    districts = read_json(config.DISTRICTS_GEOJSON)
    census_tracts = read_json(config.CENSUS_TRACTS_GEOJSON)

    # num of records that will be processed per request
    # not to exceed the postgrest db request limit, which you need to know
    interval = 5000
    total = 0

    pgrest = Postgrest(secrets.PG["url"], auth=secrets.PG["token"])

    while True:
        # loop until request no longer yields trips

        params = {
            "select": "trip_id,provider_id,start_latitude,start_longitude,end_latitude,end_longitude",
            "census_geoid_start": "is.null",  # assume if census geoid is null the record has not been processed
            "limit": interval,
        }

        print("get data")

        trips = pgrest.select(params)

        if not trips:
            logging.info("All records processed.")
            break

        trips = split_trips(trips, field_map)

        trips = create_points(trips)

        trips = point_in_poly(
            points=trips,
            polys=census_tracts,
            row_property_key="census_geoid",
            feature_property_key="GEOID10",
            null_val="OUT_OF_BOUNDS",
        )

        trips = point_in_poly(
            points=trips,
            polys=districts,
            row_property_key="council_district",
            feature_property_key="district_n",
            null_val=0,
        )

        trips = merge_trips(trips)

        trips = reduce_fields(
            trips,
            [field["name"] for field in config.FIELDS if field.get("upload_postgrest")],
        )

        post_trips(pgrest, trips)

        total += len(trips)

    return total


if __name__ == "__main__":
    results = main()
