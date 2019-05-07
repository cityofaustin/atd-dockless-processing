'''
For py'ing n polys a whole bune of data. Backlog processing only.
'''
from multiprocessing.dummy import Pool as ThreadPool

import json
import pdb
import time

import requests
from rtree import index
from shapely.geometry import shape, point
from pypgrest import Postgrest

from config import secrets
from config import config


def main():

    def split_trips(data, fields):
        """
        Split trip into separate rows (one for trip start and one fore trip end)
        """
        #todo: use two list comprehensions
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
        #TODO list comprehension
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
            null_val=None
        ):
        """
        Get properties of polygon that contains input point. Assumes polygons
        do not overlap.

        points: list of dicts with shapely geometries
        polys: geojson polygon feature collection

        Returns points with polygon properties specified in key_map.
            Assigns property=None to points that are missing geometries or do not
            intersect with polygon
        """
        start_time = time.time()
        count = 0

        # create grid cell index
        idx = index.Index()
        for pos, feature in enumerate(polys["features"]):
            idx.insert(pos, shape(feature[geom_key]).bounds)

        # find intersecting polyong
        for i, pt in enumerate(points):
            
            if pt[geom_key]:
                
                matched = False

                for intersect_pos in idx.intersection(pt[geom_key].coords[0]):

                    poly = shape(polys["features"][intersect_pos][geom_key])
                    
                    if pt[geom_key].intersects(poly):
                        pt.update( { row_property_key : polys["features"][intersect_pos]["properties"][feature_property_key] } )
                        matched = True
                        break

                if not matched:
                    #  add empty cell ID rows not contained by poly
                    count += 1
                    pt.update({ row_property_key : null_val })
            else:
                count += 1
                pt.update({ row_property_key : null_val })

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
            grid_id = trip.get("cell_id")
            district = trip.get("council_district")

            if grid_id:
                if trip_type == "start":
                    current_data = {
                        "orig_cell_id": grid_id,
                        "council_district_start" : district
                    }

                elif trip_type == "end":
                    current_data = {
                        "dest_cell_id": grid_id,
                        "council_district_end" : district
                    }

                if trip_id in new_trips:
                    new_trips[trip_id].update(current_data)
                
                else:
                    new_trips[trip_id] = current_data
                    new_trips[trip_id].update(**trip)

        return [new_trips[trip] for trip in new_trips.keys()]


    def reduce_fields(data, fieldnames):
        return [{ key: row[key] for key in fieldnames } for row in data]
        

    def post_trips(client, trips):
        print('upsert!')
        print(len(trips))
        return client.upsert(trips)


    def get_data(start):
        pgrest = Postgrest(secrets.PG["url"], auth=secrets.PG["token"])

        params = {
            "select" : "trip_id,provider_id,start_latitude,start_longitude,end_latitude,end_longitude",
            "council_district_start" : "is.null",
            "limit" : interval,
            "offset" : start
        }

        print('get data')
        trips = pgrest.select(params)

        print('split_trips')
        trips = split_trips(trips, field_map)

        print('create_points')
        trips = create_points(trips)

        print('point in grid')
        trips = point_in_poly(points=trips, polys=grid, null_val="OUT_OF_BOUNDS")

        print('point in council district')
        trips = point_in_poly(points=trips, polys=districts, row_property_key="council_district", feature_property_key="district_n", null_val=0)
        
        print('merge trips')
        trips = merge_trips(trips)

        print('reduce fields')
        trips = reduce_fields(trips, [field['name'] for field in config.FIELDS if field.get('upload_postgrest')])

        print('post trips')
        post_trips(pgrest, trips)
        return trips


    field_map = {
        "trip_id_field": "trip_id",
        "trip_start_y_field": "start_latitude",
        "trip_start_x_field": "start_longitude",
        "trip_end_y_field": "end_latitude",
        "trip_end_x_field": "end_longitude",
    }

    grid = read_json(config.GRID_GEOJSON)
    districts = read_json(config.DISTRICTS_GEOJSON)

    workers = 4
    pool = ThreadPool(workers)

    interval = 10000 # num of records that will be downloaded per worker
    total = 10000000 # set to more than total records expected to process

    results = pool.map(get_data, range(0, total, interval))

    total = 0

    for result in results:
        if result:
            total += len(result)

    return total

if __name__ == "__main__":
    results = main()
    print(results)
