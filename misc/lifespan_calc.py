from datetime import datetime
import dateutil.parser
import json
import pdb
from pprint import pprint as print
import statistics

res = {
    'scooter' : {},
    'bicycle' : {}
} 

with open('grouped_by_unit.json', 'r') as fin:
    data = json.loads(fin.read())

    for unit_id in data.keys():
        start = data[unit_id]['min_start']
        end = data[unit_id]['max_end']

        if not start or not end:
            continue

        if start < 1533081600:
            # exclude before Aug 1, 2018
            continue

        if end > 1551398400:
            # exclude after Mar 1, 2019
            continue
        
        lifespan_days = (end - start) / 86400
        duration = data[unit_id]['trip_duration']
        distance = data[unit_id]['trip_distance']
        trip_count = data[unit_id]['trip_count']
        provider =  data[unit_id]['provider']
        vehicle_type =  data[unit_id]['vehicle_type']
        
        if provider not in res[vehicle_type]:
            res[vehicle_type][provider] = {
                'avg_days' : 0,
                'median_days' : 0,
                'all_days' : [],
                'all_durations' : [],
                'all_trip_distances' : [],
                'all_trip_counts' : [],
                'unit_count' : 0
            }

        res[vehicle_type][provider]['unit_count'] += 1

        # lifespan in days
        res[vehicle_type][provider]['all_days'].append(lifespan_days)
        res[vehicle_type][provider]['avg_days'] = sum(res[vehicle_type][provider]['all_days']) / len(res[vehicle_type][provider]['all_days'])
        res[vehicle_type][provider]['median_days'] = statistics.median(res[vehicle_type][provider]['all_days'])

        # duration
        res[vehicle_type][provider]['all_durations'].append(duration)
        res[vehicle_type][provider]['avg_duration'] = sum(res[vehicle_type][provider]['all_durations']) / len(res[vehicle_type][provider]['all_durations'])
        res[vehicle_type][provider]['median_duration'] = statistics.median(res[vehicle_type][provider]['all_durations'])

        # distance
        res[vehicle_type][provider]['all_trip_distances'].append(trip_count)
        res[vehicle_type][provider]['avg_trip_distance'] = sum(res[vehicle_type][provider]['all_trip_distances']) / len(res[vehicle_type][provider]['all_trip_distances'])
        res[vehicle_type][provider]['median_trip_distance'] = statistics.median(res[vehicle_type][provider]['all_trip_distances'])        

        # trip counts
        res[vehicle_type][provider]['all_trip_counts'].append(trip_count)
        res[vehicle_type][provider]['avg_trip_count'] = sum(res[vehicle_type][provider]['all_trip_counts']) / len(res[vehicle_type][provider]['all_trip_counts'])
        res[vehicle_type][provider]['median_trip_count'] = statistics.median(res[vehicle_type][provider]['all_trip_counts'])        

for vtype in res.keys():
    for provider in res[vtype].keys():
        res[vtype][provider]['all_trip_distances'] = []
        res[vtype][provider]['all_durations'] = []
        res[vtype][provider]['all_days'] = []
        res[vtype][provider]['all_trip_counts'] = []

with open('results.json', 'w') as fout:
    fout.write(json.dumps(res))