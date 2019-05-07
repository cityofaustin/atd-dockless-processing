# \copy (SELECT * from api.dockless) TO 'data.csv' DELIMITER ',' CSV HEADER

import csv
from datetime import datetime
import dateutil.parser
import json
import pdb
from pprint import pprint as print

units = {}

with open('data.csv', 'r') as fin:
    reader = csv.DictReader(fin)
    count = 0
    
    for row in reader:

        unit_id = row.get('device_id')
        
        if not unit_id:
            continue

        if unit_id not in units:
            units[unit_id] = {
                'min_start' : None,
                'max_end' : None,
                'trip_distance' : 0,
                'trip_duration' : 0,
                'trip_count' : 0,
                'districts' : []
            }

    
        try:
            duration = int(row['trip_duration'])
            distance = int(row['trip_distance'])

            if duration < 1 or duration > 86400 or distance < 1 or distance > 80467:
                # if duration is not less than a day and distance is no less than 50 miles
                continue

            start = dateutil.parser.parse(row['start_time'])
            start = datetime.timestamp(start)
            
            end = dateutil.parser.parse(row['end_time'])
            end = datetime.timestamp(end)
            
            if not units[unit_id]['min_start']:
                units[unit_id]['min_start'] = start
            
            elif units[unit_id]['min_start'] > start:
                units[unit_id]['min_start'] = start
            
            if not units[unit_id]['max_end']:
                units[unit_id]['max_end'] = end

            elif units[unit_id]['min_start'] > end:
                units[unit_id]['min_start'] = end

            district = row['council_district_start']
            
            if district not in units[unit_id]['districts']:
                units[unit_id]['districts'].append(district)

            units[unit_id]['vehicle_type'] = row['vehicle_type']
            units[unit_id]['provider'] = row['provider_name']
            units[unit_id]['trip_duration'] += duration
            units[unit_id]['trip_distance'] += distance
            units[unit_id]['trip_count'] += 1

        except KeyError as e:
            print(row)
            raise e
    
with open('grouped_by_unit.json', 'w') as fout:
    fout.write(json.dumps(units))