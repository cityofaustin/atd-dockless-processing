# compare pg and socrata data, format time. output is a diff that can be imported to socrata
import csv
import pdb
from datetime import datetime

timefields = [
    "start_time",
    "end_time",
    "modified_date"
]

def to_unix(iso_string):
    # convert time string to isoformat, for socrata_trips
    pdb.set_trace()
    iso_string = iso_string[0:19]
    dt = datetime.strptime(iso_string, "%Y-%m-%d %H:%M:%S")
    return dt.isoformat()


with open('socrata.csv', 'r') as fin:
    reader = csv.DictReader(fin)
    ids = set([row['trip_id'] for row in reader])


print('compare to pg')

# with open('postgrest_trips.csv', 'r') as fin2:
with open('pgrest.csv', 'r') as fin:
    reader = csv.DictReader(fin)
    trips = [row for row in reader if row['trip_id'] not in ids]

# print('handle timestamps')
# for row in trips:
#     for field in timefields:
#         row[field] = to_unix(row[field])

print('write csv')

with open('missing_trips_v2.csv', 'w', newline='\n') as fout:
    fieldnames = trips[0].keys()
    writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=",")
    writer.writeheader()
    for row in trips:
        writer.writerow(row)