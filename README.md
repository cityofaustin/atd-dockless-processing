# atd-dockless-processing

This repository contains Python scripts for processing dockless mobility data from [MDS](https://github.com/CityOfLosAngeles/mobility-data-specification)-compliant feeds.

The data processing is addressed in two steps:

- `get_data.py` downloads MDS data and loads it to a [PostgREST](http://postgrest.org) endpoint.

- `pt_in_poly.py` geocodes trip data with council district and census tract attributes
