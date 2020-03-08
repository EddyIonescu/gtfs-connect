import csv
import json
import numpy as np
import partridge as ptg
import pandas as pd
import numpy as np
import os



def get_feed_df(inpath):
    print(inpath)
    _date, service_ids = ptg.read_busiest_date(inpath)
    # assume it'll be a typical weekday; GO rail is the same every weekday
    view = {
        'trips.txt': {'service_id': service_ids},
    }
    feed = ptg.load_geo_feed(inpath, view)
    return feed

inpath = 'YRT'
yrt_df = get_feed_df('gtfs_winter_2019/'+inpath+'.zip')
print(yrt_df.shapes.head())
print(yrt_df.routes.head())
print(yrt_df.trips.head())

yrt_df.shapes.to_file("../catviz/src/res/yrt.geo.json", driver='GeoJSON')
