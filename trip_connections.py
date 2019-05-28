import csv
import numpy as np
import partridge as ptg
import pandas as pd


from haversine import haversine

# This script gets all MSP arrivals and departures that occur near a GO station, organized by Stop ID.

# read input.csv
with open('input.csv', newline='') as csvfile:
    inputreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    # station names and route IDs agnostic among agencies
    station_names = next(inputreader)[1:]
    corridor_route_ids = next(inputreader)[1:]
    connection_max_distance = int(next(inputreader)[1])
    min_inbound_minutes = int(next(inputreader)[1])
    max_inbound_minutes = int(next(inputreader)[1])
    min_outbound_minutes = int(next(inputreader)[1])
    max_outbound_minutes = int(next(inputreader)[1])


def get_feed_df(inpath):
    _date, service_ids = ptg.read_busiest_date(inpath)
    # assume it'll be a typical weekday; GO rail is the same every weekday
    view = {
        'trips.txt': {'service_id': service_ids},
    }
    feed = ptg.load_feed(inpath, view)
    return feed

# corridor_stop_ids = ['GO GO', 'GO 02629'] # all stops matching station name
inpaths = ['gtfs/GO.zip', 'gtfs/YRT.zip']
feed_dfs = [get_feed_df(inpath) for inpath in inpaths]

def add_agency_col(df, agency, id_fields):
    df['agency'] = agency
    for id_field in id_fields:
        df[id_field] = df[id_field].apply(lambda id : agency + ' ' + id)
    return df

stops_df = pd.concat([
    add_agency_col(feed_df.stops, feed_df.routes.agency_id.head(1).item(), ['stop_id']) for feed_df in feed_dfs
], ignore_index=True, join='inner')
station_stops = stops_df.loc[np.isin(stops_df['stop_name'], station_names)]
print(station_stops)

stops_df['connection_distance'] = stops_df.apply(
    # use minimum distance to any stop matching the station name
    lambda stop_df : min([haversine(
        lon1=stop_df['stop_lon'],
        lat1=stop_df['stop_lat'],
        lon2=station_stop.stop_lon,
        lat2=station_stop.stop_lat,
    ) for station_stop in station_stops.itertuples()]),
    axis=1,
)
nearby_stops_df = stops_df.loc[stops_df['connection_distance'] <= connection_max_distance]
print(nearby_stops_df.head())

# get stop times for each stop

trips_df = pd.concat([add_agency_col(feed_df.trips, feed_df.routes.agency_id.head(1).item(), ['trip_id']) for feed_df in feed_dfs], ignore_index=True, join='inner')
stop_times_df = pd.concat([add_agency_col(feed_df.stop_times, feed_df.routes.agency_id.head(1).item(), ['stop_id', 'trip_id']) for feed_df in feed_dfs], ignore_index=True, join='inner')

print(trips_df.head())
print(stop_times_df.head())

nearby_stops_df.set_index('stop_id', inplace=True)
stop_times_df.set_index('stop_id', inplace=True)
nearby_stop_times_df = stop_times_df.merge(
    nearby_stops_df,
    left_index=True,
    right_index=True,
    validate='many_to_one',
)
nearby_stop_times_df.reset_index(inplace=True)
nearby_stop_times_df.set_index('trip_id', inplace=True)
trips_df.set_index('trip_id', inplace=True)
nearby_stop_times_df = nearby_stop_times_df.merge(
    trips_df,
    left_index=True,
    right_index=True,
    validate='many_to_one',
)
nearby_stop_times_df.reset_index(inplace=True)
print(nearby_stop_times_df.head())

# now export trips for station + all nearby stops

def seconds_to_clocktime(time):
    return format(int(time // 3600), '02') + ':' + format(int((time % 3600) // 60), '02')

nearby_stop_times_df['arrival_time_hhmm'] = nearby_stop_times_df['arrival_time'].apply(seconds_to_clocktime)
nearby_stop_times_df['departure_time_hhmm'] = nearby_stop_times_df['departure_time'].apply(seconds_to_clocktime)
nearby_stop_times_df = nearby_stop_times_df.sort_values(['arrival_time_hhmm', 'departure_time_hhmm'])
print('Transit at Gormeley GO:', nearby_stop_times_df)

nearby_stop_times_df.to_csv(
    './output/msp_connections_raw.csv',
    index=False,
)

# now turn it into the readable format with inbound/outbound/both fields
def get_stop_time_route_stop(row):
    trip_name = row['trip_headsign'] or row['trip_short_name']
    route_id = row['route_id'] + ' ' if row['route_id'] not in trip_name else ''
    return row['agency'] + ' ' + route_id + trip_name + ' at ' + row['stop_name']

route_stops = nearby_stop_times_df.apply(
    lambda row : get_stop_time_route_stop(row),
    axis=1,
).unique()

def is_corridor_stop_time(stop_time):
    contains_stop = len(station_stops.loc[station_stops['stop_id'] == stop_time['stop_id']]) > 0
    route_id = str(stop_time['route_id'])
    if '-' in route_id:
        # example of a GO route_id: 122345-40 (for the 40 GO bus)
        route_id = str(stop_time['route_id']).split('-')[1] 
    if contains_stop and route_id in corridor_route_ids:
        return True
    return False

def get_stop_time_meeing_types(nearby_stop_times_df, direction):
    """Using the supplied inbound/outbound transfer limits, determines whether each transit trip
    corresponds to a Corridor trip, an Inbound connection (to a corridor), an Outbound connection
    (from a corridor), Both, or None.
    Note that the list must be reversed when the direction is Inbound

    :param direction: one of Inbound or Outbound
    """
    recentmost_corridor_arrival_time = 0
    def set_meeting_type(row):
        nonlocal recentmost_corridor_arrival_time
        if is_corridor_stop_time(row): 
            recentmost_corridor_arrival_time = row['arrival_time']
            row[get_stop_time_route_stop(row)] = 'Corridor'
        outbound_transfer_min = (row['departure_time'] - recentmost_corridor_arrival_time) // 60
        inbound_transfer_min = (recentmost_corridor_arrival_time - row['departure_time']) // 60
        if direction == 'outbound' and outbound_transfer_min >= min_outbound_minutes and outbound_transfer_min <= max_outbound_minutes:
            row[get_stop_time_route_stop(row)] = 'Outbound' 
        if direction == 'inbound' and inbound_transfer_min >= min_inbound_minutes and inbound_transfer_min <= max_inbound_minutes:
            if row[get_stop_time_route_stop(row)] == 'Outbound':
                row[get_stop_time_route_stop(row)] = 'Both'
            else:
                row[get_stop_time_route_stop(row)] = 'Inbound'
        if get_stop_time_route_stop(row) not in row:
            row[get_stop_time_route_stop(row)] = 'None'
        return row
    return nearby_stop_times_df.apply(
        set_meeting_type,
        axis=1,
    )

nearby_stop_times_df = get_stop_time_meeing_types(nearby_stop_times_df, 'outbound')
nearby_stop_times_df = get_stop_time_meeing_types(nearby_stop_times_df[::-1], 'inbound')
nearby_stop_times_df = nearby_stop_times_df[::-1]

with open('./output/msp_connections.csv', 'w', newline='') as csvfile:
    fieldnames = ['Arrival Time', 'Departure Time', *sorted(route_stops)]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    output_dicts = nearby_stop_times_df.apply(
        lambda row : {
            'Arrival Time': row['arrival_time_hhmm'],
            'Departure Time': row['departure_time_hhmm'],
            get_stop_time_route_stop(row): row[get_stop_time_route_stop(row)] or '',
        },
        axis=1,
    )
    for output_dict in output_dicts:
        writer.writerow(output_dict)

