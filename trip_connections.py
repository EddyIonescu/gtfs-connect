import csv
import json
import numpy as np
import partridge as ptg
import pandas as pd
import numpy as np
import os
import xlsxwriter


# This script gets all MSP arrivals and departures that occur near a GO station, organized by Stop ID.

inpaths = [
    'GO',
    'YRT',
    'TTC',
    'Barrie Transit',
    'Brampton Transit',
    'Burlington Transit',
    'DRT',
    'GRT',
    'Guelph Transit',
    'HSR',
    #'Milton Transit',
    'MiWay',
    'Niagara Falls Transit',
    'Oakville Transit',
]

def get_feed_df(inpath):
    print(inpath)
    _date, service_ids = ptg.read_busiest_date(inpath)
    print("Selected date for", inpath, ":", _date)
    # assume it'll be a typical weekday; GO rail is the same every weekday
    view = {
        'trips.txt': {'service_id': service_ids},
    }
    feed = ptg.load_feed(inpath, view)
    return feed

def add_agency_col(df, agency, id_fields):
    df['agency'] = agency
    return df

def fill_in_stop_times(df):
    # Barrie Transit doesn't put in arrival/departure times for untimed points, so
    # simply copy the timing point one forward
    return df.fillna(method='ffill') # forward-fill

def get_agency_short_name(feed_df):
    agency_short_name = ''
    if 'agency_id' in feed_df.agency:
        agency_short_name = feed_df.agency.agency_id.head(1).item()
    if not hasattr(agency_short_name, '__len__') or len(agency_short_name) < 2:
        agency_short_name = feed_df.agency.agency_name.head(1).item()
    return agency_short_name

def initialize_feeds():
    feed_dfs = [get_feed_df('gtfs/'+inpath+'.zip') for inpath in inpaths]
    global stops_df
    stops_df = pd.concat([
        add_agency_col(
            feed_df.stops,
            get_agency_short_name(feed_df),
            ['stop_id'],
        ) for feed_df in feed_dfs
    ], ignore_index=True, join='inner')
    global trips_df
    trips_df = pd.concat(
        [add_agency_col(
            feed_df.trips,
            get_agency_short_name(feed_df),
            ['trip_id', 'route_id'],
        ) for feed_df in feed_dfs],
        ignore_index=True,
        join='inner',
    )
    trips_df.set_index(['agency', 'trip_id'], inplace=True)
    global stop_times_df
    stop_times_df = pd.concat(
        [fill_in_stop_times(add_agency_col(
            feed_df.stop_times,
            get_agency_short_name(feed_df),
            ['stop_id', 'trip_id'],
        )) for feed_df in feed_dfs],
        ignore_index=True,
        join='inner',
    )
    stop_times_df.set_index(['agency', 'stop_id'], inplace=True)

    # convert times to readable format
    def seconds_to_clocktime(time):
        return format(int(time // 3600), '02') + ':' + format(int((time % 3600) // 60), '02')
    stop_times_df['arrival_time_hhmm'] = stop_times_df['arrival_time'].apply(seconds_to_clocktime)
    stop_times_df['departure_time_hhmm'] = stop_times_df['departure_time'].apply(seconds_to_clocktime)

    # add stops list ({stop_code};dep_time,...), needed by catviz
    trips_df = stop_times_df.reset_index().groupby(['agency', 'trip_id']).agg({
        'stop_id': lambda stop_id : tuple(stop_id),
        'departure_time_hhmm': lambda stop_dep_time : tuple(stop_dep_time), 
    }).merge(trips_df, left_index=True, right_index=True, validate='one_to_one').rename(columns={
        'stop_id': 'trip_stops',
        'departure_time_hhmm': 'trip_stop_departure_times',
    })

    global routes_df
    routes_df = pd.concat([
        add_agency_col(
            feed_df.routes,
            get_agency_short_name(feed_df),
            ['route_id'],
        ) for feed_df in feed_dfs],
        ignore_index=True,
        join='inner',
    )
    routes_df.set_index(['agency', 'route_id'], inplace=True)
    print(stops_df.head())
    print(trips_df.head())
    print(stop_times_df.head())
    print(routes_df.head())


# returns map of station -> list of values
def read_stations_config_csv(path):
    with open(path, encoding='utf-8-sig') as stations_csv:
        stations_reader = csv.reader(stations_csv, delimiter=',')
        return {
            station_row[0]: station_row[1:]
            for station_row in stations_reader
        }


def read_stations(input_path):
   return read_stations_config_csv(input_path+'/Stations.csv')


def read_location_overrides(input_path):
    return read_stations_config_csv(input_path+'/Locations.csv')


def read_config():
    with open('./config.json') as config_file:
        return json.load(config_file)

def haversine(lat1, lon1, lat2, lon2):
    MILES = 3959
    lat1, lon1, lat2, lon2 = map(np.deg2rad, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1 
    dlon = lon2 - lon1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    total_miles = MILES * c
    return total_miles * 1.6 * 1000 # return metres

# now turn it into the readable format with inbound/outbound/both fields
# result also used as a unique ID for arrivals returned
def get_stop_time_route_stop(row):
    trip_name = str(row['trip_headsign']) or str(row['trip_short_name'])
    # always needed for new rows format
    route_id = str(row['route_short_name']) # + ' ' if str(row['route_short_name']) not in trip_name else ''
    # token inserted as to properly split when outputting
    return str(row['agency']) + ' &gtfstoken& ' + str(route_id) + ' &gtfstoken& '  + trip_name + ' at ' + str(row['stop_name'])

# returns hour (hh:00) of time string (hh:mm)
# used in-place of corridor to more easily view and compare trips on an hour-by-hour basis
def get_hour_of_time(time_hhmm):
    return time_hhmm[:2] + ':00'

# whether a stop arrival belongs to a Corridor route
def is_corridor_stop_time(stop_time, station_stops, corridor_route_ids):
    contains_stop = False
    for station_stop in station_stops.itertuples(index=False):
        if station_stop.stop_id == stop_time['stop_id']:
            contains_stop = True
            break
    route_id = str(stop_time['route_short_name'])
    if contains_stop and route_id in corridor_route_ids:
        return True
    return False


def get_stop_time_meeting_types(
    nearby_stop_times_df,
    station_stops,
    corridor_route_ids,
    min_inbound_minutes,
    max_inbound_minutes,
    min_outbound_minutes,
    max_outbound_minutes,
    hourly_summary,
    union_station_is_inbound=False,
):
    """Using the supplied inbound/outbound transfer limits, determines whether each transit trip
    corresponds to a Corridor trip, an Inbound connection (to a corridor), an Outbound connection
    (from a corridor), Both, or None.

    If union_station_is_inbound is set to true, then the connection type has a second section,
    the addition of -{connection_type} for inbound corridor trips only.

    Returns connection_type, with -peak_connection_type appended if it is not None
    """
    corridor_arrival_by_direction = {}
    def set_corridors(row):
        """Populates corridor_arrival_by_direction and sets the connection type to
        corridor if applicable"""
        if is_corridor_stop_time(row, station_stops, corridor_route_ids): 
            corridor_arrival_time = row['arrival_time']
            corridor_direction = row['trip_headsign']
            if corridor_direction not in corridor_arrival_by_direction:
                corridor_arrival_by_direction[corridor_direction] = []
            corridor_arrival_by_direction[corridor_direction].append(corridor_arrival_time)
            if hourly_summary:
                row[get_stop_time_route_stop(row)] = get_hour_of_time(row['arrival_time_hhmm'])
                return row
            row[get_stop_time_route_stop(row)] = 'Corridor'
        return row

    def set_meeting_type(row):
        if row.get(get_stop_time_route_stop(row)) == 'Corridor':
            return row

        # Skip Inbound if stop is the trip's first (ie. departing at the bus loop)
        skip_inbound = False
        if row['stop_sequence'] == 1:
            skip_inbound = True
        # Skip Outbound if the stop is the trip's last (ie. arriving at the bus loop)
        skip_outbound = False
        if row['stop_sequence'] == len(row['trip_stops']):
            skip_outbound = True

        nonlocal corridor_arrival_by_direction

        def has_connection(arrival_time, is_outbound, union_station_is_inbound=False):
            """Given the arrival time of a local route, verifies whether it has an inbound
            or outbound (based on argument) connection to the corridor, comparing only to inbound
            corridor arrivals if union_station_is_inbound is set to true."""
            for corridor_direction in corridor_arrival_by_direction:
                if union_station_is_inbound and 'Union Station' not in corridor_direction:
                    continue
                for corridor_arrival in corridor_arrival_by_direction[corridor_direction]:
                    outbound_transfer = (arrival_time - corridor_arrival) // 60
                    inbound_transfer = (corridor_arrival - arrival_time) // 60
                    if is_outbound and outbound_transfer <= max_outbound_minutes and outbound_transfer >= min_outbound_minutes:
                        return True
                    if not is_outbound and inbound_transfer <= max_inbound_minutes and inbound_transfer >= min_inbound_minutes:
                        return True
            return False

        connection_type = 'None'
        departure_time = row['departure_time']
        if not skip_inbound and has_connection(departure_time, False):
            connection_type = 'Inbound'
        if not skip_outbound and has_connection(departure_time, True):
            connection_type = 'Both' if connection_type == 'Inbound' else 'Outbound'

        peak_connection_type = 'None'
        if not skip_inbound and has_connection(departure_time, False, True):
            peak_connection_type = 'Inbound'
        if not skip_outbound and has_connection(departure_time, True, True):
            peak_connection_type = 'Both' if peak_connection_type == 'Inbound' else 'Outbound'

        peak_connection_type = f'-{peak_connection_type}' if peak_connection_type != 'None' else ''
        row[get_stop_time_route_stop(row)] = connection_type + peak_connection_type
        return row

    nearby_stop_times_df = nearby_stop_times_df.apply(
        set_corridors,
        axis=1,
    )
    return nearby_stop_times_df.apply(
        set_meeting_type,
        axis=1,
    )

"""Returns dataframe of all MSP connections at the given station.
Also writes CSV containing all MSP departures at the given station in the dev directory"""
def get_local_msp_connections(
    station_name,
    corridor_route_ids,
    connection_max_distance,
    min_inbound_minutes,
    max_inbound_minutes,
    min_outbound_minutes,
    max_outbound_minutes,
    only_show_corridors,
    hourly_summary,
    location_overrides,
    union_station_is_inbound,
):
    station_stops = stops_df.loc[stops_df['stop_name'] == station_name]
    if len(station_stops) == 0:
        # station doesn't exist, return empty
        return ([], [], station_name)

    if len(location_overrides) > 0:
        new_station_stops = []
        station_stops = station_stops.to_dict(orient='records')
        for station_stop in station_stops:
            for location in location_overrides:
                new_station_stop = dict(station_stop)
                new_station_stop['stop_lat'] = float(location.split(',')[0])
                new_station_stop['stop_lon'] = float(location.split(',')[1])
                new_station_stops.append(new_station_stop)
        station_stops = pd.DataFrame(new_station_stops)

    # gets connection distance between each station and all stops
    connections = []
    for station_stop in station_stops.itertuples():
       connections.append(haversine(
           station_stop.stop_lat,
           station_stop.stop_lon,
           stops_df['stop_lat'],
           stops_df['stop_lon'],
        ))
    stops_df['connection_distance'] = pd.concat(connections).groupby(level=0).min()
    nearby_stops_df = stops_df[(stops_df['connection_distance'] <= connection_max_distance) | (stops_df['stop_name'] == station_name)]
    print('Nearby Stops: ', nearby_stops_df.head())

    nearby_stops_df.set_index(['agency', 'stop_id'], inplace=True)
    nearby_stop_times_df = stop_times_df.merge(
        nearby_stops_df,
        left_index=True,
        right_index=True,
        validate='many_to_one',
    )
    nearby_stop_times_df.reset_index(inplace=True)

    nearby_stop_times_df.set_index(['agency', 'trip_id'], inplace=True)
    # only keep trip arrival closest to station, as one route could have
    # multiple stops close to a GO station
    nearby_stop_times_df = nearby_stop_times_df.sort_values(
        by=['connection_distance'],
    ).groupby(
        by=['agency', 'trip_id'],
    ).first()
    nearby_stop_times_df = nearby_stop_times_df.merge(
        trips_df,
        left_index=True,
        right_index=True,
        validate='many_to_one',
    )
    nearby_stop_times_df.reset_index(inplace=True)

    nearby_stop_times_df.set_index(['agency', 'route_id'], inplace=True)
    nearby_stop_times_df = nearby_stop_times_df.merge(
        routes_df,
        left_index=True,
        right_index=True,
        validate='many_to_one',
    )
    nearby_stop_times_df.reset_index(inplace=True)
    print(nearby_stop_times_df.head())

    nearby_stop_times_df = nearby_stop_times_df.sort_values(['arrival_time_hhmm', 'departure_time_hhmm'])
    print('Transit at Station:', nearby_stop_times_df)
    # output dev file
    nearby_stop_times_df.to_csv(
        './output/dev/{station_name}-raw.csv'.format(
            station_name=station_name,
        ),
        index=False,
    )
    # identify whether arrivals/departures are inbound/outbound/both/none
    nearby_stop_times_df = get_stop_time_meeting_types(
        nearby_stop_times_df,
        station_stops,
        corridor_route_ids,
        min_inbound_minutes,
        max_inbound_minutes,
        min_outbound_minutes,
        max_outbound_minutes,
        hourly_summary,
        union_station_is_inbound,
    )
    if only_show_corridors:
        nearby_stop_times_df = nearby_stop_times_df[nearby_stop_times_df.apply(
            lambda row : is_corridor_stop_time(row, station_stops, corridor_route_ids),
            axis=1,
        )]
    print('Matched to meeting type', nearby_stop_times_df.head())
    try:
        route_stops = sorted(nearby_stop_times_df[nearby_stop_times_df.apply(
            lambda row : is_corridor_stop_time(row, station_stops, corridor_route_ids),
            axis=1,
        )].apply(
            get_stop_time_route_stop,
            axis=1,
        ).unique())
    except:
        # station has no trips
        return ([], [], station_name)

    route_non_corridor_stops_df = nearby_stop_times_df[nearby_stop_times_df.apply(
        lambda row : is_corridor_stop_time(row, station_stops, corridor_route_ids) == False,
        axis=1,
    )].apply(
        get_stop_time_route_stop,
        axis=1,
    )
    route_non_corridor_stops = []
    if not route_non_corridor_stops_df.empty:
        # unique errors on empty dataframes, which is possible for non-corridors
        route_non_corridor_stops = sorted(route_non_corridor_stops_df.unique())
    route_stops += route_non_corridor_stops
    # return final file here
    header = ['Arrival Time', 'Departure Time', *route_stops]
    connection_dicts = nearby_stop_times_df.apply(
        lambda row : {
            'Arrival Time': row['arrival_time_hhmm'],
            'Departure Time': row['departure_time_hhmm'],
            get_stop_time_route_stop(row): row[get_stop_time_route_stop(row)] or '',
        },
        axis=1,
    )
    return (header, connection_dicts, station_name)


# each column is agency, route, pattern, stop; eachrow is an arrival/departure
# advantage is that it's much easier to filter in excel for stops with
# many different routes
def output_workbook(connections, union_station_is_inbound):
    workbook = xlsxwriter.Workbook('./output/transit_connections.xlsx')
    cell_format = workbook.add_format()
    cell_format.set_text_wrap()
    for (headers, connection_dicts, station_name) in connections:
        headers = ['Arrival Time', 'Departure Time', 'Connection', 'Agency', 'Route', 'Direction', 'Stop', 'Peak Connection']
        worksheet = workbook.add_worksheet(name=station_name)
        worksheet.autofilter(0, 0, 0, len(headers)-1)
        worksheet.set_column(0, 4, 15)
        worksheet.set_column(5, 6, 60)
        for i in range(len(headers)):
            worksheet.write(0, i, headers[i], cell_format)
        row = 1
        for connection_dict in connection_dicts:
            cell_format = workbook.add_format()
            cell_format.set_text_wrap()
            inbound = False
            outbound = False
            both = False
            corridor = False
            peak_inbound = False # bus to station, with train to union
            peak_outbound = False # bus from station, with train from union
            for connection in list(connection_dict.values()):
                _connection = connection.split('-')[0]
                _corridor_direction = None
                if len(connection.split('-')) > 1:
                    _corridor_direction = connection.split('-')[1]
                if _connection == 'Inbound':
                    inbound = True
                    if _corridor_direction == 'Inbound' or _corridor_direction == 'Both':
                        peak_inbound = True
                if _connection == 'Outbound':
                    outbound = True
                    if _corridor_direction == 'Outbound' or _corridor_direction == 'Both':
                        peak_outbound = True
                if _connection == 'Both':
                    both = True
                    if _corridor_direction == 'Inbound' or _corridor_direction == 'Both':
                        peak_inbound = True
                    if _corridor_direction == 'Outbound' or _corridor_direction == 'Both':
                        peak_outbound = True
                if _connection == 'Corridor':
                    corridor = True
            highlight_blue = False
            highlight_green = False
            # highlight_green means it's a peak connection; blue means corridor connection
            if (inbound or both) and connection_dict['Arrival Time'] < '12:00':
                if union_station_is_inbound:
                    if peak_inbound:
                        highlight_green = True
                else:
                    highlight_green = True
            if (outbound or both) and connection_dict['Departure Time'] >= '12:00':
                if union_station_is_inbound:
                    if peak_outbound:
                        highlight_green = True
                else:
                    highlight_green = True
            if corridor:
                highlight_blue = True
            if highlight_green:
                cell_format.set_bg_color('#6afc9f')
            if highlight_blue:
                cell_format.set_bg_color('#6bd7ff')
            for k, v in connection_dict.items():
                if k == 'Arrival Time':
                    worksheet.write(row, 0, v, cell_format)
                    continue
                if k == 'Departure Time':
                    worksheet.write(row, 1, v, cell_format)
                    continue
                sections = k.split('&gtfstoken&')
                agency = sections[0].strip()
                route = sections[1].strip()
                direction = sections[2].split(' at ')[0].strip()
                stop = sections[2].split(' at ')[1].strip()
                if corridor:
                    worksheet.write(row, 2, 'Corridor', cell_format)
                elif both:
                    worksheet.write(row, 2, 'Both', cell_format) 
                elif inbound:
                    worksheet.write(row, 2, 'Inbound', cell_format) 
                elif outbound:
                    worksheet.write(row, 2, 'Outbound', cell_format)
                else:
                    worksheet.write(row, 2, 'None', cell_format)
                # cannot change this ordering since CAT dashboard hardcodes column letter
                worksheet.write(row, 3, agency, cell_format)
                worksheet.write(row, 4, route, cell_format)
                worksheet.write(row, 5, direction, cell_format)
                worksheet.write(row, 6, stop, cell_format)
                worksheet.write(row, 7, 'TRUE' if highlight_green else 'FALSE')
            row += 1
    workbook.close()


input_dict = read_config()
stations = read_stations(input_dict['input_path'])
location_overrides = read_location_overrides(input_dict['input_path'])
initialize_feeds()
station_connections = []
for (station_name, corridors) in stations.items():
    if station_name != '':
        connections = get_local_msp_connections(
            station_name=station_name,
            corridor_route_ids=corridors,
            connection_max_distance=input_dict['connection_max_distance'],
            min_inbound_minutes=input_dict['min_inbound_minutes'],
            max_inbound_minutes=input_dict['max_inbound_minutes'],
            min_outbound_minutes=input_dict['min_outbound_minutes'],
            max_outbound_minutes=input_dict['max_outbound_minutes'],
            only_show_corridors=input_dict['only_show_corridors'],
            hourly_summary=input_dict['hourly_summary'],
            location_overrides=location_overrides.get(station_name, []),
            union_station_is_inbound=input_dict.get('union_station_is_inbound', False),
        )
        station_connections.append(connections)
# write each connection_df as an excel sheet in a workbook having all stations
output_workbook(
    station_connections,
    union_station_is_inbound=input_dict.get('union_station_is_inbound', False),
)
