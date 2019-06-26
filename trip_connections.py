import csv
import numpy as np
import partridge as ptg
import pandas as pd
import os
import xlsxwriter


from haversine import haversine

# This script gets all MSP arrivals and departures that occur near a GO station, organized by Stop ID.



inpaths = [
    'GO',
    'YRT',
    'TTC',
    'Barrie',
    'Brampton',
    'Burlington',
    'DRT',
    'GRT',
    'Guelph',
    'HSR',
    'Milton',
    'MiWay',
    'Niagara',
    'Oakville',
]

def get_feed_df(inpath):
    print(inpath)
    _date, service_ids = ptg.read_busiest_date(inpath)
    # assume it'll be a typical weekday; GO rail is the same every weekday
    view = {
        'trips.txt': {'service_id': service_ids},
    }
    feed = ptg.load_feed(inpath, view)
    return feed

def add_agency_col(df, agency, id_fields):
    df['agency'] = agency
    #for id_field in id_fields:
    #    df[id_field] = df[id_field].apply(lambda id : agency + ' ' + id)
    return df

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
        [add_agency_col(
            feed_df.stop_times,
            get_agency_short_name(feed_df),
            ['stop_id', 'trip_id'],
        ) for feed_df in feed_dfs],
        ignore_index=True,
        join='inner',
    )
    stop_times_df.set_index(['agency', 'stop_id'], inplace=True)
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

def read_input_file(inputreader):
    # station names and route IDs agnostic among agencies
    station_names = next(inputreader)[1:]
    corridor_route_ids = next(inputreader)[1:]
    connection_max_distance = int(next(inputreader)[1])
    min_inbound_minutes = int(next(inputreader)[1])
    max_inbound_minutes = int(next(inputreader)[1])
    min_outbound_minutes = int(next(inputreader)[1])
    max_outbound_minutes = int(next(inputreader)[1])
    return {
        'station_names': station_names,
        'corridor_route_ids': corridor_route_ids,
        'connection_max_distance': connection_max_distance,
        'min_inbound_minutes': min_inbound_minutes,
        'max_inbound_minutes': max_inbound_minutes,
        'min_outbound_minutes': min_outbound_minutes,
        'max_outbound_minutes': max_outbound_minutes,
    }


# now turn it into the readable format with inbound/outbound/both fields
# result also used as a unique ID for arrivals returned
def get_stop_time_route_stop(row):
    trip_name = row['trip_headsign'] or row['trip_short_name']
    # always needed for new rows format
    route_id = str(row['route_short_name']) # + ' ' if str(row['route_short_name']) not in trip_name else ''
    return row['agency'] + '_' + str(route_id) + '_' + trip_name + ' at ' + row['stop_name']


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

# returns hour (hh:00) of time string (hh:mm)
# used in-place of corridor to more easily view and compare trips on an hour-by-hour basis
def get_hour_of_time(time_hhmm):
    return time_hhmm[:2] + ':00'


def get_stop_time_meeting_types(
    nearby_stop_times_df,
    direction,
    station_stops,
    corridor_route_ids,
    min_inbound_minutes,
    max_inbound_minutes,
    min_outbound_minutes,
    max_outbound_minutes,
):
    """Using the supplied inbound/outbound transfer limits, determines whether each transit trip
    corresponds to a Corridor trip, an Inbound connection (to a corridor), an Outbound connection
    (from a corridor), Both, or None.
    Note that the list must be reversed when the direction is Inbound

    :param direction: one of Inbound or Outbound
    """
    recentmost_corridor_arrival_time = 0
    def set_meeting_type(row):
        nonlocal recentmost_corridor_arrival_time
        if is_corridor_stop_time(row, station_stops, corridor_route_ids): 
            recentmost_corridor_arrival_time = row['arrival_time']
            row[get_stop_time_route_stop(row)] = 'Corridor'
            # get_hour_of_time(row['arrival_time_hhmm']) - used in rail corridor task
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
    only_show_corridors=False,
):
    # corridor_stop_ids = ['GO GO', 'GO 02629'] # all stops matching station name
    station_stops = stops_df.loc[stops_df['stop_name'] == station_name]
    print(station_stops)
    if len(station_stops) == 0:
        # station doesn't exist, return empty
        return ([], [], station_name)

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

    # convert times to readable format
    def seconds_to_clocktime(time):
        return format(int(time // 3600), '02') + ':' + format(int((time % 3600) // 60), '02')

    nearby_stop_times_df['arrival_time_hhmm'] = nearby_stop_times_df['arrival_time'].apply(seconds_to_clocktime)
    nearby_stop_times_df['departure_time_hhmm'] = nearby_stop_times_df['departure_time'].apply(seconds_to_clocktime)
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
        'outbound',
        station_stops,
        corridor_route_ids,
        min_inbound_minutes,
        max_inbound_minutes,
        min_outbound_minutes,
        max_outbound_minutes,
    )
    nearby_stop_times_df = get_stop_time_meeting_types(
        nearby_stop_times_df[::-1],
        'inbound',
        station_stops,
        corridor_route_ids,
        min_inbound_minutes,
        max_inbound_minutes,
        min_outbound_minutes,
        max_outbound_minutes,
    )
    nearby_stop_times_df = nearby_stop_times_df[::-1]
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

# each column is a route/pattern/stop, each row is an arrival/departure
def output_workbook(workbook_name, connections):
    workbook = xlsxwriter.Workbook('./output/'+workbook_name+'.xlsx')
    cell_format = workbook.add_format()
    cell_format.set_text_wrap()
    for (headers, connection_dicts, station_name) in connections:
        worksheet = workbook.add_worksheet(name=station_name)
        worksheet.autofilter(0, 0, 0, len(headers)-1)
        for i in range(len(headers)):
            worksheet.write(0, i, headers[i], cell_format)
        row = 1
        for connection_dict in connection_dicts:
            cell_format = workbook.add_format()
            cell_format.set_text_wrap()
            highlight_green = False
            highlight_blue = False
            for connection_type in ['Inbound', 'Both']:
                if connection_type in list(connection_dict.values()) and connection_dict['Arrival Time'] < '12:00':
                    highlight_green = True
            for connection_type in ['Outbound', 'Both']:
                if connection_type in list(connection_dict.values()) and connection_dict['Departure Time'] >= '12:00':
                    highlight_green = True
            if 'Corridor' in list(connection_dict.values()):
                highlight_blue = True
            if highlight_green:
                cell_format.set_bg_color('#6afc9f')
            if highlight_blue:
                cell_format.set_bg_color('#6bd7ff')
            for k, v in connection_dict.items():
                worksheet.write(row, headers.index(k), v, cell_format)
            row += 1
        
    workbook.close()

# each column is agency, route, pattern, stop; eachrow is an arrival/departure
# advantage is that it's much easier to filter in excel for stops with
# many different routes
def output_workbook_rows(workbook_name, connections):
    workbook = xlsxwriter.Workbook('./output/rows/'+workbook_name+'.xlsx')
    cell_format = workbook.add_format()
    cell_format.set_text_wrap()
    for (headers, connection_dicts, station_name) in connections:
        headers = ['Arrival Time', 'Departure Time', 'Connection', 'Agency', 'Route', 'Direction', 'Stop']
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
            for connection in list(connection_dict.values()):
                if connection == 'Inbound':
                    inbound = True
                if connection == 'Outbound':
                    outbound = True
                if connection == 'Both':
                    both = True
                if connection == 'Corridor':
                    corridor = True
            highlight_blue = False
            highlight_green = False
            if (inbound or both) and connection_dict['Arrival Time'] < '12:00':
                highlight_green = True
            if (outbound or both) and connection_dict['Departure Time'] >= '12:00':
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
                sections = k.split('_')
                agency = sections[0]
                route = sections[1]
                direction = sections[2].split(' at ')[0]
                stop = sections[2].split(' at ')[1]
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
                worksheet.write(row, 3, agency, cell_format)
                worksheet.write(row, 4, route, cell_format)
                worksheet.write(row, 5, direction, cell_format)
                worksheet.write(row, 6, stop, cell_format)
            row += 1
    workbook.close()


initialize_feeds()
# read input.csv
with open('input.csv', newline='') as csvfile:
    inputreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    use_inputs_dir = next(inputreader)[1]
    if use_inputs_dir != 'TRUE':
        input_dict = read_input_file(inputreader)
        connections = [get_local_msp_connections(
            station_name=station_name,
            corridor_route_ids=input_dict['corridor_route_ids'],
            connection_max_distance=input_dict['connection_max_distance'],
            min_inbound_minutes=input_dict['min_inbound_minutes'],
            max_inbound_minutes=input_dict['max_inbound_minutes'],
            min_outbound_minutes=input_dict['min_outbound_minutes'],
            max_outbound_minutes=input_dict['max_outbound_minutes'],
        ) for station_name in input_dict['station_names']]
        # write each connection_df as an excel sheet in a workbook
        output_workbook('msp_connections', connections)
        output_workbook_rows('msp_connections', connections)

if use_inputs_dir == 'TRUE':
    input_dir = 'inputs'
    for input_file in os.listdir(input_dir):
        with open(input_dir+'/'+input_file, newline='') as csvfile:
            if '.csv' not in input_file:
                continue
            inputreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            input_dict = read_input_file(inputreader)
            workbook_name = input_file.split('.csv')[0]
            connections = [get_local_msp_connections(
                station_name=station_name,
                corridor_route_ids=input_dict['corridor_route_ids'],
                connection_max_distance=input_dict['connection_max_distance'],
                min_inbound_minutes=input_dict['min_inbound_minutes'],
                max_inbound_minutes=input_dict['max_inbound_minutes'],
                min_outbound_minutes=input_dict['min_outbound_minutes'],
                max_outbound_minutes=input_dict['max_outbound_minutes'],
            ) for station_name in input_dict['station_names']]
            # write each connection_df as an excel sheet in a workbook
            # with the name input_file (with an xslx extension)
            output_workbook(workbook_name, connections)
            output_workbook_rows(workbook_name, connections)
