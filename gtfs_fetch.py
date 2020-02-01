# Gets the current GTFS files and updates the GTFS directory.
# MAKE SURE YOU MOVE THE GTFS FILES ALREADY IN THE GTFS DIRECTORY SOMEWHERE ELSE
# (ie. rename the folder to something like gtfs_fall_2019)


import requests
import os

gtfs_directory = 'gtfs'
gtfs_config = [
  {
    'name': 'Barrie Transit',
    'source': 'http://www.myridebarrie.ca/gtfs/Google_transit.zip',
  },
  {
    'name': 'Brampton Transit',
    'source': 'http://www.brampton.ca/EN/City-Hall/OpenGov/Open-Data-Catalogue/Documents/Google_Transit.zip',
  },
  {
    'name': 'Burlington Transit',
    'source': 'http://opendata.burlington.ca/gtfs-rt/GTFS_Data.zip',
  },
  {
    'name': 'DRT',
    'source': 'https://maps.durham.ca/OpenDataGTFS/GTFS_Durham_TXT.zip',
  },
  {
    'name': 'GO',
    'source': 'https://www.gotransit.com/static_files/gotransit/assets/Files/GO_GTFS.zip',
  },
  {
    'name': 'GRT',
    'source': 'http://www.regionofwaterloo.ca/opendatadownloads/GRT_GTFS.zip',
  },
  {
    'name': 'Guelph Transit',
    'source': 'http://data.open.guelph.ca/datafiles/guelph-transit/guelph_transit_gtfs.zip',
  },
  {
    'name': 'HSR',
    'source': 'http://googlehsrdocs.hamilton.ca/',
  },
  {
    'name': 'Milton Transit',
  },
  {
    'name': 'MiWay',
    'source': 'https://www.miapp.ca/GTFS/google_transit.zip?#',
  },
  {
    'name': 'Niagara Falls Transit',
    'source': 'https://maps.niagararegion.ca/googletransit/NiagaraRegionTransit.zip',
  },
  {
    'name': 'Oakville Transit',
    'source': 'https://www.arcgis.com/sharing/rest/content/items/d78a1c1ad6a940009de8b68839a8f606/data',
  },
  {
    'name': 'TTC',
    'source': 'http://opendata.toronto.ca/TTC/routes/OpenData_TTC_Schedules.zip',
  },
  {
    'name': 'YRT',
    'source': 'https://www.yrt.ca/google/google_transit.zip',
  },
]
if not os.path.exists(gtfs_directory):
    os.mkdir(gtfs_directory)

for gtfs in gtfs_config:
    if 'source' not in gtfs:
        print('Warning:', gtfs['name'], 'does not have a GTFS source - it will be skipped. Open gtfs_fetch.py and add in the GTFS link under the "source" key as is done in the other agencies.')
        continue
    req = requests.get(gtfs['source'])
    with open(gtfs_directory + '/' + gtfs['name'] + '.zip', 'wb') as gtfs_stream:
        gtfs_stream.write(req.content)
    print('Updated', gtfs['name'], 'in', gtfs_directory)
