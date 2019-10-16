from Main_Parallized import *
import json
import argparse
import os
from time import time
import multiprocessing as mp


# Set up argument parser
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", dest="config_file",
                    help="configuration file", metavar="CONFIGFILE")
args = parser.parse_args()

# Parse configuration parameters to dict
config_file = args.config_file
if config_file is None or not os.path.exists(config_file):
    print("Config file does not exist.")
    exit()

with open('config.json', 'r') as src:
    config = json.load(src)

# Get Parameter from config-file
try:
    bbox = config['bounding_box']
    date = config['dates']
    property = config['property']
    tile_size_x = config['tilex']
    tile_size_y = config['tiley']
    outfile = config['outfile']

except:
    print('Usage of this Script: Boundingbox as int or float, '
          '2 Dates or Range of Dates as string, Property as string,'
          'tilex and tiley as float or integer, name of outfile as string')
    sys.exit(1)

# Get number of processors
num = mp.Pool(mp.cpu_count())


# Search for Satellite-Images
image_timestep1 = search_image(date[0],
                               bbox,
                               property)
image_timestep2 = search_image(date[1],
                               bbox,
                               property)
print("Images found")

# Get the URLs of the red and nir Band for both Time steps
urls_timestep1 = get_urls(image_timestep1)
urls_timestep2 = get_urls(image_timestep2)
print(("Got urls"))

# if optimal-tiled-calculation was choosen
if tile_size_x and tile_size_y == 0:
    optimal_tiled_calc(image_timestep1,
                       image_timestep2,
                       outfile,
                       max_workers=num)

# otherwise use custom-tiled-calculation
else:
    customized_tiled_calc(image_timestep1,
                          image_timestep2,
                          outfile,
                          tile_size_x,
                          tile_size_y,
                          max_workers=num)