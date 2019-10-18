#!/bin/python
# -*- coding: utf8 -*-
# Author: J. Vetter, 2019
# Script calculating the difference between the
# NDVI of two different points in time using
# concurrent and tiled image processing.
###########################################

from Main_Parallized_resampled import *
import json
import argparse
import os
from time import time
import multiprocessing as mp


# Set up argument parser
#parser = argparse.ArgumentParser()
#parser.add_argument("-c", "--config", dest="config_file",
#                    help="configuration file", metavar="CONFIGFILE")
#args = parser.parse_args()

# Parse configuration parameters to dict
#config_file = args.config_file
#if config_file is None or not os.path.exists(config_file):
#    print("Config file does not exist.")
#    exit()

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
    num = 4

except:
    print('Usage of this Script: Boundingbox as int or float, '
          '2 Dates or Range of Dates as string, Property as string,'
          'tilex and tiley as float or integer, name of outfile as string')
    sys.exit(1)
#"2015-09-01/2015-12-04", "2016-06-01/2016-08-04"
"2017-09-01/2017-12-04", "2018-06-01/2018-08-04"
time_start = time()

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
if tile_size_x and tile_size_y > 0:
    print('Start with customized image-processing')
    customized_tiled_calc(image_timestep1,
                          image_timestep2,
                          outfile,
                          tile_size_x,
                          tile_size_y,
                          max_workers=num)

# otherwise use custom-tiled-calculation
else:
    print('Start with optimal image-processing')
    optimal_tiled_calc(image_timestep1,
                       image_timestep2,
                       outfile,
                       max_workers=num)

time_end = time()

print('This took %i seconds' % (time_end-time_start))
