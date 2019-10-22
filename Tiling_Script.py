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
    dates = config['dates']
    property = config['property']
    tile_size_x = config['tilex']
    tile_size_y = config['tiley']
    outfile = config['outfile']
    num = config['processors']

except:
    print('Usage of this Script: Boundingbox as int or float, '
          '2 Dates or Range of Dates as string, Property as string,'
          'tilex and tiley as float or integer, name of outfile as string')
    sys.exit(1)


# Check if User-Input is correct
try:
    date_1 = str(dates[0])
    date_2 = str(dates[1])
except ValueError:
    print('The Dates need to be formatted as string. "YYYY-MM-DD"')

try:
    coord_1 = float(bbox[0])
    coord_2 = float(bbox[1])
    coord_3 = float(bbox[2])
    coord_4 = float(bbox[3])
except ValueError:
    print('The coordinates for the Bounding Box need to be float')
    sys.exit(1)

try:
    prop = str(property)
except ValueError:
    print('Property needs to be formatted as string. ""eo:cloud_cover<X"')
    sys.exit(1)

try:
    tile_x = int(tile_size_x)
    tile_y = int(tile_size_y)
except ValueError:
    print('The tilesize needs to be an integer')
    sys.exit(1)

try:
    out = str(outfile)
except ValueError:
    print('Outfile needs to be a string with the appendix .tif')

try:
    processors = int(num)
except ValueError:
    print('Processors needs to be an integer')
    sys.exit(1)


time_start = time()

# Search for Satellite-Images
image_timestep1 = search_image(dates[0],
                               bbox,
                               property)
image_timestep2 = search_image(dates[1],
                               bbox,
                               property)
print("Images found")

# Get the URLs of the red and nir Band for both Time steps
# This is just to inform the user
urls_timestep1 = get_urls(image_timestep1)
urls_timestep2 = get_urls(image_timestep2)
print(("Got urls"))

print(urls_timestep1[0], urls_timestep2[0])
print(urls_timestep2[0], urls_timestep2[1])

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
