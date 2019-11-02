"""
#!/bin/python
# -*- coding: utf8 -*-
# Author: J. Vetter, 2019
# Script calculating the difference between the
# NDVI of two different points in time using
# concurrent and tiled image processing.
###########################################
"""

import os
import sys
import json
import argparse
from time import time
from parallized_resampled_intersection import search_image
from parallized_resampled_intersection import get_urls
from parallized_resampled_intersection import optimal_tiled_calc
from parallized_resampled_intersection import customized_tiled_calc


# Set up argument parser
PARSER = argparse.ArgumentParser()
PARSER.add_argument("-c", "--config",
                    dest="config_file",
                    help="configuration file",
                    metavar="CONFIGFILE")
ARGS = PARSER.parse_args()

# Parse configuration parameters to dict
CONFIG_FILE = ARGS.config_file
if CONFIG_FILE is None or not os.path.exists(CONFIG_FILE):
    print("Config file does not exist.")
    exit()


with open('CONFIG.json', 'r') as src:
    CONFIG = json.load(src)

# Get Parameter from config-file
try:
    BBOX = CONFIG['bounding_box']
    DATES = CONFIG['dates']
    PROP = CONFIG['property']
    TILE_SIZE_X = CONFIG['tilex']
    TILE_SIZE_Y = CONFIG['tiley']
    OUTFILE = CONFIG['outfile']
    NUM = CONFIG['processors']

except:
    print('Usage of this Script: Boundingbox as int or float, '
          '2 Dates or Range of Dates as string, Property as string,'
          'tilex and tiley as float or integer, name of outfile as string')
    sys.exit(1)


# Check if User-Input is correct
try:
    DATE_1 = str(DATES[0])
    DATE_2 = str(DATES[1])
except ValueError:
    print('The Dates need to be formatted as string. "YYYY-MM-DD"')

try:
    COORD_1 = float(BBOX[0])
    COORD_2 = float(BBOX[1])
    COORD_3 = float(BBOX[2])
    COORD_4 = float(BBOX[3])
except ValueError:
    print('The coordinates for the Bounding Box need to be float')
    sys.exit(1)

try:
    PROPE = str(PROP)
except ValueError:
    print('Property needs to be formatted as string. ""eo:cloud_cover<X"')
    sys.exit(1)

try:
    TILE_X = int(TILE_SIZE_X)
    TILE_Y = int(TILE_SIZE_Y)
except ValueError:
    print('The tilesize needs to be an integer')
    sys.exit(1)

try:
    OUT = str(OUTFILE)
except ValueError:
    print('Outfile needs to be a string with the appendix .tif')

try:
    PROCESSORS = int(NUM)
except ValueError:
    print('Processors needs to be an integer')
    sys.exit(1)


# Search for Satellite-Images
IMAGE_TIMESTEP_1 = search_image(DATES[0],
                                BBOX,
                                PROP)
IMAGE_TIMESTEP_2 = search_image(DATES[1],
                                BBOX,
                                PROP)
print("Images found")


# Get the URLs of the red and nir Band for both Time steps
# This is just to inform the user
URLS_TIMESTEP_1 = get_urls(IMAGE_TIMESTEP_1)
URLS_TIMESTEP_2 = get_urls(IMAGE_TIMESTEP_2)
print(URLS_TIMESTEP_1[0], URLS_TIMESTEP_1[1])
print(URLS_TIMESTEP_2[0], URLS_TIMESTEP_2[1])
print(("Got urls"))


# if optimal-tiled-calculation was choosen
if TILE_SIZE_X and TILE_SIZE_Y > 0:
    TIME_1 = time()
    print('Start with customized image-processing')

    customized_tiled_calc(IMAGE_TIMESTEP_1,
                          IMAGE_TIMESTEP_2,
                          OUTFILE,
                          TILE_SIZE_X,
                          TILE_SIZE_Y,
                          max_workers=NUM)
    TIME_2 = time()
    print('This took %s' % (TIME_2-TIME_1))


# otherwise use custom-tiled-calculation
else:
    print('Start with optimal image-processing')
    TIME_3 = time()

    optimal_tiled_calc(
                       IMAGE_TIMESTEP_1,
                       IMAGE_TIMESTEP_2,
                       OUTFILE,
                       max_workers=NUM)

    TIME_4 = time()
    print('This took %s' % (TIME_4-TIME_3))


CWD = os.getcwd()
print('The file has been saved in %s' % CWD)
