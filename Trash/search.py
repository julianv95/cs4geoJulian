import sys
from itertools import product
import concurrent.futures
import numpy as np
import rasterio as rio
from rasterio import windows
from rasterio.enums import Resampling
from satsearch import Search
import json



# search sources
def search_image(date, bounding_box, prop):
    """Searches Satellite-Image for given Boundingbox, Date and Properties
    :parameter:
    single Date,
    Bounding Box as List
    Properties as String
    :return:
    statsac.Item Object with the lowest
    cloud-coverage for the given Date and bounding box"""

    image = []

    # search image for given date or period of time,
    # always takes the first image
    search = Search(bbox=bounding_box,
                    datetime=date,
                    property=[prop],
                    sort=[{'field': 'eo:cloud_cover', 'direction': 'asc'}]
                    )

    items = search.items()

    # filter for Landsatimages since Sentinel doesn't work and the
    # collection option for sat-search seems to be broken
    workaround = [str(item) for item in items]
    counter = 0

    for raster in workaround:
        if 'S2' in raster:
            counter += 1
        else:
            image.append(items[counter])
            break

    # check if image was found
    assert len(image) == 1, 'No Images for given Parameters found. ' \
                            'Please try new ones'

    return items[counter]


def get_urls(statsac_item):
    """Searches for the URLS of satellite-images
    :parameter:
    satsac.Item Object
    :returns:
    List of URLS containing the red and near infrared
    Bands of the satellite-images.
    Order: RED, NIR
    """
    band_urls = []

    # extract the urls for the red and near-infrared band of the images
    # for the given date or period of time
    # Check if Landsat or Sentinel
    # Since the script only works with Landsat-Data, this is not necessary,
    # but if at some point in time SAT-Search API is fixed,
    # this is an easy way to implement Sentinel-Data
    if 'B4' and 'B5' in statsac_item.assets:
        band_red_ls = statsac_item.assets['B4']['href']
        band_nir_ls = statsac_item.assets['B5']['href']
        band_urls.append(band_red_ls)
        band_urls.append(band_nir_ls)
    elif 'B03' and 'B08' in statsac_item.assets:
        band_red_se = statsac_item.assets['B04']['href']
        band_nir_se = statsac_item.assets['B08']['href']
        band_urls.append(band_red_se)
        band_urls.append(band_nir_se)
    else:
        print('No red or near infrared Band available')
        sys.exit(1)
    assert len(band_urls) == 2, 'No Red or Nir-Band found. ' \
                                'Please check the sources or try ' \
                                'new Parameters'
    return band_urls



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


with rio.open(URLS_TIMESTEP_1[0]) as src_red_ts1:
    nols, nrows = src_red_ts1.meta['width'], src_red_ts1.meta['height']
    big_window = rio.windows.Window(col_off=0, row_off=0, width=nols, height=nrows)
    print(big_window)

    tiles = [window for ij, window in src_red_ts1.block_windows()]
    with rio.open(URLS_TIMESTEP_2[0]) as src_red_ts2:
        nols2, nrows2 = src_red_ts2.meta['width'], src_red_ts2.meta['height']
        big_window2 = rio.windows.Window(col_off=0, row_off=0, width=nols2, height=nrows2)
        print(big_window2)

        window_new = rio.windows.intersection(big_window2, big_window)
        print(window_new)
        out_profile = src_red_ts1.profile.copy()
        out_profile.update({'dtype': 'float32'})
        # open outfile with outprofile
        with rio.open('test.tif', "w", **out_profile) as dst:
            result = src_red_ts1.read(window=window_new)
            dst.write(result, window=window_new)













