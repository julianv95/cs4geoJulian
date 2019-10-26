"""
#!/bin/python
# -*- coding: utf8 -*-
# Author: J. Vetter, 2019
# Script containing the functions to
# calculate the difference between the
# NDVI of two different points in time using
# concurrent and tiled image processing.
###########################################
"""


import sys
from itertools import product
import concurrent.futures
import numpy as np
import rasterio as rio
from rasterio import windows
from rasterio.enums import Resampling
from satsearch import Search


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


# Concurrent Processing Functions
def calculate_ndvi(red, nir):
    """Uses a red and near infrared band in array-form to calculate the ndvi.
    The Output-Array will have the same size/shape as the input array.
    :parameter:
    red band as array
    infrared band as array
    :returns: Numpy-Array with ndvi values"""
    band_red = red.astype(rio.float32)
    band_nir = nir.astype(rio.float32)
    np.seterr(divide='ignore', invalid='ignore')

    # check array sizes
    assert band_red.shape == band_nir.shape, "This won't work, the tile sizes are different"

    # create empty array with the same shape as one of the input arrays
    ndvi = np.empty(nir.shape, dtype=rio.float32)
    search = np.logical_or(band_red > 0, band_nir > 0)
    # fill the empty array with the calculated ndvi values for each
    # cell where red and nir > 0 otherwise fill up with -2
    ndvi = np.where(search, (1.0 * (band_nir - band_red)) / (1.0 * (band_nir + band_red)), -2)

    return ndvi


def calculate_difference(ndvi_tile1, ndvi_tile2):
    """Calculates the difference between to arrays
    :parameter: Arrays containing the ndvi values
    :return: Numppy array with difference"""
    tile_1 = ndvi_tile1.astype(rio.float32)
    tile_2 = ndvi_tile2.astype(rio.float32)

    ndvi_difference = np.subtract(tile_1, tile_2)

    return ndvi_difference


def tiled_cacl_chunky(urls_timestep1, urls_timestep2, window, window_lst, window_idx=0):
    """Calculates the difference of the NDVI
    between to image tiles. In case the two images have a different shape,
    the red and nir band from urls_timestep2 are resampled to the size of
    the red and nir band from urls_timestep1
    :parameter:
    List for each Date containing the urls of the red and nir band,
    the window for the current tile,
    a list of all the windows used for the tiling process
    and the index of the current window
    :returns:
    Numpy-Array containing the difference of the two tiles"""

    # open red band and read window of timestep1
    with rio.open(urls_timestep1[0]) as src_red_ts1:
        red_block_ts1 = src_red_ts1.read(window=window)

    # open nir band and read window of timestep1
    with rio.open(urls_timestep1[1]) as src_nir_ts1:
        nir_block_ts1 = src_nir_ts1.read(window=window)

    # calculate ndvi for timestep1
    ndvi_ts_1 = calculate_ndvi(red_block_ts1, nir_block_ts1)
    # open red band and resample window of timestep2

    try:
        with rio.open(urls_timestep2[0]) as src_red_ts2_re:
            red_block_ts2_re = src_red_ts2_re.read(window=window,
                                                   out_shape=(
                                                       window.height,
                                                       window.width)
                                                   ,
                                                   resampling=Resampling.bilinear
                                                   )
        # open red band and resample window of timestep2
        with rio.open(urls_timestep2[1]) as src_nir_ts2_re:
            nir_block_ts2_re = src_nir_ts2_re.read(window=window,
                                                   out_shape=(
                                                       window.height,
                                                       window.width)
                                                   ,
                                                   resampling=Resampling.bilinear
                                                   )

    except rio.RasterioIOError:
        # Exception for special boundary-cases
        # Take window before error occurred, intersect with datasource boundaries(big_window),
        # Create new Window with the intersection, read the new window and resample it
        # to the size of the original window

        with rio.open(urls_timestep2[0]) as src_red_ts2_re:
            nols, nrows = src_red_ts2_re.meta['width'], src_red_ts2_re.meta['height']
            big_window = rio.windows.Window(col_off=0, row_off=0, width=nols, height=nrows)
            window_new = window_lst[window_idx-1].intersection(big_window)

            red_block_ts2_re = src_red_ts2_re.read(window=window_new,
                                                   out_shape=(
                                                       window.height,
                                                       window.width)
                                                   ,
                                                   resampling=Resampling.bilinear
                                                   )

        with rio.open(urls_timestep2[1]) as src_nir_ts2_re:
            nir_block_ts2_re = src_nir_ts2_re.read(window=window_new,
                                                   out_shape=(
                                                       window.height,
                                                       window.width)
                                                   ,
                                                   resampling=Resampling.bilinear
                                                   )


    # clalculate ndvi for timestep2
    ndvi_ts_2 = calculate_ndvi(red_block_ts2_re, nir_block_ts2_re)

    # check if both arrays have the same size
    assert ndvi_ts_1.shape == ndvi_ts_2.shape, 'The NDVI Shapes do not match'

    # calculate difference between timestep1 and 2
    result_block = calculate_difference(ndvi_ts_1, ndvi_ts_2)

    return result_block


# optimal tiling
def optimal_tiled_calc(statsac_item_ts1, statsac_item_ts2, outfile, max_workers=1):
    """Process infiles block-by-block, calculate the NDVI for each block,
    and write the difference to a new file. Uses Optimal block-size and
    concurrent processing. Uses the internal Blocks of statsac_item_ts1
    for tiling.
    :parameter:
    Statsac-Item Object of date x,
    Statsac-Item object of date y,
    Name of outfile,
    Number of Processors"""

    # get the urls for the red and nir bands for timestep1 and 2
    urls_timestep1 = get_urls(statsac_item_ts1)
    urls_timestep2 = get_urls(statsac_item_ts2)

    # start with concurrent processing
    # source: https://gist.github.com/sgillies/b90a79917d7ec5ca0c074b5f6f4857e3.js.
    # This was adapted for the ndvi processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        # Create a destination dataset based on source params. The
        # destination will be tiled, and tiles will be processed
        # concurrently.
        with rio.open(urls_timestep1[0]) as src_red:
            out_profile = src_red.profile.copy()
            out_profile.update({'dtype': 'float32'})
            # open outfile with outprofile
            with rio.open(outfile, "w", **out_profile) as dst:
                # create windows for tiling
                tiles = [window for ij, window in dst.block_windows()]
                counter = 0

                # chunkify(windows):
                for chunk in [tiles]:

                    future_to_window = dict()

                    for window in chunk:
                        future = executor.submit(tiled_cacl_chunky,
                                                 urls_timestep1,
                                                 urls_timestep2,
                                                 window,
                                                 tiles,
                                                 window_idx=counter)
                        future_to_window[future] = window
                        counter += 1

                    for future in concurrent.futures.as_completed(future_to_window):
                        window = future_to_window[future]
                        result = future.result()
                        dst.write(result, window=window)


# Customized Tiles Functions
def get_tiles(dataset, tile_a, tile_b):
    """Creates rasterio.windows for a given Band with the size of
    tile_a x tile_b
    :parameter:
    Open Source,
    tile-width in m,
    tile-height in m,
    :returns:
    rasterio window """

    # calculate Width and Height as Distance from Origin
    tile_x, tile_y = (dataset.bounds.left + tile_a, dataset.bounds.top - tile_b)
    height, width = dataset.index(tile_x, tile_y)

    # get max rows and cols of dataset
    nols, nrows = dataset.meta['width'], dataset.meta['height']
    # create offset for the window processing
    offsets = product(range(0, nols, width), range(0, nrows, height))
    # create big_window around the whole dataset
    big_window = windows.Window(col_off=0,
                                row_off=0,
                                width=nols,
                                height=nrows)

    # create custom set blocks
    for col_off, row_off in offsets:
        # get windows with the custom parameters
        # until it intersects with
        # the boundaries of the source dataset
        window = windows.Window(col_off=col_off,
                                row_off=row_off,
                                width=width,
                                height=height).intersection(big_window)
        transform = windows.transform(window, dataset.transform)
        yield window, transform


def customized_tiled_calc(statsac_item_ts1, statsac_item_ts2, outfile,
                          tile_size_x, tile_size_y, max_workers=1):
    """Process infiles block-by-block, calculate the NDVI for each block,
        and write the difference to a new file. Uses custom block-size.
        :parameter:
        Statsac-Item Object of date x,
        Statsac-Item object of date y,
        Name of outfile,
        tile size x,
        tile size y,
        Number of Processors"""

    # get the urls for the red and nir bands for timestep1 and 2
    urls_timestep1 = get_urls(statsac_item_ts1)
    urls_timestep2 = get_urls(statsac_item_ts2)
    # start with concurrent processing
    # source: https://gist.github.com/sgillies/b90a79917d7ec5ca0c074b5f6f4857e3.js.
    # This was adapted for the ndvi processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a destination dataset based on source params. The
        # destination will be tiled, and tiles will be processed
        # concurrently.
        with rio.open(urls_timestep1[0]) as src_red:
            out_profile = src_red.profile.copy()
            out_profile.update({'dtype': 'float32'})
            # open outfile with outprofile
            with rio.open(outfile, "w", **out_profile) as dst:
                # create windows for tiling
                tiles = []
                for window, transform in get_tiles(src_red, tile_size_x, tile_size_y):
                    out_profile['transform'] = transform
                    out_profile['width'], out_profile['height'] = window.width, window.height
                    tiles.append(window)
                # chunkify(windows)/ concurrent processing of the tiled_calc_function
                counter = 0

                for chunk in [tiles]:
                    future_to_window = dict()
                    for window in chunk:

                        future = executor.submit(tiled_cacl_chunky,
                                                 urls_timestep1,
                                                 urls_timestep2,
                                                 window,
                                                 tiles,
                                                 window_idx=counter)

                        future_to_window[future] = window
                        counter += 1

                    for future in concurrent.futures.as_completed(future_to_window):
                        window = future_to_window[future]
                        result = future.result()
                        dst.write(result, window=window)
