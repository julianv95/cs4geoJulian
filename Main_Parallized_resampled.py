#!/bin/python
# -*- coding: utf8 -*-
# Author: J. Vetter, 2019
# Script containing the functions to
# calculating the difference between the
# NDVI of two different points in time using
# concurrent and tiled image processing.
###########################################


from satsearch import Search
import os
import sys
import numpy as np
import rasterio as rio
from itertools import product
from rasterio import windows
import concurrent.futures
from rasterio.enums import Resampling



# search sources
def search_image(date, bb, prop):
    """Searches Satellite-Image for given Boundingbox, Date and Properties
    :parameter:
    singel Date,
    Bounding Box as List
    Properties as String
    :return:
    statsac.Item Object with the lowest
    cloud-coverage for the given Date"""

    image = []

    # search image for given date or periode of time,
    # always takes the first image
    search = Search(bbox=bb,
                    datetime=date,
                    property=[prop],
                    sort=[{'field': 'eo:cloud_cover', 'direction': 'asc'}]
                    )

    items = search.items()

    # filter for Landsatimages since Sentinel doesn't work and the
    # collection option for sat-search seems to be broken
    workaround = [str(item) for item in items]
    counter = 0

    for z in workaround:
        if 'S2' in z:
            counter += 1
            pass
        else:
            image.append(items[counter])
            break

    # check if image was found
    assert len(image) == 1, 'No Images for given Parameters found. ' \
                            'Please try new ones'

    return items[counter]


def get_urls(statsac_item):
    """Searches for the URLS of satellite-images for given date
    :parameter:
    satsac.Item Object
    :returns:
    List of URLS containing the red and near infared Bands of the satellite-images.
    Order: RED, NIR
    """
    band_urls = []

    # extract the urls for the red and near-infared band of the images for the given date or period of time
    # Check if Landsat or Sentinel
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
        print('No red or near infared Band available')
        sys.exit(1)
    assert len(band_urls) == 2, 'No Red or Nir-Band found. Please check the sources or try new Parameters'
    return band_urls


# Concurrent Processing Functions
def calculate_ndvi(red, nir):
    """Uses a red and near infared band in array-form to claculate the ndvi
    :parameter:
    red band as array
    infared band as array
    :returns: array with ndvi values"""
    band_red = red.astype(rio.float32)
    band_nir = nir.astype(rio.float32)
    np.seterr(divide='ignore', invalid='ignore')

    # check array sizes
    assert band_red.shape == band_nir.shape, "This won't work, the tile sizes are different"

    # create empty array with the same shape as one of the input arrays
    ndvi = np.empty(nir.shape, dtype=rio.float32)
    check = np.logical_or(band_red > 0, band_nir > 0)
    # fill the empty array with the calculated ndvi values for each cell where red and nir > 0 otherwise fill up with -2
    ndvi = np.where(check, (1.0 * (band_nir - band_red)) / (1.0 * (band_nir + band_red)), -2)

    return ndvi


def calculate_difference(ndvi_tile1, ndvi_tile2):
    """Calculates the difference between to arrays
    :parameter: 2 Arrays
    :return: Numppy array with difference"""
    tile_1 = ndvi_tile1.astype(rio.float32)
    tile_2 = ndvi_tile2.astype(rio.float32)
    ndvi_difference = np.subtract(tile_1, tile_2)
    return ndvi_difference


def tiled_cacl_chunky(urls_timestep1, urls_timestep2, window):
    """Calculates the difference of the NDVI
    between to image tiles.
    :parameter:
    List for each Date containing the urls for the red and nir band,
    the windows for tiling
    :returns: Numpy-Array containing the difference of the two tiles"""

    # open red band and read window of timestep1
    with rio.open(urls_timestep1[0]) as src_red_ts1:
        red_block_ts1 = src_red_ts1.read(window=window)

    # open nir band and read window of timestep1
    with rio.open(urls_timestep1[1]) as src_nir_ts1:
        nir_block_ts1 = src_nir_ts1.read(window=window)

    print(red_block_ts1.shape, nir_block_ts1.shape)

    # calculate ndvi for timestep1
    ndvi_ts_1 = calculate_ndvi(red_block_ts1, nir_block_ts1)

    with rio.open(urls_timestep2[0]) as src_red_ts2_re:
        red_block_ts2_re = src_red_ts2_re.read(window=window,
                                               out_shape=(
                                                    window.height,
                                                    window.width,
                                                    ),
                                               resampling=Resampling.bilinear
                                               )

    with rio.open(urls_timestep2[1]) as src_nir_ts2_re:
        nir_block_ts2_re = src_nir_ts2_re.read(window=window,
                                               out_shape=(
                                                    window.height,
                                                    window.width,
                                                    ),
                                               resampling=Resampling.bilinear
                                               )
    print(red_block_ts2_re.shape)
    print(nir_block_ts2_re.shape)
    ndvi_ts_2 = calculate_ndvi(red_block_ts2_re, nir_block_ts2_re)

    # check if both arrays have the same size
    assert ndvi_ts_1.shape == ndvi_ts_2.shape

    # calculate difference between timestep1 and 2
    result_block = calculate_difference(ndvi_ts_1, ndvi_ts_2)



    return result_block


# optimal tiling
def optimal_tiled_calc(statsac_item_ts1, statsac_item_ts2, outfile, max_workers=1):
    """Process infiles block-by-block, calculate the NDVI for each block,
    and write the difference to a new file. Uses Optimal block-size.
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
                windows = [window for ij, window in dst.block_windows()]

                # chunkify(windows):
                for chunk in [windows]:

                    future_to_window = dict()

                    for window in chunk:
                        future = executor.submit(tiled_cacl_chunky, urls_timestep1, urls_timestep2, window)
                        future_to_window[future] = window

                    for future in concurrent.futures.as_completed(future_to_window):
                        window = future_to_window[future]
                        result = future.result()
                        dst.write(result, window=window)
                        print(dst)


# Customized Tiles Functions
def get_tiles(ds, tile_a, tile_b):
    """Creates windows for a given Band with the size of
    tile_a x tile_b
    :parameter:
    Open Source,
    tile-width in m,
    tile-height in m,
    :returns:
    rasterio window """

    # calculate Width and Height as Distance from Origin
    x, y = (ds.bounds.left + tile_a, ds.bounds.top - tile_b)
    height, width = ds.index(x, y)

    # get max rows and cols of dataset
    nols, nrows = ds.meta['width'], ds.meta['height']
    # create offset for the window processing
    offsets = product(range(0, nols, width), range(0, nrows, height))
    # create big_window around the whole dataset
    big_window = windows.Window(col_off=0, row_off=0, width=nols, height=nrows)

    # create custom set blocks
    for col_off, row_off in offsets:
        # get windows with the custom parameters until it intersects with
        # the boundaries of the source dataset
        window = windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(big_window)
        transform = windows.transform(window, ds.transform)
        yield window, transform


def customized_tiled_calc(statsac_item_ts1, statsac_item_ts2, outfile, tile_size_x, tile_size_y, max_workers=1):
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
                windows = []

                for window, transform in get_tiles(src_red, tile_size_x, tile_size_y):
                    out_profile['transform'] = transform
                    out_profile['width'], out_profile['height'] = window.width, window.height
                    windows.append(window)

                # chunkify(windows)/ concurrent processing of the tiled_calc_function
                for chunk in [windows]:

                    future_to_window = dict()

                    for window in chunk:
                        future = executor.submit(tiled_cacl_chunky, urls_timestep1, urls_timestep2, window)
                        future_to_window[future] = window

                    for future in concurrent.futures.as_completed(future_to_window):
                        window = future_to_window[future]
                        result = future.result()
                        dst.write(result, window=window)
                        print(dst)


