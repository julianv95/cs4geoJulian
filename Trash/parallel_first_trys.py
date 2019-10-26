"""Concurrent read-process-write example"""


from Trash.Main2 import *
import concurrent.futures
from itertools import islice
import numpy as np
from time import time
import threading

import rasterio


CHUNK = 100


def chunkify(iterable, chunk=CHUNK):
    it = iter(iterable)
    while True:
        piece = list(islice(it, chunk))
        if piece:
            yield piece
        else:
            return

def calculate_ndvi(red, nir):
    """Uses a red and near infared band in array-form to claculate the ndvi
    :parameter: red band and infared band as arrays
    :returns: array with ndvi values"""
    band_red = red.astype(float) #datentyp noch ändern
    band_nir = nir.astype(float)
    np.seterr(divide='ignore', invalid='ignore')

    # check array sizes
    assert band_red.shape == band_nir.shape, 'This wont work'

    # create empty array with the same shape as one of the input arrays
    ndvi = np.empty(nir.shape, dtype=rio.float32)# hier auch
    # No Zeoridivision
    check = np.logical_or(band_red > 0, band_nir > 0)
    # fill the empty array with the calculated ndvi values for each cell; -2 is broadcast to fill up the array
    ndvi = np.where(check, (1.0 * (band_nir - band_red)) / (1.0 * (band_nir + band_red)), -2)

    return ndvi


def calculate_difference(ndvi_tile1, ndvi_tile2):
    """Calculates the difference between to arrays
    :parameter: 2 Arrays
    :return: Array with difference"""
    tile_1 = ndvi_tile1.astype(float)  # datentyp noch ändern
    tile_2 = ndvi_tile2.astype(float)
    ndvi_difference = np.subtract(tile_2, tile_1)
    return ndvi_difference


def tiled_cacl_chunky(urls_timestep1, urls_timestep2, window):

    with rio.open(urls_timestep1[0]) as src_red_ts1:
        red_block_ts1 = src_red_ts1.read(window=window)

    with rio.open(urls_timestep1[1]) as src_nir_ts1:
        nir_block_ts1 = src_nir_ts1.read(window=window)

    ndvi_ts_1 = calculate_ndvi(red_block_ts1, nir_block_ts1)

    with rio.open(urls_timestep2[0]) as src_red_ts2:
        red_block_ts2 = src_red_ts2.read(window=window)

    with rio.open(urls_timestep2[1]) as src_nir_ts2:
        nir_block_ts2 = src_nir_ts2.read(window=window)

    ndvi_ts_2 = calculate_ndvi(red_block_ts2, nir_block_ts2)

    result_block = calculate_difference(ndvi_ts_1, ndvi_ts_2)

    return result_block


def parallel_chunky(statsac_item_ts1, statsac_item_ts2, outfile, max_workers=1):

    urls_timestep1 = get_urls(statsac_item_ts1)
    urls_timestep2 = get_urls(statsac_item_ts2)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        with rasterio.open(urls_timestep1[0]) as src_red:
            out_profile = src_red.profile.copy()
            out_profile.update({'dtype': 'float64'}, tiled=True)
            with rasterio.open(outfile, "w", **out_profile) as dst:

                windows = [window for ij, window in dst.block_windows()]

                for chunk in [windows]:  # chunkify(windows):

                    future_to_window = dict()

                    for window in chunk:
                        future = executor.submit(tiled_cacl_chunky, urls_timestep1, urls_timestep2, window)
                        future_to_window[future] = window

                    for future in concurrent.futures.as_completed(future_to_window):
                        window = future_to_window[future]
                        result = future.result()
                        dst.write(result, window=window)
                        print(dst)


def main(statsac_item_ts1, statsac_item_ts2, outfile, max_workers=1):
    """Process infile block-by-block and write to a new file

    The output is the same as the input, but with band order
    reversed.
    """
    rls_timestep1 = get_urls(statsac_item_ts1)
    urls_timestep2 = get_urls(statsac_item_ts2)

    with rasterio.Env():

        with rasterio.open(urls_timestep1[0]) as src_red:

            # Create a destination dataset based on source params. The
            # destination will be tiled, and we'll process the tiles
            # concurrently.
            out_profile = src_red.profile.copy()
            out_profile.update({'dtype': 'float64'}, tiled=True)

            with rasterio.open(outfile, "w", **out_profile) as dst:

                # Materialize a list of destination block windows
                # that we will use in several statements below.
                windows = [window for ij, window in dst.block_windows()]

                # This generator comprehension gives us raster data
                # arrays for each window. Later we will zip a mapping
                # of it with the windows list to get (window, result)
                # pairs.
                data_gen = (src.read(window=window) for window in windows)

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:

                    # We map the compute() function over the raster
                    # data generator, zip the resulting iterator with
                    # the windows list, and as pairs come back we
                    # write data to the destination dataset.
                    for window, result in zip(
                        windows, executor.map(compute, data_gen)
                    ):
                        dst.write(result, window=window)

def tiled_locked(statsac_item, outfile, max_workers=1):

    urls_timestep2 = get_urls(statsac_item)

    with rasterio.open(urls_timestep2[1]) as src_nir:

        with rasterio.open(urls_timestep2[0]) as src_red:
            out_profile = src_red.profile.copy()
            out_profile.update({'dtype': 'float64'})

            with rasterio.open(outfile, "w", **out_profile) as dst:

                windows = [window for ij, window in dst.block_windows()]

                read_lock = threading.Lock()
                write_lock = threading.Lock()

                def process(window):
                    with read_lock:
                        red_tile = src_red.read(window=window)
                        nir_tile = src_nir.read(window=window)

                    result_tile = calculate_ndvi(red_tile, nir_tile)
                    with write_lock:
                        dst.write(result_tile, window=window)
                    print(dst)

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    executor.map(process, windows)


if __name__ == "__main__":
    rasteri

    bbox = [8.66744, 49.41217, 8.68465, 49.42278]
    date = ['2015-09-01/2015-12-04', '2016-06-01/2016-08-04']
    property = "eo:cloud_cover<5"

    time1 = time()
    image_timestep1 = search_image(date[0], bbox, property)
    image_timestep2 = search_image(date[0], bbox, property)

    urls_timestep1 = get_urls(image_timestep1)
    urls_timestep2 = get_urls(image_timestep2)

    outfile, num = 'test.tif', 4
    parallel_chunky(image_timestep1, image_timestep2, outfile, max_workers=num)

    time2 = time()
    print('This took %i seconds' % (time2-time1))
