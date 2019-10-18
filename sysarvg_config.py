import numpy as np
from satsearch import Search
import sys
import numpy as np
import rasterio as rio
from itertools import product
from rasterio import windows
import concurrent.futures
from rasterio.enums import Resampling

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

        # calculate ndvi for timestep1
        ndvi_ts_1 = calculate_ndvi(red_block_ts1, nir_block_ts1)

        # open red band and read window of timestep2
        with rio.open(urls_timestep2[0]) as src_red_ts2:
            #red_block_ts2 = src_red_ts2.read(window=window)

            red_block_ts2 = src_red_ts2.read(window=window,
                                             out_shape=(
                                                 red_block_ts1.height,
                                                 red_block_ts1.width,
                                                 red_block_ts1.count),
                                             resampling=Resampling.bilinear
                                             )

        # open nir band and read window of timestep2
        with rio.open(urls_timestep2[1]) as src_nir_ts2:
            #nir_block_ts2 = src_nir_ts2.read(window=window)

            nir_block_ts2 = src_nir_ts2.read(window=window,
                                             out_shape=(
                                                 red_block_ts1.height,
                                                 red_block_ts1.width,
                                                 red_block_ts1.count),
                                             resampling=Resampling.bilinear
                                             )

    # calculate ndvi for timestep2
    ndvi_ts_2 = calculate_ndvi(red_block_ts2, nir_block_ts2)

    # check if both arrays have the same size
    assert ndvi_ts_2.shape == ndvi_ts_1.shape

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
