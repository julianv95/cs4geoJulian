"""Concurrent read-process-write test with local file"""


from Main2 import *
import concurrent.futures
from itertools import islice
from time import sleep
import numpy as np

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




def tiled_cacl(infile, window):

    with rio.open(infile) as src:
        red_block = src.read(window=window, masked=True)
        nir_block = src.read(window=window, masked=True)

    result_block = calculate_ndvi(red_block, nir_block)

    return result_block


def main(infile1, outfile, max_workers=1):

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        with rasterio.open(infile1) as src:
            out_profile = src.profile.copy()
            out_profile.update({'dtype': 'float64'})


            with rasterio.open(outfile, "w", **out_profile) as dst:

                windows = [window for ij, window in dst.block_windows()]

                for chunk in [windows]:  # chunkify(windows):

                    future_to_window = dict()

                    for window in chunk:
                        future = executor.submit(tiled_cacl, infile1, window)
                        future_to_window[future] = window

                    for future in concurrent.futures.as_completed(future_to_window):
                        window = future_to_window[future]
                        result = future.result()
                        dst.write(result, window=window)
                        print(dst)


if __name__ == "__main__":

    with rio.open(r'/Users/Julian/Downloads/ortho_RGBI (1).tif') as src:
        red = src.read(1)
        nir = src.read(4)

    outfile, num = 'test.tif', 4
    main(r'/Users/Julian/Downloads/ortho_RGBI (1).tif', outfile, max_workers=int(num))
