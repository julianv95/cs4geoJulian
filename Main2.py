from satsearch import Search
import sys
import numpy as np
import rasterio as rio
from itertools import product
from rasterio import windows
from rasterio.enums import Resampling
import matplotlib.pyplot as plt


def search_image(date, bb, prop):
    """Searches Satellite-Image for given Boundingbox, Date and Properties
    :parameter: singel Date or range of Days, Bounding Box as List and Properties as String
    :return: List containing statsac.Item Object with the lowest cloud-coverage for the given Date"""

    image = []

    # search image for given date or periode of time, always takes the first image
    search = Search(bbox=bb,
                    datetime=date,
                    property=[prop],
                    sort=[{'field': 'eo:cloud_cover', 'direction': 'asc'}]
                    )

    items = search.items()

    # filter for Landsatimages since Sentinel doesn't work and the collection option for sat-search seems to be broken
    workaround = [str(item) for item in items]
    counter = 0

    for z in workaround:
        if 'S2' in z:
            counter += 1
            print(counter)
            pass
        else:
            image.append(items[counter])
            print(items[counter])
            break

    # check if image was found
    assert len(image) == 1, 'No Images for given Parameters found. Please try new ones'

    return items[counter]


def get_urls(statsac_item):
    """Searches for the URLS of satellite-images for given date
    :parameter: List of satsac.Item Objects
    :returns: List of URLS containing the red and near infared Bands of the satellite-images.
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
        #raise wenn kein band ausgewählt wurde bzw. keine daten für den Termin vorliegen
        print('No red or near infared Band available')
        sys.exit(1)
    assert len(band_urls) == 2, 'No Red or Nir-Band found. Please check the sources or try new Parameters'
    return band_urls


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
    ndvi_difference = np.subtract(tile_1, tile_2)
    return ndvi_difference


def optimal_tiled_calc(red_timestep_1, nir_timestep_1, red_timestep_2, nir_timestep_2):
    """Uses the red and near infared band and calcultes the ndvi in optimal tiled blocks and puts them back together
    resulting in a new raster image
    :parameter: filepath for red band and near infared band
    :returns: raster with the calculated ndvi values"""
    # open datasets
    src_red_timestep_1 = rio.open(red_timestep_1, 'r')
    src_nir_timestep_1 = rio.open(nir_timestep_1, 'r')
    src_red_timestep_2 = rio.open(red_timestep_2, 'r')
    src_nir_timestep_2 = rio.open(nir_timestep_2, 'r')


    # create outfile and update datatype
    out_profile = src_red_timestep_1.profile.copy()
    out_profile.update({'dtype': 'float64'})
    outfile = 'optimal_tiled_calc_ndvi.tif'
    dst = rio.open(outfile, 'w', **out_profile)

    # iterate over internal blocks of the bands, calculate ndvi for each block and put them back together
    for block_index, window in src_red_timestep_1.block_windows(1):
        # read Data for Timestep1 and calculate NDVI
        red_tile_timestep_1 = src_red_timestep_1.read(window=window, masked=True,
                                                      out_shape=(
                                                          src_red_timestep_2.height,
                                                          src_red_timestep_2.width,
                                                          src_red_timestep_2.count),
                                                      resampling=Resampling.bilinear
                                                      )
        print(red_tile_timestep_1.shape)

        nir_tile_timestep_1 = src_nir_timestep_1.read(window=window, masked=True,
                                                      out_shape=(
                                                          src_red_timestep_2.height,
                                                          src_red_timestep_2.width,
                                                          src_red_timestep_2.count),
                                                      resampling=Resampling.bilinear
                                                      )
        print(nir_tile_timestep_1.shape)
        ndvi_tile_timestep_1 = calculate_ndvi(red_tile_timestep_1, nir_tile_timestep_1)

        # read Data for Timestep2 and calculate NDVI
        red_tile_timestep_2 = src_red_timestep_2.read(window=window,
                                                      masked=True,
                                                      out_shape=(
                                                          src_red_timestep_2.height,
                                                          src_red_timestep_2.width,
                                                          src_red_timestep_2.count),
                                                      resampling=Resampling.bilinear
                                                      )
        print(red_tile_timestep_1.shape, red_tile_timestep_2.shape)
        nir_tile_timestep_2 = src_nir_timestep_2.read(window=window,
                                                      masked=True,
                                                      out_shape=(
                                                          src_red_timestep_2.height,
                                                          src_red_timestep_2.width,
                                                          src_red_timestep_2.count),
                                                      resampling=Resampling.bilinear
                                                      )
        ndvi_tile_timestep_2 = calculate_ndvi(red_tile_timestep_2, nir_tile_timestep_2)

        # check if both arrays have the same size
        assert ndvi_tile_timestep_1.shape == ndvi_tile_timestep_1.shape

        # Calculate difference between Timestep1 and Timestep2
        result_tile = calculate_difference(ndvi_tile_timestep_1, ndvi_tile_timestep_2)

        # Write Result into new Raster-File
        dst.write(result_tile, window=window)
        print(dst)

    # close datasets
    src_red_timestep_1.close()
    src_nir_timestep_1.close()
    dst.close()
    assert isinstance(dst, object)


def get_tiles(ds, tile_a, tile_b):
    """Tiles a band into blocks with the dimension of tile_a x tile_b
    :parameter: Band, tile-width and tile-height
    :returns: rasterio window """
    # calculate Width and Height as Distance from Origin
    x, y = (ds.bounds.left + tile_a, ds.bounds.top - tile_b)
    height, width = ds.index(x, y)

    nols, nrows = ds.meta['width'], ds.meta['height']
    offsets = product(range(0, nols, width), range(0, nrows, height))
    big_window = windows.Window(col_off=0, row_off=0, width=nols, height=nrows)

    # create custom set blocks
    for col_off, row_off in offsets:
        window = windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(big_window)
        transform = windows.transform(window, ds.transform)
        yield window, transform


def customized_tiled_calc(red_timestep_1, nir_timestep_1, red_timestep_2, nir_timestep_2, tile_size_x, tile_size_y):
    """Tiles a band into blocks with the dimension of tile_a x tile_b, calculates the ndvi of the blocks and and puts
    them back together resulting in a new raster image
    :parameter: filepath for the red and infared band
    :returns:  raster with the calculated ndvi values """
    # open datasets
    src_red_timestep_1 = rio.open(red_timestep_1)
    src_nir_timestep_1 = rio.open(nir_timestep_1, 'r')
    src_red_timestep_2 = rio.open(red_timestep_2, 'r')
    src_nir_timestep_2 = rio.open(nir_timestep_2, 'r')

    # create outfile and update datatype
    meta = src_red_timestep_1.meta.copy()
    meta.update({'dtype': 'float64'})
    outfile = 'customized_tiled_calc_ndvi.tif'
    dst = rio.open(outfile, 'w', **meta)

    # iterate over custom set blocks, calculate ndvi for each block and put the blocks back together
    for window, transform in get_tiles(src_red_timestep_1, tile_size_x, tile_size_y):
        meta['transform'] = transform
        meta['width'], meta['height'] = window.width, window.height

        # read Data for Timestep1 and calculate NDVI
        red_tile_timestep_1 = src_red_timestep_1(window=window, masked=True)
        nir_tile_timestep_1 = src_nir_timestep_1(window=window, masked=True)
        ndvi_tile_timestep_1 = calculate_ndvi(red_tile_timestep_1, nir_tile_timestep_1)

        # read Data for Timestep2 and calculate NDVI
        red_tile_timestep_2 = src_red_timestep_2(window=window, masked=True)
        nir_tile_timestep_2 = src_nir_timestep_2(window=window, masked=True)
        ndvi_tile_timestep_2 = calculate_ndvi(red_tile_timestep_2, nir_tile_timestep_2)

        # check if both arrays have the same size
        assert ndvi_tile_timestep_1.shape == ndvi_tile_timestep_1.shape

        # Calculate difference between Timestep1 and Timestep2
        result_tile = calculate_difference(ndvi_tile_timestep_1, ndvi_tile_timestep_2)

        # Write Result into new Raster-File
        dst.write(result_tile, window=window)

    # close Datasets
    src_red_timestep_1.close()
    src_nir_timestep_1.close()
    src_red_timestep_2.close()
    src_nir_timestep_2.close()
    dst.close()
