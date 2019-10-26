from satsearch import Search
import sys
import numpy as np
import rasterio as rio
from itertools import product
from rasterio import windows
import matplotlib.pyplot as plt


import multiprocessing as mp
print("Number of processors: ", mp.cpu_count())


search = Search(bbox=[8.66744,49.41217,8.68465,49.42278],
               datetime='2018-06-01/2018-08-04',
               property=["eo:cloud_cover<5"]
               )

items = search.items()
item = items[6]

def search_image(date, bb, prop):
    """Searches Satellite-Image for given Boundingbox, Date and Properties
    :parameter: singel Date or range of Days, Bounding Box as List and Properties as String
    :return: List containing statsac.Item Object with the lowest cloud-coverage for the given Date"""

    image = []


    # search image for given date or periode of time, always takes the first image
    search = Search(bbox=bb,
                    datetime=date,
                    property=[prop],
                    sort=[{'field': 'eo:cloud_cover', 'direction': 'desc'}]
                    )
    items = search.items()
    image.append(items[-1])

    assert len(image) == 1, 'No Images for given Parameters found. Please try new ones'

    return image


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

    # create empty array with the same shape as one of the input arrays
    ndvi = np.empty(nir.shape, dtype=rio.float32)# hier auch
    # No Zeoridivision
    check = np.logical_or(band_red > 0, band_nir > 0)
    # fill the empty array with the calculated ndvi values for each cell; -2 is broadcast to fill up the array
    ndvi = np.where(check, (1.0 * (band_nir - band_red)) / (1.0 * (band_nir + band_red)), -2)

    return ndvi


red = np.array([[0,0,4],[1,3,5]])
nir = np.array([[1,2,4],[8,9,7]])

ass = calculate_ndvi(red, nir)
print(ass)



def optimal_tiled_calc_old(red, nir):
    """Uses the red and near infared band and calcultes the ndvi in optimal tiled blocks and puts them back together
    resulting in a new raster image
    :parameter: filepath for red band and near infared band
    :returns: raster with the calculated ndvi values"""
    # open datasets
    src_red = rio.open(red)
    src_nir = rio.open(nir, 'r')

    # create outfile and update datatype
    out_profile = src_red.profile.copy()
    out_profile.update({'dtype': 'float64'})
    outfile = 'optimal_tiled_calc_ndvi.tif'
    dst = rio.open(outfile, 'w', **out_profile)

    # iterate over internal blocks of the bands, calculate ndvi for each block and put them back together
    for block_index, window in src_red.block_windows(1):
        red_block = src_red.read(window=window, masked=True)
        nir_block = src_nir.read(window=window, masked=True)

        result_block = calculate_ndvi(red_block, nir_block)
        dst.write(result_block, window=window)
        print(dst)
    # close dataset
    src_red.close()
    src_nir.close()
    dst.close()
    assert isinstance(dst, object)
    return outfile


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


def customized_tiled_calc(red, nir, tile_size_x, tile_size_y):
    """Tiles a band into blocks with the dimension of tile_a x tile_b, calculates the ndvi of the blocks and and puts
    them back together resulting in a new raster image
    :parameter: filepath for the red and infared band
    :returns:  raster with the calculated ndvi values """
    # open datasets
    src_red = rio.open(red)
    src_nir = rio.open(nir)

    # create outfile and update datatype
    meta = src_red.meta.copy()
    meta.update({'dtype': 'float64'})
    outfile = 'customized_tiled_calc_ndvi.tif'
    dst = rio.open(outfile, 'w', **meta)

    # iterate over custom set blocks, calculate ndvi for each block and put the blocks back together
    for window, transform in get_tiles(src_red, tile_size_x, tile_size_y):
        print(window)
        meta['transform'] = transform
        meta['width'], meta['height'] = window.width, window.height
        red_block = src_red.read(window=window, masked=True)
        nir_block = src_nir.read(window=window, masked=True)

        result_block = calculate_ndvi(red_block, nir_block)
        dst.write(result_block, window=window)

    src_red.close()
    src_nir.close()
    dst.close()
    return outfile


def calculate_difference(ndvi_tile1, ndvi_tile2):
    """Calculates the difference between to arrays
    :parameter: 2 Arrays
    :return: Array with difference"""
    tile_1 = ndvi_tile1.astype(float)  # datentyp noch ändern
    tile_2 = ndvi_tile2.astype(float)
    ndvi_difference = np.subtract(tile_1, tile_2)
    return ndvi_difference


def optimal_tiled_difference(ndvi1, ndvi2):
    """Uses two raster images containing the ndvi value of 2 different points in time and calculates the difference of
    the two images using optimal tiled blocks and returns a new raster image containing of the ndvi values
    :parameter: filepath of the to ndvi images (filepath of first scene, filepath of second scene)
    :returns: raster with the calculated difference"""
    src_timestep_1 = rio.open(ndvi1)
    src_timestep_2 = rio.open(ndvi2)

    # create outfile and update datatype
    out_profile = src_timestep_1.profile.copy()
    out_profile.update({'dtype': 'float64'})
    difference = rio.open(r'ndvi_difference.tif', 'w', **out_profile)

    assert len(set(src_timestep_1.block_shapes)) == 1

    # iterate over internal blocks of the bands, calculate the difference for each block and put them back together
    for block_index, window in src_timestep_1.block_windows(1):
        time1_block = src_timestep_1.read(window=window, masked=True)
        time2_block = src_timestep_2.read(window=window, masked=True)

        result_block = calculate_difference(time1_block, time2_block)
        difference.write(result_block, window=window)
        print(difference)

    # close datasets
    src_timestep_1.close()
    src_timestep_2.close()
    difference.close()
    assert isinstance(difference, object)
    return difference


def customized_tiled_difference(ndvi1, ndvi2, tile_size_x, tile_size_y):
    """Uses two raster images containing the ndvi value of 2 different points in time and calculates the difference of
        the two images using customized tiled blocks and returns a new raster image containing of the ndvi values
        :parameter: filepath of the ndvi images (filepath of first scene, filepath of second scene)
        :returns: raster with the calculated difference"""
    src_timestep_1 = rio.open(ndvi1)
    src_timestep_2 = rio.open(ndvi2)

    # create outfile and update datatype
    meta = src_timestep_1.meta.copy()
    meta.update({'dtype': 'float64'})
    difference = rio.open(r'ndvi_difference.tif', 'w', **meta)

    # customized tile calculation
    for window, transform in get_tiles(src_timestep_1, tile_size_x, tile_size_y):
        print(window)
        meta['transform'] = transform
        meta['width'], meta['height'] = window.width, window.height
        time1_block = src_timestep_1.read(window=window, masked=True)
        time2_block = src_timestep_2.read(window=window, masked=True)

        result_block = calculate_difference(time1_block, time2_block)
        difference.write(result_block, window=window)

    src_timestep_1.close()
    src_timestep_2.close()
    difference.close()
    return difference


#"2015-09-01/2015-12-04", "2016-06-01/2016-08-04"
#"2017-09-01/2017-12-04", "2018-06-01/2018-08-04"
with rio.open('NDVI.tif', 'r') as src:
    values = src.read()
    print('The minimum integer or floating point data type required to represent values is %s. ' % (rio.dtypes.get_minimum_dtype(values)))


#    print("The difference of the NDVIs has been calculated. ndvi_difference.tif has been saved in %s")


#optimal_tiled_calc(file1, file2)
