from satsearch import Search
import sys
import numpy as np
import rasterio as rio
from itertools import product
from rasterio import windows
import matplotlib.pyplot as plt


try:
    xmin, xmax, ymin, ymax = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
    bounding_box = [xmin, xmax, ymin, ymax]  # vllt noch aufräumen
    date_times = [sys.argv[5], sys.argv[6]]
    properties = sys.argv[7]
    tile_size_x, tile_size_y = 1000000, 1000000

except:
    print("Usage of this Script: Coordinates from Bounding Box, Date, Properties")
    sys.exit(1)


def search_image(bb, date, prop):
    """Searches Satellite-Image for given Boundingbox, Date and Properties
    :parameter: Boundingbox, Date, Property
    :return: Satstac.items.Item Object"""
    search = Search(bbox=bb,
                    datetime=date,
                    property=[prop]
                    # sort={'field'='eo:cloud_cover'}
                    )
    return search.items()


def search_dates(date_x):
    """Searches satellite-images for multiple dates for given list of timesteps
    :parameter: List of singel or range of Dates
    :return: List with satsac.Item Objects for the given Dates"""
    images = []

    # search image for given date or periode of time, always takes the first image
    for date in date_x:
        image = search_image(bounding_box, date, properties)
        images.append(image[0])
    return images


def get_urls(date):
    """Searches for the URLS of satellite-images for given date
    :parameter: List of Dates
    :returns: List of URLS containing the red and near infared Bands of the satellite-images.
    Order: Green1, Red1, Green2, Red2
    """
    band_urls = []

    # extract the urls for the red and near-infared band of the images for the given date or period of time
    for x in search_dates(date):
        # Check if Sentinel or Landsat
        if 'B3' and 'B4' in x.assets: # change infared and red band for sentinel
            band_red_se = x.assets['B3']['href']
            band_nir_se = x.assets['B4']['href']
            band_urls.append([band_red_se, band_nir_se])
        elif 'B03' and 'B04' in x.assets:
            band_red_ls = x.assets['B04']['href']
            band_nir_ls = x.assets['B05']['href']
            band_urls.append([band_red_ls, band_nir_ls])
        else:
            print('No red or near infared Band available')
            sys.exit(1)
    return band_urls


def calculate_ndvi(red, nir):
    """Uses a red and near infared band in array-form to claculate the ndvi
    :parameter: red band and infared band as arrays
    :returns: array with ndvi values"""
    red = red.astype(float) #datentyp noch ändern
    nir = nir.astype(float)
    np.seterr(divide='ignore', invalid='ignore')

    # create empty array with the same shape as one of the input arrays
    ndvi = np.empty(nir.shape, dtype=rio.float32)# hier auch
    # No Zeoridivision
    check = np.logical_or(red > 0, nir > 0)
    # fill the empty array with the calculated ndvi values for each cell; -2 is broadcast to fill up the array
    ndvi = np.where(check, (1.0 * (nir - red)) / (1.0 * (nir + red)), -2)

    return ndvi


#file1 = r'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/195/026/LC08_L1GT_195026_20180721_20180721_01_RT/LC08_L1GT_195026_20180721_20180721_01_RT_B4.TIF'
#file2 = r'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/195/026/LC08_L1GT_195026_20180721_20180721_01_RT/LC08_L1GT_195026_20180721_20180721_01_RT_B5.TIF'


def optimal_tiled_calc(red, nir):
    """Uses the red and near infared band and calcultes the ndvi in optimal tiled blocks and puts them back together
    resulting in a new raster image
    :parameter: filepath for red band and near infared band
    :returns: raster with the calculated ndvi values"""
    # open datasets
    src_red = rio.open(red)
    src_nir = rio.open(nir)

    # create outfile and update datatype
    out_profile = src_red.profile.copy()
    out_profile.update({'dtype': 'float64'})
    dst = rio.open(r'result.tif', 'w', **out_profile)

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
    return dst


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

    for col_off, row_off in offsets:
        window = windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(big_window)
        transform = windows.transform(window, ds.transform)
        yield window, transform


def customized_tiled_calc(red, nir):
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
    dst = rio.open(r'result.tif', 'w', **meta)

    # customized tile calculation
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
    return dst


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

    # close dataset
    src_timestep_1.close()
    src_timestep_2.close()
    difference.close()
    assert isinstance(difference, object)
    return difference


def customized_tiled_difference(ndvi1, ndvi2):
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

#    print("The difference of the NDVIs has been calculated. ndvi_difference.tif has been saved in %s")
