import numpy as np
from Main import calculate_ndvi, optimal_tiled_calc, customized_tiled_calc
import rasterio as rio
import nose


def test_calculate_ndvi():
    """Test if input and output array are the same size"""
    # Given
    red = np.array([[1,3,4],[1,3,5]])
    nir = np.array([[1,2,1],[8,9,7]])
    expected = nir.shape
    # Then
    result = calculate_ndvi(red, nir).shape
    # When
    assert result == expected


def test_zero_division_ndvi():
    """Test if Zero-Division is allowed"""
    



def test_tiled_calc_arr_size():
    """Test if optimal and customized tiled claculation give an output array with the same size"""
    # Given
    red = r'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/195/026/LC08_L1GT_195026_20180721_20180721_01_RT/LC08_L1GT_195026_20180721_20180721_01_RT_B4.TIF'
    nir = r'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/195/026/LC08_L1GT_195026_20180721_20180721_01_RT/LC08_L1GT_195026_20180721_20180721_01_RT_B5.TIF'
    # Then
    customized_calc = customized_tiled_calc(red, nir, 100000, 100000)
    customized_file = rio.open(customized_calc)
    customized_arr = customized_file.read(1)
    customized_shape = customized_arr.shape

    optimal_calc = optimal_tiled_calc(red, nir)
    optimal_file = rio.open(optimal_calc)
    optimal_arr = optimal_file.read(1)
    optimal_shape = optimal_arr.shape
    # When
    assert optimal_shape == customized_shape


def test_tiled_calc_content():
    """Test if optimal and customized tiled calculation give an output with the same content"""
    # Given
    red = r'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/195/026/LC08_L1GT_195026_20180721_20180721_01_RT/LC08_L1GT_195026_20180721_20180721_01_RT_B4.TIF'
    nir = r'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/195/026/LC08_L1GT_195026_20180721_20180721_01_RT/LC08_L1GT_195026_20180721_20180721_01_RT_B5.TIF'
    # Then
    customized_calc = customized_tiled_calc(red, nir, 100000, 100000)
    customized_file = rio.open(customized_calc)
    customized_arr = customized_file.read(1)

    optimal_calc = optimal_tiled_calc(red, nir)
    optimal_file = rio.open(optimal_calc)
    optimal_arr = optimal_file.read(1)
    # When
    assert np.array_equal(customized_arr, optimal_arr)




nose.main()

# Given
from rasterio.transform import from_origin
# Create Random-Rasterio Test-Files
# red = np.random.randint(1, 5, size=(100, 100)).astype(np.float)
# nir = np.random.randint(1, 5, size=(100, 100)).astype(np.float)
#
# transform = from_origin(472137, 5015782, 0.5, 0.5)
#
# dataset_red = rio.open('test1.tif', 'w', driver='GTiff',
#                        height=red.shape[0], width=red.shape[1],
#                        count=1, dtype=str(red.dtype),
#                        crs='+proj=utm +zone=10 +ellps=GRS80 +datum=NAD83 +units=m +no_defs',
#                        transform=transform)
#
# dataset_nir = rio.open('test1.tif', 'w', driver='GTiff',
#                        height=nir.shape[0], width=nir.shape[1],
#                        count=1, dtype=str(nir.dtype),
#                        crs='+proj=utm +zone=10 +ellps=GRS80 +datum=NAD83 +units=m +no_defs',
#                        transform=transform)
# dataset_red.write(red, 1)
# dataset_nir.write(nir, 1)
# dataset_red.close
# dataset_nir.close