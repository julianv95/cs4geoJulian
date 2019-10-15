import numpy as np
from Main_Parallized import *
import rasterio as rio
import nose


def test_nedvi_zeros():

    # Given
    red = np.zeros((1, 3, 3))
    nir = np.zeros((1, 3, 3))

    expected = np.array([[[-2., -2., -2.], [-2., -2., -2.], [-2., -2., -2.]]])

    # Then
    result = calculate_ndvi(red, nir)

    # Expected
    assert np.array_equal(result, expected)


def test_ndvi_zero_division():

    # Given
    red = np.array(([1, 2, 3], [0, 0, 0], [0, 0, 0]))
    nir = np.array(([1, 2, 3], [4, 5, 6], [0, 0, 0]))
    expected = np.array([[0., 0., 0.], [1., 1., 1.], [-2., -2., -2.]])

    # Then
    result = calculate_ndvi(red, nir)

    # Expected
    assert np.array_equal(result, expected)

def test_custom_tile_size():




nose.main()

# Given
