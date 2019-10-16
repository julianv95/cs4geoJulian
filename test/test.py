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
    # Given
    source = r'/Users/Julian/Downloads/ExerciseE-20190719/dsm.tif'
    tile_size_x = 100
    tile_size_y = 100
    counter = 0

    # Then
    with rio.open(source) as src:
        # create outfile and update datatype
        meta = src.meta.copy()
        outfile = 'customized_tiled_calc_ndvi.tif'

        x, y = (src.bounds.left + tile_size_x, src.bounds.top - tile_size_y)
        print(x,y)
        height, width = src.index(x, y)
        expected = [height, width]

        with rio.open(outfile, 'w', **meta) as dst:

            for window, transform in get_tiles(src, tile_size_x, tile_size_y):
                if counter == 0:
                    counter += 1
                    meta['transform'] = transform
                    meta['width'], meta['height'] = window.width, window.height
                    result_tile = src.read(window=window, masked=True)
                    dst.write(result_tile, window=window)
                else:
                    break

    with rio.open('customized_tiled_calc_ndvi.tif') as dst:

        v, w = (dst.bounds.left + tile_size_x, dst.bounds.top - tile_size_y)
        height_dst, width_dst = src.index(v, w)
        result = [height_dst, width_dst]

    # When
    assert result == expected







nose.main()

# Given
