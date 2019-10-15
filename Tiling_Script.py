from Main_Parallized import *
import json
import argparse
import os
from time import time
import multiprocessing as mp
from Main_Parallized import calculate_ndvi
import riomucho
import rasterio as rio

time1 = time()

#pool = mp.Pool(mp.cpu_count())

red = np.array(([1, 2, 3], [0, 0, 0], [0, 0, 0]))
nir = np.array(([1, 2, 3], [4, 5, 6], [0, 0, 0]))
ndvi = calculate_ndvi(red, nir)
x = np.array([[-2., -2., -2.], [-2., -2., -2.], [-2., -2., -2.]])
print(ndvi)




bbox = [8.66744, 49.41217, 8.68465, 49.42278]
date = ['2017-09-01/2018-12-04', '2018-06-01/2018-08-04']
property = "eo:cloud_cover<5"

time1 = time()
image_timestep1 = search_image(date[0], bbox, property)
image_timestep2 = search_image(date[1], bbox, property)
print("images found")

urls_timestep1 = get_urls(image_timestep1)
urls_timestep2 = get_urls(image_timestep2)
print(("got urls"))

#with rio.open(urls_timestep2[0]) as src:


tile_sizex, tile_sizey = 100000, 100000

outfile, num = 'test.tif', 4
#parallel_chunky(image_timestep1, image_timestep2, outfile, max_workers=num)

time2 = time()
time3 = time()

y = np.zeros((1, 4, 4))

print(y)
with rio.open('test.tif') as src:
    block = src.read()
    o = rio.dtypes.get_minimum_dtype(block)
    print(o)

#customized_tiled_calc(urls_timestep1[0], urls_timestep1[1], urls_timestep2[0], urls_timestep2[1], tile_sizex, tile_sizey)
time4 = time()


print(time2-time1)
print(time4-time3)