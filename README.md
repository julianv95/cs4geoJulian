# cs4geo Julian Vetter
#Tiled NDVI Calculation

The scripts provided in this repository calculate the difference of the NDVI of an area of interest between two different points in time using concurrent and tiled image processing. To prevent any environment issues it's recommended to use the Conda-Environment provided in the repository. The scripts are runnable from the command line.

python tiling_script.py -o config_file.json

There are 2 scripts in this repository, tiling_script.py and tiling_script_intersection.py. The API used for the Image-Search-Function sometimes returns only marginally overlapping images for the area of interest. If thats the case it's recommended to use the latter script, which calculates the ndvi-difference for the intersection of the two images. This is necessary because tiling_script.py would interpolate the missing bits which would lead to an inaccurate result (only if there is to less overhang). To check wheter the images are only marginally overlapping you need to download the landsat-images of both timesteps and look at them in a GIS-program. If you run tiling_script.py the sources for the landsat-images will be printed in the console and you can download them from there. 

To use either script you need to configure a json-config file which should look like this:

'''
{
  "dates": ["2015-09-01/2015-12-04", "2016-06-01/2016-08-04"],
  
  "bounding_box": [8.66744, 49.41217, 8.68465, 49.42278],
  
  "property": "eo:cloud_cover<5",
  
  "tilex": 0,
    
  "tiley": 0,
    
  "outfile": "NDVI.tif",
    
  "processors": 4
}
'''

There is also a template in the repository.

Dates needs to be a List containing two different points in time. You can either choose to 
set a range like "2015-09-01/2015-12-04" or a single point in time like "2015-09-01".

The Bounding-Box should be self explanatory: A List containing the geographic coordinates of the
area of interest.

For now  the only property which is working for this script is cloud coverage. It 
needs to be formated as "eo:cloud_cover<5". The % is up to you.

Tilex and Tiley are only relevant if you are interested in using custom tiled blocks for the
ndvi-calculation. If you want to use it, you can choose the tile-size in meters with those parameters. The Tile-Size can't be larger than the image itself. If you leave it at 0 the optimal-tiled-processing is used as default.

The outfile should be self explanatory too: Just choose a name and add the suffix .tif. 

With processors you can choose the number of processing-units which will be used for the concurrent
processing. 

#The tiling_sricpt.py operates as follows:

It searches for Landsat-images with the lowest cloud-coverage for the given Dates. The script always
uses the image of the first point in time as source for the outfile. Meaning in case the two satellite images
have a different size (shape) the image of the second point in time is resampled with bilinear interpolation
to match the shape of the first image. The Outfile will be created in the same directory as the script.
Considering the NDVI Calculation it's important to note, that if the nir and red band both have the value 0 for a pixel, the corresponding
pixel in the ndvi-array will be assigned the value -2 and not 0.

#tiling_script_intersection.py operates as follows:

Basically the scripts operates in a similar way as tiling_script.py. The difference is that it only calculates the ndvi-difference for the intersection of the two Landsat-images. The rest of the ouput.tif will be filled up with the value 10. Considering the NDVI Calculation it's important to note, that if the nir and red band both have the value 0 for a pixel, the corresponding pixel in the ndvi-array will be assigned the value -2 and not 0. Furthermore it's important to say that the intersection is not 100% accurate and there will be some overhead which is not part of the intersection. But it should at least give an indication about the ndvi-difference in the inersecting areas.
