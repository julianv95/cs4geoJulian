# cs4geo Julian Vetter
#Tiled NDVI Calculation

This Script calculates the difference of the NDVI of an area of interest between two different points in time using
concurrent and tiled image processing. To prevent any environment issues it's recommended to use the Conda-Environment provided in the repository. The script is runnable from the command line.

python my_program.py -o config_file.json


To use this script you need to configure a json-config file which should look like this:

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

#The script operates as follows:

It searches for Landsat-images with the lowest cloud-coverage for the given Dates. The script always
uses the image of the first point in time as source for the outfile. Meaning in case the two satellite images
have a different size (shape) the image of the second point in time is resampled with bilinear interpolation
to match the shape of the first image. The Outfile will be created in the same directory as the script.
