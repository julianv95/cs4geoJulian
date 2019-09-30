try:
    xmin, xmax, ymin, ymax = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
    bounding_box = [xmin, xmax, ymin, ymax]  # vllt noch aufräumen
    date_times = [sys.argv[5], sys.argv[6]]
    properties = sys.argv[7]
    tile_size_x, tile_size_y = 1000000, 1000000

except:
    print("Usage of this Script: Coordinates from Bounding Box, Date, Properties")
    sys.exit(1)


def search_image(date, bb, prop):
    """Searches Satellite-Image for given Boundingbox, Date and Properties
    :parameter: Boundingbox, Date, Property
    :return: Satstac.items.Item Object"""
    search = Search(bbox=bb,
                    datetime=date,
                    property=[prop]
                    # sort={'field'='eo:cloud_cover'}
                    )
    return search.items()