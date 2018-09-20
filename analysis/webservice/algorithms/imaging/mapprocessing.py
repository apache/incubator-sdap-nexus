"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved


NOTE: This code is an experimental proof-of-concept. The algorithms and methods have not yet been vetted.
"""

import numpy as np
import math

from scipy.misc import imresize
from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw
import multiprocessing

import colortables
import colorization


NO_DATA_IMAGE = None


def translate_interpolation(interp):
    if interp.upper() == "LANCZOS":
        return Image.LANCZOS
    elif interp.upper() == "BILINEAR":
        return Image.BILINEAR
    elif interp.upper() == "BICUBIC":
        return Image.BICUBIC
    else:
        return Image.NEAREST


def get_xy_resolution(tile):
    """
    Computes the x/y (lon, lat) resolution of a tile
    :param tile: A tile
    :return: Resolution as (x_res, y_res)
    """

    # Sometimes there are NaN values in the latitude and longitude lists....
    # This is a quick hack to make it work for now. Don't actually keep it like this.

    lon_idx = int(round(len(tile.longitudes) / 2))
    lat_idx = int(round(len(tile.latitudes) / 2))

    x_res = abs(tile.longitudes[lon_idx] - tile.longitudes[lon_idx+1])
    y_res = abs(tile.latitudes[lat_idx] - tile.latitudes[lat_idx+1])

    return x_res, y_res


def positioning_for_tile(tile, tllr, canvas_height, canvas_width):
    """
    Computes the x/y position of a tile matrix within the larger canvas
    :param tile: A tile
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param canvas_height: Height of the canvas
    :param canvas_width: Width of the canvas
    :return: The top left pixels as (tl_pixel_y, tl_pixel_x)
    """

    tl_lat = tile.bbox.max_lat
    tl_lon = tile.bbox.min_lon

    max_lat = tllr[0] + 90.0
    min_lon = tllr[1] + 180.0
    min_lat = tllr[2] + 90.0
    max_lon = tllr[3] + 180.0

    tl_pixel_y = int(round((max_lat - (tl_lat + 90.0)) / (max_lat - min_lat) * canvas_height))
    tl_pixel_x = int(round((tl_lon + 180.0 - min_lon) / (max_lon - min_lon) * canvas_width))

    return tl_pixel_y, tl_pixel_x


def process_tile(tile, tllr, data_min, data_max, table, canvas_height, canvas_width):
    """
    Processes a tile for colorization and positioning
    :param tile: The tile
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param data_min: Minimum value
    :param data_max: Maximum value
    :param table: A color table
    :param canvas_height: Height of the canvas
    :param canvas_width: Width of the canvas
    :return: The tile image data and top left pixel positions as (tile_img_data, tl_pixel_y, tl_pixel_x)
    """

    tile_img_data = colorization.colorize_tile_matrix(tile.data[0], data_min, data_max, table)
    tl_pixel_y, tl_pixel_x = positioning_for_tile(tile, tllr, canvas_height, canvas_width)
    return (tile_img_data, tl_pixel_y, tl_pixel_x)


def process_tile_async(args):
    """
    A proxy for process_tile for use in multiprocessing. Accepts a list of parameters
    :param args: The list of parameters in the order accepted by process_tile
    :return: The results of process_tile
    """

    return process_tile(args[0], args[1], args[2], args[3], args[4], args[5], args[6])


def process_tiles(tiles, tllr, data_min, data_max, table, canvas_height, canvas_width):
    """
    Loops through a list of tiles and calls process_tile on each
    :param tiles: A list of tiles
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param data_min: The minimum value
    :param data_max: The maximum value
    :param table: A color table
    :param canvas_height: The height of the canvas
    :param canvas_width: The width of the canvas
    :return: The results of each call to process_tile in a list
    """

    results = []
    for tile in tiles:
        result = process_tile(tile, tllr, data_min, data_max, table, canvas_height, canvas_width)
        results.append(result)
    return results


def process_tiles_async(args):
    """
    A proxy for process_tiles for use in multiprocessing. Accepts a list of parameters.
    :param args: The list of parameters in the order accepted by process_tiles
    :return: The results of process_tiles
    """

    return process_tiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6])


def compute_canvas_size(tllr, x_res, y_res):
    """
    Computes the necessary size of the canvas given a tllr and spatial resolutions
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param x_res: The longitudinal (x) resolution
    :param y_res: The latitudinal (y) resolution
    :return: The canvas dimentions as (height, width)
    """

    max_lat = tllr[0]
    min_lon = tllr[1]
    min_lat = tllr[2]
    max_lon = tllr[3]

    canvas_width = int(math.ceil((max_lon - min_lon) / x_res))
    canvas_height = int(math.ceil((max_lat - min_lat) / y_res))

    return canvas_height, canvas_width


def compute_tiles_tllr(nexus_tiles):
    """
    Computes a tllr for a given list of nexus tiles.
    :param nexus_tiles: A list of nexus tiles
    :return: The top left, lower right coordinate boundaries of the tiles as (maxLat, minLon, minLat, maxLon)
    """

    min_lat = 90.0
    max_lat = -90.0
    min_lon = 180.0
    max_lon = -180.0

    for tile in nexus_tiles:
        tile_max_lat = tile.bbox.max_lat
        tile_min_lat = tile.bbox.min_lat
        tile_max_lon = tile.bbox.max_lon
        tile_min_lon = tile.bbox.min_lon

        min_lat = np.array((min_lat, tile_min_lat)).min()
        max_lat = np.array((max_lat, tile_max_lat)).max()
        min_lon = np.array((min_lon, tile_min_lon)).min()
        max_lon = np.array((max_lon, tile_max_lon)).max()

    return (max_lat, min_lon, min_lat, max_lon)


def trim_map_to_requested_tllr(data, reqd_tllr, data_tllr):
    """
    Trims a canvas to the requested tllr. Only trims (crops), will not expand.
    :param data: A canvas image data
    :param reqd_tllr: Requested top left, lower right boundaries as (maxLat, minLon, minLat, maxLon)
    :param data_tllr: Data (canvas) top left, lower right boundaries as (maxLat, minLon, minLat, maxLon)
    :return: The trimmed canvas data
    """

    data_height = data.shape[0]
    data_width = data.shape[1]

    max_lat = data_tllr[0]
    min_lat = data_tllr[2]

    max_lon = data_tllr[3]
    min_lon = data_tllr[1]

    reqd_max_lat = reqd_tllr[0]
    reqd_min_lat = reqd_tllr[2]

    reqd_max_lon = reqd_tllr[3]
    reqd_min_lon = reqd_tllr[1]

    t_pixel_y = int(round((max_lat - reqd_max_lat) / (max_lat - min_lat) * data_height))
    b_pixel_y = int(round((max_lat - reqd_min_lat) / (max_lat - min_lat) * data_height))

    l_pixel_x = int(round((reqd_min_lon - min_lon) / (max_lon - min_lon) * data_width))
    r_pixel_x = int(round((reqd_max_lon - min_lon) / (max_lon - min_lon) * data_width))

    # Make sure the top and left pixels are at least 0
    t_pixel_y = np.array((0, t_pixel_y)).max()
    l_pixel_x = np.array((0, l_pixel_x)).max()

    # Make sure the bottom and right pixels are at most the highest index in data
    b_pixel_y = np.array((len(data) - 1, b_pixel_y)).min()
    r_pixel_x = np.array((len(data[0]) - 1, r_pixel_x)).min()

    data = data[t_pixel_y:b_pixel_y, l_pixel_x:r_pixel_x]

    return data


def process_tiles_to_map(nexus_tiles, stats, reqd_tllr, width=None, height=None, force_min=None, force_max=None, table=colortables.get_color_table("grayscale"), interpolation="nearest"):
    """
    Processes a list of tiles into a colorized image map.
    :param nexus_tiles: A list of nexus tiles
    :param stats: Stats from Solr
    :param reqd_tllr: Requested top left, lower right image boundaries as (maxLat, minLon, minLat, maxLon)
    :param width: Requested output width. Will use native data resolution if 'None'
    :param height: Requested output height. Will use native data resolution if 'None'
    :param force_min: A forced minimum value for the data. Will use data minimum from 'stats' if 'None'
    :param force_max: A forced maximum value for the data. Will use data maximum from 'stats' if 'None'
    :param table: A color table
    :param interpolation: Resizing interpolation mode. Defaults to "nearest"
    :return: A colorized image map as a PIL Image object
    """

    data_min = stats["minValue"] if force_min is None else force_min
    data_max = stats["maxValue"] if force_max is None else force_max

    x_res, y_res = get_xy_resolution(nexus_tiles[0])

    tiles_tllr = compute_tiles_tllr(nexus_tiles)
    canvas_height, canvas_width = compute_canvas_size(tiles_tllr, x_res, y_res)

    data = np.zeros((canvas_height, canvas_width, 4))

    pool = multiprocessing.Pool(8)
    n = int(math.ceil(len(nexus_tiles) / 8))
    tile_chunks = [nexus_tiles[i * n:(i + 1) * n] for i in range((len(nexus_tiles) + n - 1) // n)]
    params = [(tiles, tiles_tllr, data_min, data_max, table, canvas_height, canvas_width) for tiles in tile_chunks]
    proc_results = pool.map(process_tiles_async, params)

    for results in proc_results:
        for result in results:
            tile_img_data, tl_pixel_y, tl_pixel_x = result

            # Subset the tile image data matrix to the max allowable size (prevent overflows on the 'data' matrix.)
            data_shape = data[tl_pixel_y:(tl_pixel_y + tile_img_data.shape[0]),tl_pixel_x:(tl_pixel_x + tile_img_data.shape[1])].shape
            tile_img_data = tile_img_data[:data_shape[0],:data_shape[1],:]

            data[tl_pixel_y:(tl_pixel_y + tile_img_data.shape[0]),tl_pixel_x:(tl_pixel_x + tile_img_data.shape[1])] = tile_img_data

    data = trim_map_to_requested_tllr(data, reqd_tllr, tiles_tllr)

    if width is not None and height is not None:
        data = imresize(data, (height, width), interp=interpolation)

    im = Image.fromarray(np.asarray(data, dtype=np.uint8))
    return im


def create_no_data(width, height):
    """
    Creates a 'No Data' image at the given width and height
    :param width: Output width
    :param height: Output height
    :return: A 'No Data' image as a PIL Image object
    """

    global NO_DATA_IMAGE
    if NO_DATA_IMAGE is None:
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        fnt = ImageFont.truetype('webservice/algorithms/imaging/Roboto/Roboto-Bold.ttf', 40)

        for x in range(0, width, 500):
            for y in range(0, height, 500):
                draw.text((x, y), "NO DATA", (180, 180, 180), font=fnt)
        NO_DATA_IMAGE = img

    return NO_DATA_IMAGE






def create_map(tile_service, tllr, ds, dataTimeStart, dataTimeEnd, width=None, height=None, force_min=None, force_max=None, table=colortables.get_color_table("grayscale"), interpolation="nearest"):
    """
    Creates a colorized data map given a dataset, tllr boundaries, timeframe, etc.
    :param tile_service: The Nexus tile service instance.
    :param tllr: The requested top left, lower right boundaries as (maxLat, minLon, minLat, maxLon)
    :param ds: The Nexus shortname for the requested dataset
    :param dataTimeStart: An allowable minimum date
    :param dataTimeEnd:  An allowable maximum date.
    :param width: An output width in pixels. Will use native data resolution if 'None'
    :param height: An output height in pixels. Will use native data resolution if 'None'
    :param force_min: Force a minimum data value. Will use data resultset minimum if 'None'
    :param force_max: Force a maximum data value. Will use data resultset maximum if 'None'
    :param table: A colortable
    :param interpolation: A image resize interpolation model. Defaults to 'nearest'
    :return: A colorized map image as a PIL Image object. Image will contain 'No Data' if no data was found within the given parameters.
    """

    assert len(tllr) == 4, "Invalid number of parameters for tllr"

    max_lat = tllr[0]
    min_lon = tllr[1]
    min_lat = tllr[2]
    max_lon = tllr[3]

    print "A"
    stats = tile_service.get_dataset_overall_stats(ds)

    print "B"
    daysinrange = tile_service.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, ds, dataTimeStart, dataTimeEnd)

    if len(daysinrange) > 0:
        print "D"
        ds1_nexus_tiles = tile_service.get_tiles_bounded_by_box_at_time(min_lat, max_lat, min_lon, max_lon, ds, daysinrange[0])

        print "E"
        img = process_tiles_to_map(ds1_nexus_tiles, stats, tllr, width, height, force_min, force_max,
                                   table, interpolation)
    else:
        img = create_no_data(width, height)

    return img