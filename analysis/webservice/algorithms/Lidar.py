# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import json
import logging
import os.path
import warnings
import zipfile
from datetime import datetime
from functools import partial
from io import BytesIO
from itertools import zip_longest, chain
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely.wkt as wkt
import xarray as xr
from geopy.distance import geodesic
from mpl_toolkits.axes_grid1 import make_axes_locatable
from PIL import Image
from pytz import timezone
from scipy.interpolate import griddata
from shapely.geometry import LineString
from shapely.errors import ShapelyError
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

logger = logging.getLogger(__name__)

# NOTE: Current implementation expects the data vars to be in separate collections named
# <root_name>_<var_name>


EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
NP_EPOCH = np.datetime64('1970-01-01T00:00:00')
PD_EPOCH = pd.Timestamp('1970-01-01T00:00:00')
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

# Precomputed resolutions of LIDAR data grids, computed over the entire ABoVE dataset
# Temporary solution to use for grid mapping
LAT_RES_AVG = 0.0005339746629669614
LON_RES_AVG = 0.0005339746629670938
LAT_RES_MIN = 0.0002819728712069036
LON_RES_MIN = 0.0002819728712069036


@nexus_handler
class LidarVegetation(NexusCalcHandler):
    name = "LIDAR Vegetation Data"
    path = "/stv/lidar"
    description = "Gets vegetation stats (canopy height, mean height, canopy cover)"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required."
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "renderType": {
            "name": "Rendering type",
            "type": "string",
            "description": "Type of rendering to perform. Must be either 2D or 3D. Default: 2D"
        },
        "output": {
            "name": "Output format",
            "type": "string",
            "description": "Desired output format. Must be one of \"PNG\", \"JSON\", \"NetCDF\", \"CSV\", or \"ZIP\" "
                           "if renderType == \"2D\"; \"PNG\" or \"GIF\" if renderType == \"3D\". Required.",
        },
        "latSlice": {
            "name": "Latitude slice",
            "type": "string",
            "description": "Comma separated values: latitude to slice on[,min lon of slice,max lon of slice]"
        },
        "lonSlice": {
            "name": "Longitude slice",
            "type": "string",
            "description": "Comma separated values: longitude to slice on[,min lat of slice,max lat of slice]"
        },
        "sliceWKT": {
            "name": "Slice WKT",
            "type": "string",
            "description": "WTK LineString representation of the slice along the Lidar data you wish to display. "
                           "Control the number of samples along this line with the sliceSamples param. Plot x-axes will"
                           " be distance along the line."
        },
        "sliceSamples": {
            "name": "Slice Samples",
            "type": "integer > 0",
            "description": "Number of points along sliceWKT to plot. Default = 100"
        },
        "orbit": {
            "name": "Orbit settings",
            "type": "comma-delimited pair of ints",
            "description": "If renderType = 3D and output==GIF, specifies the orbit to be used in the animation. "
                           "Format: elevation angle,orbit step. Default: 30, 10; Ranges: [-180,180],[1,90]"
        },
        "viewAngle": {
            "name": "Static view angle",
            "type": "comma-delimited pair of ints",
            "description": "If renderType = 3D and output==PNG, specifies the angle to be used for the render. Format: "
                           "azimuth,elevation angle. Default: 300,30; Ranges: [0,359],[-180,180]"
        },
        "frameDuration": {
            "name": "Frame duration",
            "type": "int",
            "description": "If renderType = 3D and output==GIF, specifies the duration of each frame in the animation "
                           "in milliseconds. Default: 100; Range: >=100"
        },
        "mapToGrid": {
            "name": "Map scenes to grid",
            "type": "boolean",
            "description": "If multiple LIDAR captures are matched in the defined bounds, map their points to a shared "
                           "lat/lon grid at a similar resolution to the source data. Otherwise, the data cannot be "
                           "merged & rendered properly and will thus raise an error. NOTE: this option may take a long "
                           "time to compute. Default: false"
        }
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        NexusCalcHandler.__init__(self, tile_service_factory, desired_projection='swath')

    def parse_arguments(self, request):
        # Parse input arguments

        try:
            ds = request.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        try:
            start_time = request.get_start_datetime()
            start_time = int((start_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = int((end_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(reason="Parameter 'b' is required. 'b' must be comma-delimited float "
                                                  "formatted as Minimum (Western) Longitude, Minimum (Southern) "
                                                  "Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude.")

        max_lat = bounding_polygon.bounds[3]
        min_lon = bounding_polygon.bounds[0]
        min_lat = bounding_polygon.bounds[1]
        max_lon = bounding_polygon.bounds[2]

        render_type = request.get_argument('renderType', '2D').upper()

        if render_type not in ['2D', '3D']:
            raise NexusProcessingException(
                reason=f'Missing or invalid required parameter: renderType = {render_type}',
                code=400
            )

        output = request.get_argument('output', '').upper()

        if (render_type == '2D' and output not in ['PNG', 'JSON', 'NETCDF', 'CSV', 'ZIP']) or \
                (render_type == '3D' and output not in ['PNG', 'GIF']):
            raise NexusProcessingException(
                reason=f'Missing or invalid required parameter: output = {output}',
                code=400
            )

        lat_slice = request.get_argument('latSlice', None)
        lon_slice = request.get_argument('lonSlice', None)

        slice_wkt = request.get_argument('sliceWKT', None)
        slice_samples = request.get_int_arg('sliceSamples', 100)

        slice_line = None

        if render_type == '2D':
            # if (lat_slice is not None or lon_slice is not None) and slice_wkt is not None:
            #     raise NexusProcessingException(
            #         reason='Cannot define latSlice and/or lonSlice and sliceWKT at the same time',
            #         code=400
            #     )

            if lat_slice is not None:
                parts = lat_slice.split(',')

                if len(parts) not in [1, 3]:
                    raise NexusProcessingException(
                        reason='latSlice must consist of either one number (lat to slice on), or 3 numbers separated by '
                               'commas (lat to slice on, min lon of slice, max lon of slice)', code=400
                    )

                try:
                    if len(parts) == 1:
                        lat_slice = (float(parts[0]), None, None)
                    else:
                        lat_slice = tuple([float(p) for p in parts])

                    if not (min_lat <= lat_slice[0] <= max_lat):
                        raise NexusProcessingException(
                            reason='Selected slice latitude is outside the selected bounding box', code=400
                        )
                except ValueError:
                    raise NexusProcessingException(
                        reason='Invalid numerical component provided. latSlice must consist of either one number (lat to '
                               'slice on), or 3 numbers separated by commas (lon to slice on, min lat of slice, max lat of '
                               'slice)', code=400
                    )

            if lon_slice is not None:
                parts = lon_slice.split(',')

                if len(parts) not in [1, 3]:
                    raise NexusProcessingException(
                        reason='lonSlice must consist of either one number (lon to slice on), or 3 numbers separated by '
                               'commas (lon to slice on, min lat of slice, max lat of slice)', code=400
                    )

                try:
                    if len(parts) == 1:
                        lon_slice = (float(parts[0]), None, None)
                    else:
                        lon_slice = tuple([float(p) for p in parts])

                    if not (min_lon <= lon_slice[0] <= max_lon):
                        raise NexusProcessingException(
                            reason='Selected slice longitude is outside the selected bounding box', code=400
                        )
                except ValueError:
                    raise NexusProcessingException(
                        reason='Invalid numerical component provided. lonSlice must consist of either one number (lon to '
                               'slice on), or 3 numbers separated by commas (lon to slice on, min lat of slice, max lat of '
                               'slice)', code=400
                    )

            if slice_wkt is not None:
                try:
                    slice_line = wkt.loads(slice_wkt)
                except ShapelyError as e:
                    logger.exception(e)
                    raise NexusProcessingException(
                        reason=f'Invalid WKT: {slice_wkt}',
                        code=400
                    )

                if not isinstance(slice_line, LineString):
                    raise NexusProcessingException(
                        reason=f'Invalid geometry type for sliceWKT: {slice_line.geom_type}. Expected LineString',
                        code=400
                    )

                if not slice_samples > 0:
                    raise NexusProcessingException(
                        reason='Slice samples must be > 0',
                        code=400
                    )

                if not bounding_polygon.intersects(slice_line):
                    raise NexusProcessingException(
                        reason='Selected line string is entirely outside the selected bounding box', code=400
                    )

        map_to_grid = request.get_boolean_arg('mapToGrid')

        orbit_elev, orbit_step, frame_duration, view_azim, view_elev = None, None, None, None, None

        if render_type == '3D':
            lat_slice = (None, None, None)
            lon_slice = (None, None, None)

            orbit_params = request.get_argument('orbit', '30,10')
            view_params = request.get_argument('viewAngle', '300,30')

            frame_duration = request.get_int_arg('frameDuration', 100)

            if output == 'GIF':
                try:
                    orbit_params = orbit_params.split(',')
                    assert len(orbit_params) == 2
                    orbit_elev, orbit_step = tuple([int(p) for p in orbit_params])

                    assert -180 <= orbit_elev <= 180
                    assert 1 <= orbit_step <= 90
                    assert frame_duration >= 100
                except:
                    raise NexusProcessingException(
                        reason=f'Invalid orbit parameters: {orbit_params} & {frame_duration}',
                        code=400
                    )

                view_azim, view_elev = None, None
            else:
                try:
                    view_params = view_params.split(',')
                    assert len(view_params) == 2
                    view_azim, view_elev = tuple([int(p) for p in view_params])

                    assert 0 <= view_azim <= 359
                    assert -180 <= view_elev <= 180
                except:
                    raise NexusProcessingException(reason=f'Invalid view angle string: {view_params}', code=400)

                orbit_elev, orbit_step = None, None

        return (ds, start_time, end_time, bounding_polygon, lon_slice, lat_slice, slice_line, slice_samples,
                map_to_grid, render_type, (orbit_elev, orbit_step, frame_duration, view_azim, view_elev))

    def calc(self, computeOptions, **args):
        warnings.filterwarnings('ignore', category=UserWarning)

        (dataset, start_time, end_time, bounding_polygon, lon_slice, lat_slice, slice_line, slice_samples, map_to_grid,
         render_type, params_3d) = self.parse_arguments(computeOptions)

        tile_service = self._get_tile_service()

        ds_zg = f'{dataset}_ZG'
        ds_rh50 = f'{dataset}_RH050'
        ds_rh98 = f'{dataset}_RH098'
        ds_cc = f'{dataset}_CC'

        get_tiles = partial(
            tile_service.find_tiles_in_polygon,
            bounding_polygon=bounding_polygon,
            start_time=start_time,
            end_time=end_time
        )

        tiles_zg = get_tiles(ds=ds_zg)
        tiles_rh50 = get_tiles(ds=ds_rh50)
        tiles_rh98 = get_tiles(ds=ds_rh98)
        tiles_cc = get_tiles(ds=ds_cc)

        logger.info(f'Matched tile counts by variable: ZG={len(tiles_zg):,}, RH050={len(tiles_rh50):,}, '
                    f'RH098={len(tiles_rh98):,}, CC={len(tiles_cc):,}')

        if all([len(t) == 0 for t in [tiles_zg, tiles_rh50, tiles_rh98, tiles_cc]]):
            raise NoDataException(reason='No data was found within the selected parameters')

        points_zg, points_50, points_98, points_cc = [], [], [], []

        sources = set()

        def source_name_from_granule(granule: str, end=-3) -> str:
            return '_'.join(granule.split('_')[:end])

        include_nan = map_to_grid  # If we may need to map to grid, get nans from source tiles so we can best estimate
        # grid resolution

        for tile_zg, tile_50, tile_98, tile_cc in zip_longest(tiles_zg, tiles_rh50, tiles_rh98, tiles_cc):
            if tile_zg:
                logger.info(f'Processing ground height tile {tile_zg.tile_id}')
                npg_zg = tile_zg.nexus_point_generator(include_nan=include_nan)
                sources.add(source_name_from_granule(tile_zg.granule))
            else:
                npg_zg = []

            if tile_50:
                logger.info(f'Processing mean vegetation height tile {tile_50.tile_id}')
                npg_50 = tile_50.nexus_point_generator(include_nan=include_nan)
                sources.add(source_name_from_granule(tile_50.granule))
            else:
                npg_50 = []

            if tile_98:
                logger.info(f'Processing canopy height tile {tile_98.tile_id}')
                npg_98 = tile_98.nexus_point_generator(include_nan=include_nan)
                sources.add(source_name_from_granule(tile_98.granule))
            else:
                npg_98 = []

            if tile_cc:
                logger.info(f'Processing canopy coverage tile {tile_cc.tile_id}')
                npg_cc = tile_cc.nexus_point_generator(include_nan=include_nan)
                sources.add(source_name_from_granule(tile_cc.granule, -4))
            else:
                npg_cc = []

            for np_zg, np_50, np_98, np_cc in zip_longest(
                    npg_zg,
                    npg_50,
                    npg_98,
                    npg_cc
            ):
                if np_zg:
                    p_zg = dict(
                        latitude=np_zg.latitude,
                        longitude=np_zg.longitude,
                        time=np_zg.time,
                        data=np_zg.data_vals,
                        source=source_name_from_granule(tile_zg.granule)
                    )

                    if isinstance(p_zg['data'], list):
                        p_zg['data'] = p_zg['data'][0]

                    points_zg.append(p_zg)

                if np_50:
                    p_50 = dict(
                        latitude=np_50.latitude,
                        longitude=np_50.longitude,
                        time=np_50.time,
                        data=np_50.data_vals,
                        source=source_name_from_granule(tile_50.granule)
                    )

                    if isinstance(p_50['data'], list):
                        p_50['data'] = p_50['data'][0]

                    points_50.append(p_50)

                if np_98:
                    p_98 = dict(
                        latitude=np_98.latitude,
                        longitude=np_98.longitude,
                        time=np_98.time,
                        data=np_98.data_vals,
                        source=source_name_from_granule(tile_98.granule)
                    )

                    if isinstance(p_98['data'], list):
                        p_98['data'] = p_98['data'][0]

                    points_98.append(p_98)

                if np_cc:
                    p_cc = dict(
                        latitude=np_cc.latitude,
                        longitude=np_cc.longitude,
                        time=np_cc.time,
                        data=np_cc.data_vals,
                        source=source_name_from_granule(tile_cc.granule, -4)
                    )

                    if isinstance(p_cc['data'], list):
                        p_cc['data'] = p_cc['data'][0]

                    p_cc['data'] = p_cc['data'] * 100

                    points_cc.append(p_cc)

        if len(sources) == 1:
            logger.info('Only one source LIDAR scene, using simple mapping to grid')

            lats = np.unique([p['latitude'] for p in chain(points_zg, points_50, points_98, points_cc)])
            lons = np.unique([p['longitude'] for p in chain(points_zg, points_50, points_98, points_cc)])
            times = np.unique(
                [datetime.utcfromtimestamp(p['time']) for p in chain(points_zg, points_50, points_98, points_cc)])

            vals_4d = np.full((len(times), len(lats), len(lons), 4), np.nan)

            data_dict = {}

            for zg, rh50, rh98, cc in zip_longest(points_zg, points_50, points_98, points_cc):
                if zg is not None:
                    key = (datetime.utcfromtimestamp(zg['time']), zg['latitude'], zg['longitude'])

                    if key not in data_dict:
                        data_dict[key] = [zg['data'], None, None, None]
                    else:
                        data_dict[key][0] = zg['data']

                if rh50 is not None:
                    key = (datetime.utcfromtimestamp(rh50['time']), rh50['latitude'], rh50['longitude'])

                    if key not in data_dict:
                        data_dict[key] = [None, rh50['data'], None, None]
                    else:
                        data_dict[key][1] = rh50['data']

                if rh98 is not None:
                    key = (datetime.utcfromtimestamp(rh98['time']), rh98['latitude'], rh98['longitude'])

                    if key not in data_dict:
                        data_dict[key] = [None, None, rh98['data'], None]
                    else:
                        data_dict[key][2] = rh98['data']

                if cc is not None:
                    key = (datetime.utcfromtimestamp(cc['time']), cc['latitude'], cc['longitude'])

                    if key not in data_dict:
                        data_dict[key] = [None, None, None, cc['data'] / 10000]
                    else:
                        data_dict[key][3] = cc['data'] / 10000

            for i, t in enumerate(times):
                for j, lat in enumerate(lats):
                    for k, lon in enumerate(lons):
                        vals_4d[i, j, k, :] = data_dict.get((t, lat, lon), [np.nan] * 4)

            ds = xr.DataArray(
                data=vals_4d,
                dims=['time', 'lat', 'lon', 'var'],
                coords=dict(
                    time=(['time'], times),
                    lat=(['lat'], lats),
                    lon=(['lon'], lons),
                    var=(['var'], ['ground_height', 'mean_veg_height', 'canopy_height', 'canopy_coverage'])
                )
            ).to_dataset('var')
        elif map_to_grid:
            logger.info('More than one scene matched, will align points to a common grid')

            min_lat, max_lat = tuple(
                np.unique([p['latitude'] for p in chain(points_zg, points_50, points_98, points_cc)])[[0, -1]]
            )

            min_lon, max_lon = tuple(
                np.unique([p['longitude'] for p in chain(points_zg, points_50, points_98, points_cc)])[[0, -1]]
            )

            times = np.unique(
                [datetime.utcfromtimestamp(p['time']) for p in chain(points_zg, points_50, points_98, points_cc)])

            logger.debug('Building gridding coordinate meshes')

            lons = np.arange(min_lon, max_lon + (LON_RES_MIN / 2), LON_RES_MIN)
            lats = np.arange(min_lat, max_lat + (LAT_RES_MIN / 2), LAT_RES_MIN)

            X, Y = np.meshgrid(lons, lats)

            logger.info(f'Output grid shape: {X.shape}')

            gridded_zg, gridded_50, gridded_98, gridded_cc = [], [], [], []

            source_point_map = {}

            for zg, rh50, rh98, cc in zip_longest(points_zg, points_50, points_98, points_cc):
                if zg is not None:
                    zg_source = zg['source']
                    source_point_map.setdefault(zg_source, {}).setdefault('zg', []).append(zg)

                if rh50 is not None:
                    rh50_source = rh50['source']
                    source_point_map.setdefault(rh50_source, {}).setdefault('50', []).append(rh50)

                if rh98 is not None:
                    rh98_source = rh98['source']
                    source_point_map.setdefault(rh98_source, {}).setdefault('98', []).append(rh98)

                if cc is not None:
                    cc_source = cc['source']
                    source_point_map.setdefault(cc_source, {}).setdefault('cc', []).append(cc)

            for src in sorted(source_point_map):
                logger.info(f'Gridding ground heights for {src}')
                source_points = source_point_map[src]
                gridded_zg.append(griddata(
                    list(zip([p['longitude'] for p in source_points['zg']], [p['latitude'] for p in source_points['zg']])),
                    np.array([p['data'] for p in source_points['zg']]),
                    (X, Y),
                    method='nearest',
                    fill_value=np.nan
                ))

                logger.info(f'Gridding mean vegetation heights for {src}')
                gridded_50.append(griddata(
                    list(zip([p['longitude'] for p in source_points['50']], [p['latitude'] for p in source_points['50']])),
                    np.array([p['data'] for p in source_points['50']]),
                    (X, Y),
                    method='nearest',
                    fill_value=np.nan
                ))

                logger.info(f'Gridding canopy heights for {src}')
                gridded_98.append(griddata(
                    list(zip([p['longitude'] for p in source_points['98']], [p['latitude'] for p in source_points['98']])),
                    np.array([p['data'] for p in source_points['98']]),
                    (X, Y),
                    method='nearest',
                    fill_value=np.nan
                ))

                logger.info(f'Gridding canopy coverage for {src}')
                gridded_cc.append(griddata(
                    list(zip([p['longitude'] for p in source_points['cc']], [p['latitude'] for p in source_points['cc']])),
                    np.array([p['data'] / 10000 for p in source_points['cc']]),
                    (X, Y),
                    method='nearest',
                    fill_value=np.nan
                ))

            gridded_zg = np.array(gridded_zg)
            gridded_50 = np.array(gridded_50)
            gridded_98 = np.array(gridded_98)
            gridded_cc = np.array(gridded_cc)

            logger.info('Flattening gridded flightlines to single 2D view')

            # Reduce 3D arrays of gridded flight lines to single 2D array, with each value being the most recent non-nan
            # value along axis 0. In other words, stack the flight lines on top of each other, with more recent flight
            # lines covering over previous ones

            gridded_zg = np.choose((~np.isnan(gridded_zg)).cumsum(0).argmax(0), gridded_zg)
            gridded_50 = np.choose((~np.isnan(gridded_50)).cumsum(0).argmax(0), gridded_50)
            gridded_98 = np.choose((~np.isnan(gridded_98)).cumsum(0).argmax(0), gridded_98)
            gridded_cc = np.choose((~np.isnan(gridded_cc)).cumsum(0).argmax(0), gridded_cc)

            gridded_vals = np.array([
                gridded_zg,
                gridded_50,
                gridded_98,
                gridded_cc
            ])

            gridded_vals = np.moveaxis(gridded_vals, 0, -1)[np.newaxis, ...]

            ds = xr.DataArray(
                data=gridded_vals,
                dims=['time', 'lat', 'lon', 'var'],
                coords=dict(
                    time=(['time'], [times[0]]),
                    lat=(['lat'], lats),
                    lon=(['lon'], lons),
                    var=(['var'], ['ground_height', 'mean_veg_height', 'canopy_height', 'canopy_coverage'])
                )
            ).to_dataset('var')
        else:
            warnings.filterwarnings('default', category=UserWarning)

            raise NexusProcessingException(
                reason='Selected bounds match multiple scenes, there is no way to merge them to a shared grid',
                code=400
            )

        slice_lat, slice_lon = None, None
        slice_min_lat, slice_max_lat = None, None
        slice_min_lon, slice_max_lon = None, None
        slice_wkt = None

        if lat_slice is not None:
            slice_lat, slice_min_lon, slice_max_lon = lat_slice

            if slice_min_lon is None:
                slice_min_lon = ds.lon.min().item()

            if slice_max_lon is None:
                slice_max_lon = ds.lon.max().item()

            if 'time' in ds.coords and len(ds['time']) > 1:
                slice_ds = ds.mean(dim='time', skipna=True)
            else:
                slice_ds = ds

            lat_slice = slice_ds.sel(lat=slice_lat, method='nearest').sel(lon=slice(slice_min_lon, slice_max_lon))

        if lon_slice is not None:
            slice_lon, slice_min_lat, slice_max_lat = lon_slice

            if slice_min_lat is None:
                slice_min_lat = ds.lat.min().item()

            if slice_max_lat is None:
                slice_max_lat = ds.lat.max().item()

            if 'time' in ds.coords and len(ds['time']) > 1:
                slice_ds = ds.mean(dim='time', skipna=True)
            else:
                slice_ds = ds

            lon_slice = slice_ds.sel(lon=slice_lon, method='nearest').sel(lat=slice(slice_min_lat, slice_max_lat))

        if slice_line is not None:
            point_coords = []

            for p in np.linspace(0, 1, slice_samples):
                point_coords.append(slice_line.interpolate(p, True).coords[0])

            point_coords = [dict(lon=p[0], lat=p[1]) for p in point_coords]

            slice_ds = ds.copy()

            if len(slice_ds['time']) > 1:
                logger.warning('slicing: ds has multiple time steps. Reducing them to 1')
                slice_ds = slice_ds.mean(dim='time')
            else:
                slice_ds = slice_ds.squeeze(dim=['time'], drop=True)

            points = []  # {coord: tuple (l,l), ground_height: float, mean_veg_height: float, canopy_height: float, canopy_coverage: float, distance: float}

            last_point = None

            for coord in point_coords:
                ds_sel = slice_ds.sel(coord, method='nearest')

                sel_coord = (ds_sel.lat.item(), ds_sel.lon.item())

                if last_point is None:
                    last_point = dict(
                        coord=sel_coord,
                        ground_height=ds_sel.ground_height.item(),
                        mean_veg_height=ds_sel.mean_veg_height.item(),
                        canopy_height=ds_sel.canopy_height.item(),
                        canopy_coverage=ds_sel.canopy_coverage.item(),
                        distance=0.0
                    )
                    points.append(last_point)
                elif sel_coord != last_point['coord'] or coord == point_coords[-1]:
                    next_distance = last_point['distance'] + abs(geodesic(sel_coord, last_point['coord']).m)

                    if not coord == point_coords[-1] and next_distance != last_point['distance']:
                        last_point = dict(
                            coord=sel_coord,
                            ground_height=ds_sel.ground_height.item(),
                            mean_veg_height=ds_sel.mean_veg_height.item(),
                            canopy_height=ds_sel.canopy_height.item(),
                            canopy_coverage=ds_sel.canopy_coverage.item(),
                            distance=next_distance
                        )
                        points.append(last_point)

            slice_wkt = slice_line.wkt
            slice_line = points

        slices = (lat_slice, lon_slice, slice_line)

        result_meta = dict(
            ground_height_dataset=ds_zg,
            vegetation_mean_height_dataset=ds_rh50,
            canopy_height_dataset=ds_rh98,
            canopy_coverage_dataset=ds_cc,
            start_time=times[0].strftime(ISO_8601),
            end_time=times[-1].strftime(ISO_8601),
            b=f'{lons[0]},{lats[0]},{lons[-1]},{lats[-1]}'
        )

        if lat_slice is not None:
            result_meta['slice_lat'] = (slice_lat, slice_min_lon, slice_max_lon)

        if lon_slice is not None:
            result_meta['slice_lon'] = (slice_lon, slice_min_lat, slice_max_lat)

        if slice_line is not None:
            result_meta['slice_wkt'] = slice_wkt
            result_meta['slice_samples_requested'] = slice_samples
            result_meta['slice_samples_actual'] = len(slice_line)

        warnings.filterwarnings('default', category=UserWarning)

        if render_type == '2D':
            results = LidarResults(
                results=(ds, slices),
                meta=result_meta
            )
        else:
            results = LidarResults3D(
                results=ds,
                meta=result_meta,
                render_params=params_3d
            )

        return results


class LidarResults(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **kwargs):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **kwargs)

    def meta(self):
        m = NexusResults.meta(self)

        try:
            del m['shortName']
        except KeyError:
            ...

        try:
            del m['bounds']
        except KeyError:
            ...

        return m

    def results(self, reduce_time=False) -> Tuple[xr.Dataset, tuple]:
        ds, (lat_slice, lon_slice, line_slice) = NexusResults.results(self)

        if reduce_time and len(ds['time']) > 1:
            ds = ds.mean(dim='time', skipna=True)

        return ds, (lat_slice, lon_slice, line_slice)

    def points_list(self):
        points = []
        slice_points = {}

        ds, (lat_slice, lon_slice, line_slice) = self.results()

        logger.info('Generating non-NaN points list')

        for pt in ds.to_dataframe().reset_index(level=['lat', 'lon', 'time']).itertuples():
            zg = getattr(pt, 'ground_height')
            rh50 = getattr(pt, 'mean_veg_height')
            rh98 = getattr(pt, 'canopy_height')
            cc = getattr(pt, 'canopy_coverage')

            if all([np.isnan(v) for v in [zg, rh50, rh98, cc]]):
                continue

            ts: pd.Timestamp = getattr(pt, 'time')
            point_ts = int((ts - PD_EPOCH).total_seconds())

            points.append(dict(
                latitude=getattr(pt, 'lat'),
                longitude=getattr(pt, 'lon'),
                time=point_ts,
                time_iso=datetime.utcfromtimestamp(point_ts).strftime(ISO_8601),
                ground_height=zg,
                mean_vegetation_height=rh50,
                canopy_height=rh98,
                canopy_coverage=cc
            ))

        if lat_slice is not None:
            if len(lat_slice['time']) > 1:
                lat_slice = lat_slice.mean(dim='time', skipna=True)
            else:
                lat_slice = lat_slice.squeeze(dim='time')

            pts = [dict(
                longitude=lat_slice.isel(lon=l).lon.item(),
                ground_height=lat_slice.ground_height.isel(lon=l).item(),
                mean_vegetation_height=lat_slice.mean_veg_height.isel(lon=l).item(),
                canopy_height=lat_slice.canopy_height.isel(lon=l).item(),
                canopy_coverage=lat_slice.canopy_coverage.isel(lon=l).item()
            ) for l in range(len(ds['lon']))]

            slice_points['latitude'] = dict(latitude=lat_slice.lat.item(), slice=pts)

        if lon_slice is not None:
            if len(lon_slice['time']) > 1:
                lon_slice = lon_slice.mean(dim='time', skipna=True)
            else:
                lon_slice = lon_slice.squeeze(dim='time')

            pts = [dict(
                latitude=lon_slice.isel(lat=l).lat.item(),
                ground_height=lon_slice.ground_height.isel(lat=l).item(),
                mean_vegetation_height=lon_slice.mean_veg_height.isel(lat=l).item(),
                canopy_height=lon_slice.canopy_height.isel(lat=l).item(),
                canopy_coverage=lon_slice.canopy_coverage.isel(lat=l).item()
            ) for l in range(len(ds['lat']))]

            slice_points['longitude'] = dict(longitude=lon_slice.lon.item(), slice=pts)

        if line_slice is not None:
            pts = [dict(
                latitude=p['coord'][0],
                longitude=p['coord'][1],
                distance_along_slice_line=p['distance'],
                ground_height=p['ground_height'],
                mean_vegetation_height=p['mean_veg_height'],
                canopy_height=p['canopy_height'],
                canopy_coverage=p['canopy_coverage']
            ) for p in line_slice]

            slice_points['line'] = dict(line=self.meta()['slice_wkt'], slice_point_count=len(pts), slice=pts)

        return points, slice_points

    @staticmethod
    def slice_point_list(ds, s, coord):
        if len(ds['time']) > 1:
            s = s.mean(dim='time', skipna=True)
        else:
            s = s.squeeze(dim='time')

        if coord == 'latitude':
            pts = [dict(
                s=s.isel(lon=l).lon.item(),
                ground_height=s.ground_height.isel(lon=l).item(),
                mean_vegetation_height=s.mean_veg_height.isel(lon=l).item(),
                canopy_height=s.canopy_height.isel(lon=l).item(),
                canopy_coverage=s.canopy_coverage.isel(lon=l).item()
            ) for l in range(len(s['lon']))]

            return s.lat.item(), pts
        else:
            pts = [dict(
                s=s.isel(lat=l).lat.item(),
                ground_height=s.ground_height.isel(lat=l).item(),
                mean_vegetation_height=s.mean_veg_height.isel(lat=l).item(),
                canopy_height=s.canopy_height.isel(lat=l).item(),
                canopy_coverage=s.canopy_coverage.isel(lat=l).item()
            ) for l in range(len(s['lat']))]

            return s.lon.item(), pts

    def toImage(self):
        ds, (lat_slice, lon_slice, line_slice) = self.results()
        meta = self.meta()

        slice_lat, slice_lon = meta.get('slice_lat'), meta.get('slice_lon')

        n_rows = 2

        if lat_slice is not None:
            n_rows += 2
        if lon_slice is not None:
            n_rows += 2
        if line_slice is not None:
            n_rows += 2

        min_lon, min_lat, max_lon, max_lat = (
            ds.lon.min().item(),
            ds.lat.min().item(),
            ds.lon.max().item(),
            ds.lat.max().item(),
        )

        extent = [min_lon, max_lon, min_lat, max_lat]

        fig = plt.figure(
            figsize=(10, 10 + (max(0, n_rows - 2) * 4)), constrained_layout=True
        )

        gs = fig.add_gridspec(n_rows, 4, width_ratios=[1, 0.05, 1, 0.05])

        rh50_ax = fig.add_subplot(gs[0, 0])
        rh50_cax = fig.add_subplot(gs[0, 1])
        rh98_ax = fig.add_subplot(gs[0, 2])
        rh98_cax = fig.add_subplot(gs[0, 3])
        zg_ax = fig.add_subplot(gs[1, 0])
        zg_cax = fig.add_subplot(gs[1, 1])
        cc_ax = fig.add_subplot(gs[1, 2])
        cc_cax = fig.add_subplot(gs[1, 3])

        rh50_im = rh50_ax.imshow(np.flipud(np.squeeze(ds['mean_veg_height'])), extent=extent, aspect='equal',
                                 cmap='viridis')
        rh98_im = rh98_ax.imshow(np.flipud(np.squeeze(ds['canopy_height'])), extent=extent, aspect='equal',
                                 cmap='viridis')
        zg_im = zg_ax.imshow(np.flipud(np.squeeze(ds['ground_height'])), extent=extent, aspect='equal', cmap='viridis')
        cc_im = cc_ax.imshow(np.flipud(np.squeeze(ds['canopy_coverage'])), extent=extent, aspect='equal',
                             cmap='viridis', vmin=0, vmax=100)

        if slice_lat is not None:
            rh50_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')
            rh98_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')
            zg_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')
            cc_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')

        if slice_lon is not None:
            rh50_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')
            rh98_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')
            zg_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')
            cc_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')

        if line_slice is not None:
            try:
                line = wkt.loads(self.meta()['slice_wkt'])
                line_coords = list(line.coords)
                rh50_ax.plot([c[0] for c in line_coords], [c[1] for c in line_coords], 'r--')
                rh98_ax.plot([c[0] for c in line_coords], [c[1] for c in line_coords], 'r--')
                zg_ax.plot([c[0] for c in line_coords], [c[1] for c in line_coords], 'r--')
                cc_ax.plot([c[0] for c in line_coords], [c[1] for c in line_coords], 'r--')
            except:
                ...

        rh50_ax.tick_params(axis='x', labelrotation=90)
        rh98_ax.tick_params(axis='x', labelrotation=90)
        zg_ax.tick_params(axis='x', labelrotation=90)
        cc_ax.tick_params(axis='x', labelrotation=90)

        rh50_cb = plt.colorbar(rh50_im, cax=rh50_cax, label='Height above terrain [m]', use_gridspec=True)
        rh98_cb = plt.colorbar(rh98_im, cax=rh98_cax, label='Height above terrain [m]', use_gridspec=True)
        zg_cb = plt.colorbar(zg_im, cax=zg_cax, label='Height above ellipsoid [m]', use_gridspec=True)
        cc_cb = plt.colorbar(cc_im, cax=cc_cax, label='Coverage [%]', use_gridspec=True)

        rh50_ax.set_title('Mean Vegetation Height')
        rh98_ax.set_title('Canopy Height')
        zg_ax.set_title('Terrain Height')
        cc_ax.set_title('Canopy Coverage')

        row = 2

        for s, coord in zip([lat_slice, lon_slice, line_slice], ['latitude', 'longitude', None]):
            if s is None:
                continue

            slice_ax = fig.add_subplot(gs[row, :])

            if coord in ['latitude', 'longitude']:
                slice_point, pts = LidarResults.slice_point_list(ds, s, coord)

                x_lim = [min_lon, max_lon] if coord == 'latitude' else [min_lat, max_lat]

                x_pts = [p['s'] for p in pts]
                rh50_pts = np.array([p['mean_vegetation_height'] for p in pts])
                rh98_pts = np.array([p['canopy_height'] for p in pts])
                zg_pts = np.array([p['ground_height'] for p in pts])
                cc_pts = np.array([p['canopy_coverage'] for p in pts])

                slice_ax.plot(
                    x_pts, rh98_pts + zg_pts,
                    x_pts, rh50_pts + zg_pts,
                    x_pts, zg_pts,
                )

                slice_ax.set_title(f'Slice at {coord}={slice_point}\nHeights w.r.t. to reference ellipsoid (m)')
                slice_ax.ticklabel_format(useOffset=False)
                slice_ax.set_xlim(x_lim)

                slice_ax.legend([
                    'Canopy Height',
                    'Mean Vegetation Height',
                    'Ground Height',
                ])

                cc_slice_ax = fig.add_subplot(gs[row + 1, :])

                cc_slice_ax.plot(x_pts, cc_pts)
                cc_slice_ax.ticklabel_format(useOffset=False)
                cc_slice_ax.set_ylim([0, 100])
                cc_slice_ax.set_xlim(x_lim)

                cc_slice_ax.set_title(f'Slice at {coord}={slice_point}\nCanopy coverage (%)')

                cc_slice_ax.legend(['Canopy Coverage'])
            else:
                x_pts = np.array([p['distance'] for p in s])
                rh50_pts = np.array([p['mean_veg_height'] for p in s])
                rh98_pts = np.array([p['canopy_height'] for p in s])
                zg_pts = np.array([p['ground_height'] for p in s])
                cc_pts = np.array([p['canopy_coverage'] for p in s])

                slice_ax.plot(
                    x_pts, rh98_pts + zg_pts,
                    x_pts, rh50_pts + zg_pts,
                    x_pts, zg_pts,
                )

                slice_ax.set_title(f'Slice along line\nHeights w.r.t. to reference ellipsoid (m)')
                slice_ax.ticklabel_format(useOffset=False)

                slice_ax.legend([
                    'Canopy Height',
                    'Mean Vegetation Height',
                    'Ground Height',
                ])

                cc_slice_ax = fig.add_subplot(gs[row + 1, :])

                cc_slice_ax.plot(x_pts, cc_pts)
                cc_slice_ax.ticklabel_format(useOffset=False)
                cc_slice_ax.set_ylim([0, 100])

                cc_slice_ax.set_title(f'Slice along line\nCanopy coverage (%)')

                cc_slice_ax.legend(['Canopy Coverage'])

            row += 2

        buffer = BytesIO()

        plt.savefig(buffer, format='png', facecolor='white')
        buffer.seek(0)

        plt.close(fig)
        return buffer.read()

    def toJson(self):
        points, slice_points = self.points_list()

        logger.info('Dumping to JSON string')

        return json.dumps(dict(
            meta=self.meta(),
            data=points,
            slices=slice_points
        ), indent=4)

    def toNetCDF(self):
        ds, (lat_slice, lon_slice, line_slice) = self.results()
        meta = self.meta()

        ds.attrs.update(meta)

        if lat_slice is not None:
            rename_dict = {var: f'lat_slice_{var}' for var in lat_slice.data_vars}
            lat_slice = lat_slice.rename(rename_dict)
            ds = xr.merge([ds, lat_slice])

        if lon_slice is not None:
            rename_dict = {var: f'lon_slice_{var}' for var in lon_slice.data_vars}
            lon_slice = lon_slice.rename(rename_dict)
            ds = xr.merge([ds, lon_slice])

        if line_slice is not None:
            slice_ds = xr.DataArray(
                data=np.array(
                    [
                        [p['ground_height'], p['mean_veg_height'], p['canopy_height'], p['canopy_coverage']]
                        for p in line_slice
                    ]
                ),
                dims=['distance', 'var'],
                coords=dict(
                    distance=(['distance'], np.array([p['distance'] for p in line_slice])),
                    var=(
                        ['var'],
                        ['slice_ground_height', 'slice_mean_veg_height', 'slice_canopy_height', 'slice_canopy_coverage']
                    )
                )
            ).to_dataset('var')
            ds = xr.merge([ds, slice_ds])

        with NamedTemporaryFile(suffix='.nc', mode='w') as fp:
            comp = {"zlib": True, "complevel": 9}
            encoding = {vname: comp for vname in ds.data_vars}

            ds.to_netcdf(fp.name, encoding=encoding)

            fp.flush()

            with open(fp.name, 'rb') as rfp:
                buf = BytesIO(rfp.read())

        buf.seek(0)
        return buf.read()

    def toCSV(self, points=None):
        if points is None:
            points, _ = self.points_list()
        df = pd.DataFrame(points)

        buffer = BytesIO()

        df.to_csv(buffer, index=False)

        buffer.seek(0)
        return buffer.read()

    def toZip(self):
        points, slice_points = self.points_list()

        csv_results = self.toCSV(points)

        csv_slices = []

        for slice_type in slice_points:
            s = slice_points[slice_type]

            if slice_type in ['latitude', 'longitude']:
                filename = f'{slice_type}_slice.csv'
            else:
                filename = 'line_slice.csv'

            slice_csv = self.toCSV(s['slice'])

            csv_slices.append((filename, slice_csv))

        buffer = BytesIO()

        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip:
            zip.writestr('lidar_subset.csv', csv_results)
            for s in csv_slices:
                zip.writestr(*s)

        buffer.seek(0)
        return buffer.read()


class LidarResults3D(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **kwargs):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **kwargs)
        self.render_params = kwargs['render_params']

    def results(self):
        ds: xr.Dataset = NexusResults.results(self)
        df: pd.DataFrame = ds.to_dataframe()

        return df.reset_index(level=['lon', 'lat'])

    def toImage(self):
        _, _, _, view_azim, view_elev = self.render_params

        results = self.results()

        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_subplot(111, projection='3d')
        ax.view_init(elev=view_elev, azim=view_azim)

        lats = np.unique(results['lat'].values)
        lons = np.unique(results['lon'].values)

        data_dict = {}

        for r in results.itertuples(index=False):
            key = (r.lon, r.lat)
            data_dict[key] = r.ground_height

        vals = np.empty((len(lats), len(lons)))

        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                vals[i, j] = data_dict.get((lon, lat), np.nan)

        X, Y = np.meshgrid(lons, lats)

        s = ax.plot_surface(X, Y, vals, rstride=1, cstride=1, color='xkcd:dirt')

        results = results[results['mean_veg_height'].notnull()]

        xy = results[['lon', 'lat']].values

        MARKER_SIZE = 1

        s1 = ax.scatter(
            xy[:, 0], xy[:, 1], results['mean_veg_height'].values + results['ground_height'].values,
            marker=',',
            alpha=results['canopy_coverage'].values,
            facecolors='brown',
            zdir='z',
            depthshade=True,
            s=MARKER_SIZE,
            linewidth=0
        )

        s2 = ax.scatter(
            xy[:, 0], xy[:, 1], results['canopy_height'].values + results['ground_height'].values,
            marker=',',
            alpha=results['canopy_coverage'].values,
            facecolors='xkcd:leaf',
            zdir='z',
            depthshade=True,
            s=MARKER_SIZE,
            linewidth=0
        )

        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')
        ax.set_zlabel('Elevation w.r.t. dataset reference (m)')

        plt.tight_layout()

        buffer = BytesIO()

        logger.info('Writing plot to buffer')
        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        return buffer.read()

    def toGif(self):
        orbit_elev, orbit_step, frame_duration, _, _ = self.render_params

        results = self.results()

        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_subplot(111, projection='3d')
        ax.view_init(elev=orbit_elev, azim=0)

        lats = np.unique(results['lat'].values)
        lons = np.unique(results['lon'].values)

        data_dict = {}

        for r in results.itertuples(index=False):
            key = (r.lon, r.lat)
            data_dict[key] = r.ground_height

        vals = np.empty((len(lats), len(lons)))

        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                vals[i, j] = data_dict.get((lon, lat), np.nan)

        X, Y = np.meshgrid(lons, lats)

        s = ax.plot_surface(X, Y, vals, rstride=1, cstride=1, color='xkcd:dirt')

        results = results[results['mean_veg_height'].notnull()]

        xy = results[['lon', 'lat']].values

        MARKER_SIZE = 1
        ALPHA_SCALING = 1.0

        s1 = ax.scatter(
            xy[:, 0], xy[:, 1], results['mean_veg_height'].values + results['ground_height'].values,
            marker=',',
            alpha=results['canopy_coverage'].values * ALPHA_SCALING,
            facecolors='brown',
            zdir='z',
            depthshade=True,
            s=MARKER_SIZE,
            linewidth=0
        )

        s2 = ax.scatter(
            xy[:, 0], xy[:, 1], results['canopy_height'].values + results['ground_height'].values,
            marker=',',
            alpha=results['canopy_coverage'].values * ALPHA_SCALING,
            facecolors='xkcd:leaf',
            zdir='z',
            depthshade=True,
            s=MARKER_SIZE,
            linewidth=0
        )

        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')
        ax.set_zlabel('Elevation w.r.t. dataset reference (m)')

        plt.tight_layout()

        buffer = BytesIO()

        with TemporaryDirectory() as td:
            for azim in range(0, 360, orbit_step):
                logger.info(f'Saving frame for azimuth = {azim}')

                ax.view_init(azim=azim)

                plt.savefig(os.path.join(td, f'fr_{azim}.png'))

            with contextlib.ExitStack() as stack:
                logger.info('Combining frames into final GIF')

                imgs = (stack.enter_context(Image.open(os.path.join(td, f'fr_{a}.png'))) for a in range(0, 360, orbit_step))
                img = next(imgs)
                img.save(buffer, format='GIF', append_images=imgs, save_all=True, duration=frame_duration, loop=0)

        buffer.seek(0)
        return buffer.read()
