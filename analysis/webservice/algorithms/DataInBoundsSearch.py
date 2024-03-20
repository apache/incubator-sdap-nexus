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


import io
import gzip
import json
import numpy
import logging

from datetime import datetime
from pytz import timezone

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


@nexus_handler
class DataInBoundsSearchCalcHandlerImpl(NexusCalcHandler):
    name = "Data In-Bounds Search"
    path = "/datainbounds"
    description = "Fetches point values for a given dataset and geographical area"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation. Required"
        },
        "parameter": {
            "name": "Parameter",
            "type": "string",
            "description": "The parameter of interest. One of 'sst', 'sss', 'wind'. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required if 'metadataFilter' not provided"
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
        "metadataFilter": {
            "name": "Metadata Filter",
            "type": "string",
            "description": "Filter in format key:value. Required if 'b' not provided"
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

        parameter_s = request.get_argument('parameter', None)
        if parameter_s not in ['sst', 'sss', 'wind', None]:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

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

        bounding_polygon = metadata_filter = None
        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            metadata_filter = request.get_metadata_filter()
            if 0 == len(metadata_filter):
                raise NexusProcessingException(
                    reason="'b' or 'metadataFilter' argument is required. 'b' must be comma-delimited float formatted "
                           "as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, "
                           "Maximum (Northern) Latitude. 'metadataFilter' must be in the form key:value",
                    code=400)

        min_elevation, max_elevation = request.get_elevation_args()

        if (min_elevation and max_elevation) and min_elevation > max_elevation:
            raise NexusProcessingException(
                reason='Min elevation must be less than or equal to max elevation',
                code=400
            )

        compact_result = request.get_boolean_arg('compact')

        return ds, parameter_s, start_time, end_time, bounding_polygon, metadata_filter, min_elevation, max_elevation, compact_result

    def calc(self, computeOptions, **args):
        ds, parameter, start_time, end_time, bounding_polygon,\
        metadata_filter, min_elevation, max_elevation, compact = self.parse_arguments(computeOptions)

        includemeta = computeOptions.get_include_meta()

        log = logging.getLogger(__name__)

        min_lat = max_lat = min_lon = max_lon = None
        tile_service = self._get_tile_service()

        if bounding_polygon:
            min_lat = bounding_polygon.bounds[1]
            max_lat = bounding_polygon.bounds[3]
            min_lon = bounding_polygon.bounds[0]
            max_lon = bounding_polygon.bounds[2]

            tiles = tile_service.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, ds,  start_time, end_time,
                                                   min_elevation=min_elevation, max_elevation=max_elevation, fetch_data=False)

            need_to_fetch = True
        else:
            tiles = self._get_tile_service().get_tiles_by_metadata(metadata_filter, ds, start_time, end_time)
            need_to_fetch = False

        data = []

        log.info(f'Matched {len(tiles):,} tiles.')

        for i in range(len(tiles)-1, -1, -1): # tile in tiles:
            tile = tiles.pop(i)

            tile_id = tile.tile_id

            log.info(f'Processing tile {tile_id} | {i=}')

            if need_to_fetch:
                tile = tile_service.fetch_data_for_tiles(tile)[0]
                tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, [tile])
                tile = tile_service.mask_tiles_to_time_range(start_time, end_time, tile)

                if min_elevation is not None and max_elevation is not None:
                    tile = tile_service.mask_tiles_to_elevation(min_elevation, max_elevation, tile)

                if len(tile) == 0:
                    log.info(f'Skipping empty tile {tile_id}')
                    continue

                tile = tile[0]

            for nexus_point in tile.nexus_point_generator():

                point = dict()
                point['id'] = tile.tile_id

                if parameter == 'sst':
                    point['sst'] = nexus_point.data_vals
                elif parameter == 'sss':
                    point['sss'] = nexus_point.data_vals
                elif parameter == 'wind':
                    point['wind_u'] = nexus_point.data_vals
                    try:
                        point['wind_v'] = tile.meta_data['wind_v'][tuple(nexus_point.index)]
                    except (KeyError, IndexError):
                        pass
                    try:
                        point['wind_direction'] = tile.meta_data['wind_dir'][tuple(nexus_point.index)]
                    except (KeyError, IndexError):
                        pass
                    try:
                        point['wind_speed'] = tile.meta_data['wind_speed'][tuple(nexus_point.index)]
                    except (KeyError, IndexError):
                        pass
                else:
                    variables = []

                    data_vals = nexus_point.data_vals if tile.is_multi else [nexus_point.data_vals]

                    for value, variable in zip(data_vals, tile.variables):
                        if variable.standard_name:
                            var_name = variable.standard_name
                        else:
                            var_name = variable.variable_name

                        variables.append({var_name: value})

                    point['variables'] = variables

                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'time': nexus_point.time,
                    'elevation': nexus_point.depth,
                    'data': point
                })

        if includemeta and len(tiles) > 0:
            meta = [tile.get_summary() for tile in tiles]
        else:
            meta = None

        result = DataInBoundsResult(
            results=data,
            stats={},
            meta=meta,
            compact=compact
        )

        result.extendMeta(min_lat, max_lat, min_lon, max_lon, "", start_time, end_time)

        log.info(f'Finished subsetting. Generated {len(data):,} points')

        return result


class DataInBoundsResult(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, compact=False, **args):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)
        self.__compact = compact

    def toCSV(self):
        rows = []

        headers = [
            "id",
            "lon",
            "lat",
            "time"
        ]

        for i, result in enumerate(self.results()):
            cols = []

            cols.append(str(result['data'][0]['id']))
            cols.append(str(result['longitude']))
            cols.append(str(result['latitude']))
            cols.append(datetime.utcfromtimestamp(result["time"]).strftime('%Y-%m-%dT%H:%M:%SZ'))
            if 'sst' in result['data'][0]:
                cols.append(str(result['data'][0]['sst']))
                if i == 0:
                    headers.append("sea_water_temperature")
            elif 'sss' in result['data'][0]:
                cols.append(str(result['data'][0]['sss']))
                if i == 0:
                    headers.append("sea_water_salinity")
            elif 'wind_u' in result['data'][0]:
                cols.append(str(result['data'][0]['wind_u']))
                cols.append(str(result['data'][0]['wind_v']))
                cols.append(str(result['data'][0]['wind_direction']))
                cols.append(str(result['data'][0]['wind_speed']))
                if i == 0:
                    headers.append("eastward_wind")
                    headers.append("northward_wind")
                    headers.append("wind_direction")
                    headers.append("wind_speed")

            if i == 0:
                rows.append(",".join(headers))
            rows.append(",".join(cols))

        return "\r\n".join(rows)

    def toJson(self):
        if not self.__compact:
            return json.dumps(self.results(), indent=4, cls=NpEncoder)
        else:
            buffer = io.BytesIO()
            with gzip.open(buffer, 'wt', encoding='ascii') as zip:
                json.dump(self.results(), zip, cls=NpEncoder)

            buffer.seek(0)
            return buffer.read()

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.floating):
            return float(obj)
        return super(NpEncoder, self).default(obj)
