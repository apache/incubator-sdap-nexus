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

import logging
import re
from datetime import datetime
from decimal import Decimal
from typing import Tuple, Union

from pytz import UTC
from shapely.geometry import Polygon
from webservice.webmodel.RequestParameters import RequestParameters
from webservice.webmodel.StatsComputeOptions import StatsComputeOptions


class NexusRequestObject(StatsComputeOptions):
    shortNamePattern = re.compile("^[a-zA-Z0-9_\-,\.]+$")
    floatingPointPattern = re.compile('[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?')

    def __init__(self, reqHandler):
        self.__log = logging.getLogger(__name__)
        if reqHandler is None:
            raise Exception("Request handler cannot be null")
        self.requestHandler = reqHandler
        StatsComputeOptions.__init__(self)

    def get_headers(self):
        return self.requestHandler.request.headers

    def get_request_body(self):
        return self.requestHandler.request.body

    def get_argument(self, name, default=None):
        return self.requestHandler.get_argument(name, default=default)

    def get_list_int_arg(self, name, default=None):
        arg = self.get_argument(name, default=default)
        return arg.split(',')

    def __validate_is_shortname(self, v):
        if v is None or len(v) == 0:
            return False
        return self.shortNamePattern.match(v) is not None

    def __validate_is_number(self, v):
        if v is None or (type(v) == str and len(v) == 0):
            return False
        elif type(v) == int or type(v) == float:
            return True
        else:
            return self.floatingPointPattern.match(v) is not None

    def get_float_arg(self, name, default=0.0):
        arg = self.get_argument(name, default)
        if self.__validate_is_number(arg):
            return float(arg)
        else:
            return default

    def get_decimal_arg(self, name, default=0.0):
        arg = self.get_argument(name, default)
        if self.__validate_is_number(arg):
            return Decimal(arg)
        else:
            if default is None:
                return None
            return Decimal(default)

    def get_int_arg(self, name, default=0):
        arg = self.get_argument(name, default)
        if self.__validate_is_number(arg):
            return int(arg)
        else:
            return default

    def get_boolean_arg(self, name, default=False):
        arg = self.get_argument(name, "false" if not default else "true")
        return arg is not None and arg in ['true', '1', 't', 'y', 'yes', 'True', 'T', 'Y',
                                           'Yes', True]

    def get_datetime_arg(self, name, default=None):
        time_str = self.get_argument(name, default=default)
        if time_str == default:
            return default
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def get_apply_seasonal_cycle_filter(self, default=True):
        return self.get_boolean_arg(RequestParameters.SEASONAL_CYCLE_FILTER, default=default)

    def get_max_lat(self, default=Decimal(90)):
        return self.get_decimal_arg("maxLat", default)

    def get_min_lat(self, default=Decimal(-90)):
        return self.get_decimal_arg("minLat", default)

    def get_max_lon(self, default=Decimal(180)):
        return self.get_decimal_arg("maxLon", default)

    def get_min_lon(self, default=Decimal(-180)):
        return self.get_decimal_arg("minLon", default)

    def get_elevation_args(self) -> Tuple[Union[float, None], Union[float, None]]:
        min_depth = self.get_float_arg('minDepth', None)
        max_depth = self.get_float_arg('maxDepth', None)
        min_height = self.get_float_arg('minHeight', None)
        max_height = self.get_float_arg('maxHeight', None)
        min_elevation = self.get_float_arg('minElevation', None)
        max_elevation = self.get_float_arg('maxElevation', None)

        ret_min = None
        ret_max = None
        using_depth = False

        if min_elevation is not None:
            ret_min = min_elevation
        elif min_height is not None:
            ret_min = min_height
        elif min_depth is not None:
            ret_min = -1 * min_depth
            using_depth = True

        if max_elevation is not None:
            ret_max = max_elevation
        elif max_height is not None:
            ret_max = max_height
        elif max_depth is not None:
            ret_max = -1 * max_depth
            using_depth = True

        if (ret_max is not None and ret_min is not None) and ret_max < ret_min:
            if using_depth:
                ret_max, ret_min = ret_min, ret_max
            else:
                raise ValueError(f'Request max elevation less than min elevation: {ret_max} < {ret_min}')

        return ret_min, ret_max

    # added to fit the simplified version of TimeAvgMapSpark parse_argumemt
    def get_bounding_box(self):

        b = self.get_argument("b", '')
        if b:
            min_lon, min_lat, max_lon, max_lat = [float(e) for e in b.split(",")]
        else:
            max_lat = self.get_argument("maxLat", 90)
            max_lat = Decimal(max_lat) if self.__validate_is_number(max_lat) else 90

            min_lat = self.get_argument("minLat", -90)
            min_lat = Decimal(min_lat) if self.__validate_is_number(min_lat) else -90

            max_lon = self.get_argument("maxLon", 180)
            max_lon = Decimal(max_lon) if self.__validate_is_number(max_lon) else 180

            min_lon = self.get_argument("minLon", -90)
            min_lon = Decimal(min_lon) if self.__validate_is_number(min_lon) else -90

        return min_lon, min_lat, max_lon, max_lat


    def get_bounding_polygon(self):
        west, south, east, north = [float(b) for b in self.get_argument("b").split(",")]
        polygon = Polygon([(west, south), (east, south), (east, north), (west, north), (west, south)])
        return polygon

    def get_dataset(self):
        ds = self.get_argument(RequestParameters.DATASET, None)
        if ds is not None and not self.__validate_is_shortname(ds):
            raise Exception("Invalid shortname")
        else:
            return ds.split(",")

    def get_metadata_filter(self):
        return self.requestHandler.get_arguments(RequestParameters.METADATA_FILTER)

    def get_environment(self):
        env = self.get_argument(RequestParameters.ENVIRONMENT, None)
        if env is None and "Origin" in self.requestHandler.request.headers:
            origin = self.requestHandler.request.headers["Origin"]
            if origin == "http://localhost:63342":
                env = "DEV"
            if origin == "https://sealevel.uat.earthdata.nasa.gov":
                env = "UAT"
            elif origin == "https://sealevel.sit.earthdata.nasa.gov":
                env = "SIT"
            elif origin == "https://sealevel.earthdata.nasa.gov":
                env = "PROD"

        if env not in ("DEV", "SIT", "UAT", "PROD", None):
            raise Exception("Invalid Environment")
        else:
            return env

    def get_start_time(self):
        return self.get_int_arg(RequestParameters.START_TIME, 0)

    def get_end_time(self):
        return self.get_int_arg(RequestParameters.END_TIME, -1)

    def get_start_year(self):
        return self.get_int_arg(RequestParameters.START_YEAR, 0)

    def get_end_year(self):
        return self.get_int_arg(RequestParameters.END_YEAR, -1)

    def get_clim_month(self):
        return self.get_int_arg(RequestParameters.CLIM_MONTH, -1)

    def get_start_datetime(self):
        #self.__log("get start datetime as {}".format(RequestParameters.START_TIME))
        time_str = self.get_argument(RequestParameters.START_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def get_end_datetime(self):
        time_str = self.get_argument(RequestParameters.END_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def get_start_datetime_ms(self):
        time_str = self.get_argument(RequestParameters.START_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str) / 1000).replace(tzinfo=UTC)
        return dt

    def get_end_datetime_ms(self):
        time_str = self.get_argument(RequestParameters.END_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str) / 1000).replace(tzinfo=UTC)
        return dt

    def get_start_row(self):
        return self.get_int_arg(RequestParameters.START_ROW, 0)

    def get_row_count(self):
        return self.get_int_arg(RequestParameters.ROW_COUNT, 10)

    def get_content_type(self):
        return self.get_argument(RequestParameters.OUTPUT, "JSON")

    def get_apply_low_pass_filter(self, default=True):
        return self.get_boolean_arg(RequestParameters.APPLY_LOW_PASS, default)

    def get_low_pass_low_cut(self, default=12):
        return self.get_float_arg(RequestParameters.LOW_CUT, default)

    def get_low_pass_order(self, default=9):
        return self.get_float_arg(RequestParameters.ORDER, default)

    def get_include_meta(self):
        return self.get_boolean_arg("includemeta", True)

    def get_plot_series(self, default="mean"):
        return self.get_argument(RequestParameters.PLOT_SERIES, default=default)

    def get_plot_type(self, default="default"):
        return self.get_argument(RequestParameters.PLOT_TYPE, default=default)

    def get_nparts(self):
        return self.get_int_arg(RequestParameters.NPARTS, 0)

    def get_normalize_dates(self):
        return self.get_boolean_arg(RequestParameters.NORMALIZE_DATES, False)
