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
import os
import csv
import json
from datetime import datetime
import time
import itertools
import importlib_metadata
from collections import defaultdict
from decimal import Decimal

import numpy as np
from pytz import timezone, UTC

from . import config
from . import geo
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

try:
    from osgeo import gdal
    from osgeo.gdalnumeric import *
except ImportError:
    import gdal
    from gdalnumeric import *

from netCDF4 import Dataset
import netCDF4
import tempfile


class BaseDomsQueryCalcHandler(NexusCalcHandler):
    def __init__(self, tile_service_factory, **kwargs):
        NexusCalcHandler.__init__(self, tile_service_factory, **kwargs)

    def getDataSourceByName(self, source):
        for s in config.ENDPOINTS:
            if s["name"] == source:
                return s
        return None

    def _does_datasource_exist(self, ds):
        for endpoint in config.ENDPOINTS:
            if endpoint["name"] == ds:
                return True
        return False


class DomsEncoder(json.JSONEncoder):
    def __init__(self, **args):
        json.JSONEncoder.__init__(self, **args)

    def default(self, obj):
        # print 'MyEncoder.default() called'
        # print type(obj)
        if obj == np.nan:
            return None  # hard code string for now
        elif isinstance(obj, datetime):
            return int((obj - EPOCH).total_seconds())
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, np.float32):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class DomsQueryResults(NexusResults):
    def __init__(self, results=None, args=None, bounds=None, count=None, details=None, computeOptions=None,
                 executionId=None, status_code=200, page_num=None, page_size=None):
        NexusResults.__init__(self, results=results, meta=None, stats=None, computeOptions=computeOptions,
                              status_code=status_code)
        self.__args = args
        self.__bounds = bounds
        self.__count = count
        self.__details = details
        self.__executionId = str(executionId)

        if self.__details is None:
            self.__details = {}

        # Add page num and size to details block
        self.__details['pageNum'] = page_num
        self.__details['pageSize'] = page_size

    def toJson(self):
        bounds = self.__bounds.toMap() if self.__bounds is not None else {}
        return json.dumps(
            {"executionId": self.__executionId, "data": self.results(), "params": self.__args, "bounds": bounds,
             "count": self.__count, "details": self.__details}, indent=4, cls=DomsEncoder)

    def toCSV(self):
        return DomsCSVFormatter.create(self.__executionId, self.results(), self.__args, self.__details)

    def toNetCDF(self):
        return DomsNetCDFFormatter.create(self.__executionId, self.results(), self.__args, self.__details)

    def filename(self):
        return f'CDMS_{self.__executionId}'


class DomsCSVFormatter:
    @staticmethod
    def create(executionId, results, params, details):

        csv_mem_file = io.StringIO()
        try:
            DomsCSVFormatter.__addConstants(csv_mem_file)
            DomsCSVFormatter.__addDynamicAttrs(csv_mem_file, executionId, results, params, details)
            csv.writer(csv_mem_file).writerow([])

            DomsCSVFormatter.__packValues(csv_mem_file, results)

            csv_out = csv_mem_file.getvalue()
        finally:
            csv_mem_file.close()

        return csv_out

    @staticmethod
    def __get_variable_name(variable):
        def is_empty(s):
            return s is None or s == ''

        name = variable['cf_variable_name']

        return name if not is_empty(name) else variable['variable_name']

    @staticmethod
    def __packValues(csv_mem_file, results):
        primary_headers = list(dict.fromkeys(
            key for result in results for key in result if key not in ['matches', 'primary']
        ))

        primary_headers.extend(list(dict.fromkeys(
            DomsCSVFormatter.__get_variable_name(variable) for result in results for variable in result['primary']
        )))

        secondary_headers = list(dict.fromkeys(
            key for result in results for match in result['matches'] for key in match if key != 'secondary'
        ))

        secondary_headers.extend(list(dict.fromkeys(
            DomsCSVFormatter.__get_variable_name(variable) for result in results for match in result['matches'] for variable in match['secondary']
        )))

        writer = csv.writer(csv_mem_file)
        writer.writerow(list(itertools.chain(primary_headers, secondary_headers)))

        for primaryValue in results:
            for matchup in primaryValue["matches"]:
                # Primary
                primary_row = [None for _ in range(len(primary_headers))]
                for key, value in primaryValue.items():
                    if key == 'matches':
                        continue

                    if key != 'primary':
                        index = primary_headers.index(key)
                        primary_row[index] = value
                    else:
                        for variable in value:
                            index = primary_headers.index(DomsCSVFormatter.__get_variable_name(variable))
                            primary_row[index] = variable['variable_value']
                # Secondary
                secondary_row = [None for _ in range(len(secondary_headers))]
                for key, value in matchup.items():
                    if key != 'secondary':
                        index = secondary_headers.index(key)
                        secondary_row[index] = value
                    else:
                        for variable in value:
                            index = secondary_headers.index(DomsCSVFormatter.__get_variable_name(variable))
                            secondary_row[index] = variable['variable_value']
                writer.writerow(list(itertools.chain(primary_row, secondary_row)))

    @staticmethod
    def __addConstants(csvfile):

        version = importlib_metadata.distribution('nexusanalysis').version

        global_attrs = [
            {"Global Attribute": "product_version", "Value": "1.0"},
            {"Global Attribute": "Conventions", "Value": "CF-1.6, ACDD-1.3"},
            {"Global Attribute": "title", "Value": "CDMS satellite-insitu machup output file"},
            {"Global Attribute": "history",
             "Value": f"Processing_Version = V1.0, Software_Name = CDMS, Software_Version = {version}"},
            {"Global Attribute": "institution", "Value": "JPL, FSU, NCAR, Saildrone"},
            {"Global Attribute": "source", "Value": "doms.jpl.nasa.gov"},
            {"Global Attribute": "standard_name_vocabulary",
             "Value": "CF Standard Name Table v27, BODC controlled vocabulary"},
            {"Global Attribute": "cdm_data_type", "Value": "trajectory, station, point, swath, grid"},
            {"Global Attribute": "processing_level", "Value": "4"},
            {"Global Attribute": "project", "Value": "Cloud-based Data Matchup Service (CDMS)"},
            {"Global Attribute": "keywords_vocabulary",
             "Value": "NASA Global Change Master Directory (GCMD) Science Keywords"},
            # TODO What should the keywords be?
            {"Global Attribute": "keywords", "Value": "SATELLITES, OCEAN PLATFORMS, SHIPS, BUOYS, MOORINGS, AUVS, ROV, "
                                                      "NASA/JPL/PODAAC, FSU/COAPS, UCAR/NCAR, SALINITY, "
                                                      "SEA SURFACE TEMPERATURE, SURFACE WINDS"},
            {"Global Attribute": "creator_name", "Value": "Cloud-Based Data Matchup Service (CDMS)"},
            {"Global Attribute": "creator_email", "Value": "cdms@jpl.nasa.gov"},
            {"Global Attribute": "creator_url", "Value": "https://doms.jpl.nasa.gov/"},
            {"Global Attribute": "publisher_name",  "Value": "CDMS"},
            {"Global Attribute": "publisher_email", "Value": "cdms@jpl.nasa.gov"},
            {"Global Attribute": "publisher_url", "Value": "https://doms.jpl.nasa.gov"},
            {"Global Attribute": "acknowledgment", "Value": "CDMS is a NASA/ACCESS funded project with prior support from NASA/AIST"},
        ]

        writer = csv.DictWriter(csvfile, sorted(next(iter(global_attrs)).keys()))

        writer.writerows(global_attrs)

    @staticmethod
    def __addDynamicAttrs(csvfile, executionId, results, params, details):

        platforms = set()
        for primaryValue in results:
            platforms.add(primaryValue['platform'])
            for match in primaryValue['matches']:
                platforms.add(match['platform'])

        insituDatasets = params["matchup"]
        insituLinks = set()
        for insitu in insituDatasets:
            if insitu in config.METADATA_LINKS:
                insituLinks.add(config.METADATA_LINKS[insitu])


        global_attrs = [
            {"Global Attribute": "Platform", "Value": ', '.join(platforms)},
            {"Global Attribute": "time_coverage_start",
             "Value": params["startTime"].strftime(ISO_8601)},
            {"Global Attribute": "time_coverage_end",
             "Value": params["endTime"].strftime(ISO_8601)},

            {"Global Attribute": "geospatial_lon_min", "Value": params["bbox"].split(',')[0]},
            {"Global Attribute": "geospatial_lat_min", "Value": params["bbox"].split(',')[1]},
            {"Global Attribute": "geospatial_lon_max", "Value": params["bbox"].split(',')[2]},
            {"Global Attribute": "geospatial_lat_max", "Value": params["bbox"].split(',')[3]},
            {"Global Attribute": "geospatial_lat_units", "Value": "degrees_north"},
            {"Global Attribute": "geospatial_lon_units", "Value": "degrees_east"},

            {"Global Attribute": "geospatial_vertical_min", "Value": params["depthMin"]},
            {"Global Attribute": "geospatial_vertical_max", "Value": params["depthMax"]},
            {"Global Attribute": "geospatial_vertical_units", "Value": "m"},
            {"Global Attribute": "geospatial_vertical_positive", "Value": "down"},

            {"Global Attribute": "CDMS_matchID", "Value": executionId},
            {"Global Attribute": "CDMS_TimeWindow", "Value": params["timeTolerance"] / 60 / 60},
            {"Global Attribute": "CDMS_TimeWindow_Units", "Value": "hours"},

            {"Global Attribute": "CDMS_platforms", "Value": params["platforms"]},
            {"Global Attribute": "CDMS_SearchRadius", "Value": params["radiusTolerance"]},
            {"Global Attribute": "CDMS_SearchRadius_Units", "Value": "m"},

            {"Global Attribute": "CDMS_DatasetMetadata", "Value": ', '.join(insituLinks)},
            {"Global Attribute": "CDMS_primary", "Value": params["primary"]},
            {"Global Attribute": "CDMS_secondary", "Value": ','.join(params['matchup']) if isinstance(params["matchup"], list) else params['matchup']},
            {"Global Attribute": "CDMS_ParameterPrimary", "Value": params.get("parameter", "")},

            {"Global Attribute": "CDMS_time_to_complete", "Value": details["timeToComplete"]},
            {"Global Attribute": "CDMS_time_to_complete_units", "Value": "seconds"},
            {"Global Attribute": "CDMS_num_secondary_matched", "Value": details["numSecondaryMatched"]},
            {"Global Attribute": "CDMS_num_primary_matched", "Value": details["numPrimaryMatched"]},

            {"Global Attribute": "date_modified", "Value": datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)},
            {"Global Attribute": "date_created", "Value": datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)},

            {"Global Attribute": "URI_Matchup", "Value": "https://doms.jpl.nasa.gov/domsresults?id=" + executionId + "&output=CSV"}, # TODO how to replace with actual req URL

            {"Global Attribute": "CDMS_page_num", "Value": details["pageNum"]},
            {"Global Attribute": "CDMS_page_size", "Value": details["pageSize"]},
        ]

        writer = csv.DictWriter(csvfile, sorted(next(iter(global_attrs)).keys()))

        writer.writerows(global_attrs)


class DomsNetCDFFormatter:
    @staticmethod
    def create(executionId, results, params, details):

        t = tempfile.mkstemp(prefix="cdms_", suffix=".nc")
        tempFileName = t[1]

        dataset = Dataset(tempFileName, "w", format="NETCDF4")
        dataset.CDMS_matchID = executionId
        DomsNetCDFFormatter.__addNetCDFConstants(dataset)

        dataset.date_modified = datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)
        dataset.date_created = datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)
        dataset.time_coverage_start = params["startTime"].strftime(ISO_8601)
        dataset.time_coverage_end = params["endTime"].strftime(ISO_8601)
        dataset.time_coverage_resolution = "point"
        dataset.CDMS_secondary = params["matchup"]
        dataset.CDMS_num_matchup_matched = details["numSecondaryMatched"]
        dataset.CDMS_num_primary_matched = details["numPrimaryMatched"]

        bbox = geo.BoundingBox(asString=params["bbox"])
        dataset.geospatial_lat_max = bbox.north
        dataset.geospatial_lat_min = bbox.south
        dataset.geospatial_lon_max = bbox.east
        dataset.geospatial_lon_min = bbox.west
        dataset.geospatial_lat_units = "degrees_north"
        dataset.geospatial_lon_units = "degrees_east"
        dataset.geospatial_vertical_min = float(params["depthMin"])
        dataset.geospatial_vertical_max = float(params["depthMax"])
        dataset.geospatial_vertical_units = "m"
        dataset.geospatial_vertical_positive = "down"

        dataset.CDMS_TimeWindow = params["timeTolerance"] / 60 / 60
        dataset.CDMS_TimeWindow_Units = "hours"
        dataset.CDMS_SearchRadius = float(params["radiusTolerance"])
        dataset.CDMS_SearchRadius_Units = "m"
        dataset.URI_Matchup = "https://doms.jpl.nasa.gov/domsresults?id=" + executionId + "&output=NETCDF"

        dataset.CDMS_ParameterPrimary = params["parameter"] if ("parameter" in params and params['parameter'] is not None) else ""
        dataset.CDMS_platforms = params["platforms"]
        dataset.CDMS_primary = params["primary"]
        dataset.CDMS_time_to_complete = details["timeToComplete"]
        dataset.CDMS_time_to_complete_units = "seconds"
        dataset.CDMS_page_num = details["pageNum"]
        dataset.CDMS_page_size = details["pageSize"]

        insituDatasets = params["matchup"]
        insituLinks = set()
        for insitu in insituDatasets:
            if insitu in config.METADATA_LINKS:
                insituLinks.add(config.METADATA_LINKS[insitu])
        if insituLinks:
            dataset.CDMS_DatasetMetadata = ', '.join(insituLinks)

        platforms = set()
        for primaryValue in results:
            platforms.add(primaryValue['platform'])
            for match in primaryValue['matches']:
                platforms.add(match['platform'])
        dataset.platform = ', '.join(platforms)

        satellite_group_name = 'PrimaryData'
        insitu_group_name = "SecondaryData"

        #Create Satellite group, variables, and attributes
        satelliteGroup = dataset.createGroup(satellite_group_name)
        satelliteWriter = DomsNetCDFValueWriter(satelliteGroup, params["parameter"])

        # Create InSitu group, variables, and attributes
        insituGroup = dataset.createGroup(insitu_group_name)
        insituWriter = DomsNetCDFValueWriter(insituGroup, params["parameter"])

        # Add data to Insitu and Satellite groups, generate array of match ID pairs
        matches = DomsNetCDFFormatter.__writeResults(results, satelliteWriter, insituWriter)
        dataset.createDimension("MatchedRecords", size=None)
        dataset.createDimension("MatchedGroups", size=2)
        matchArray = dataset.createVariable("matchIDs", "f4", ("MatchedRecords", "MatchedGroups"))
        matchArray[:] = matches

        dataset.close()
        f = open(tempFileName, "rb")
        data = f.read()
        f.close()
        os.unlink(tempFileName)
        return data

    @staticmethod
    def __addNetCDFConstants(dataset):
        dataset.product_version = "1.0"
        dataset.Conventions = "CF-1.8, ACDD-1.3"
        dataset.title = "CDMS satellite-insitu machup output file"
        dataset.history = "Processing_Version = V1.0, Software_Name = CDMS, Software_Version = 1.03"
        dataset.institution = "JPL, FSU, NCAR, Saildrone"
        dataset.source = "doms.jpl.nasa.gov"
        dataset.standard_name_vocabulary = "CF Standard Name Table v27", "BODC controlled vocabulary"
        dataset.cdm_data_type = "Point/Profile, Swath/Grid"
        dataset.processing_level = "4"
        dataset.project = "Cloud-Based Data Matchup Service (CDMS)"
        dataset.keywords_vocabulary = "NASA Global Change Master Directory (GCMD) Science Keywords"
        dataset.keywords = "SATELLITES, OCEAN PLATFORMS, SHIPS, BUOYS, MOORINGS, AUVS, ROV, NASA/JPL/PODAAC, " \
                           "FSU/COAPS, UCAR/NCAR, SALINITY, SEA SURFACE TEMPERATURE, SURFACE WINDS"
        dataset.creator_name = "Cloud-Based Data Matchup Service (CDMS)"
        dataset.creator_email = "cdms@jpl.nasa.gov"
        dataset.creator_url = "https://doms.jpl.nasa.gov/"
        dataset.publisher_name = "Cloud-Based Data Matchup Service (CDMS)"
        dataset.publisher_email = "cdms@jpl.nasa.gov"
        dataset.publisher_url = "https://doms.jpl.nasa.gov"
        dataset.acknowledgment = "CDMS is a NASA/ACCESS funded project with prior support from NASA/AIST"

    @staticmethod
    def __writeResults(results, satelliteWriter, insituWriter):
        ids = {}
        matches = []
        insituIndex = 0

        #
        # Loop through all of the results, add each satellite data point to the array
        #
        for r in range(0, len(results)):
            result = results[r]
            satelliteWriter.addData(result)

            # Add each match only if it is not already in the array of in situ points
            for match in result["matches"]:
                depth_str = ''
                if match['depth'] is not None:
                    depth_str = f'{match["depth"]:.4}'
                key = (match['id'], depth_str)

                if key not in ids:
                    ids[key] = insituIndex
                    insituIndex += 1
                    insituWriter.addData(match)

                # Append an index pait of (satellite, in situ) to the array of matches
                matches.append((r, ids[key]))

        # Add data/write to the netCDF file
        satelliteWriter.writeGroup()
        insituWriter.writeGroup()

        return matches


class DomsNetCDFValueWriter:
    def __init__(self, group, matchup_parameter):
        group.createDimension("dim", size=None)
        self.group = group

        self.lat = []
        self.lon = []
        self.time = []
        self.depth = []

        self.primary_group_name = "PrimaryData"
        self.secondary_group_name = "SecondaryData"
        self.data_map = defaultdict(list)

    def addData(self, result_item):
        """
        Populate DomsNetCDFValueWriter fields from matchup results dict
        """
        non_data_fields = [
            'id', 'lon', 'lat',
            'source', 'device',
            'platform', 'time', 'matches',
            'point', 'fileurl'
        ]
        self.lat.append(result_item.get('lat', None))
        self.lon.append(result_item.get('lon', None))
        self.time.append(time.mktime(result_item.get('time').timetuple()))

        # All other variables are assumed to be science variables.
        # Add DataPoints accordingly.
        for key, value in result_item.items():
            if 'depth' in key:
                self.depth.append(result_item.get(key))
                continue
            if key not in non_data_fields:
                if len(self.data_map[key]) != len(self.lat) - 1:
                    # If the counts mismatch, fill this variable with
                    # None so the data matches the size
                    size_diff = len(self.lat) - len(self.data_map[key]) - 1
                    self.data_map[key].extend([None] * size_diff)
                self.data_map[key].append(value)

        # Check if there are any variables that were not appended to.
        # Append None, meaning that value is empty.
        for var_name in set(self.data_map.keys()) - set(result_item.keys()):
            self.data_map[var_name].append(None)

    def writeGroup(self):
        #
        # Create variables, enrich with attributes, and add data
        #
        lonVar = self.group.createVariable('lon', 'f4', ('dim',), fill_value=-32767.0)
        latVar = self.group.createVariable('lat', 'f4', ('dim',), fill_value=-32767.0)
        timeVar = self.group.createVariable('time', 'f4', ('dim',), fill_value=-32767.0)

        self.__enrichLon(lonVar, min(self.lon), max(self.lon))
        self.__enrichLat(latVar, min(self.lat), max(self.lat))
        self.__enrichTime(timeVar)

        latVar[:] = self.lat
        lonVar[:] = self.lon
        timeVar[:] = self.time

        # Add depth variable, if present
        if self.depth and any(self.depth):
            depthVar = self.group.createVariable('depth', 'f4', ('dim',), fill_value=-32767.0)
            self.__enrichDepth(depthVar, self.__calcMin(self.depth), max(self.depth))
            depthVar[:] = self.depth

        for variable_name, data in self.data_map.items():
            units = {}

            variables = dict.fromkeys(
                ((variable['variable_name'], variable['cf_variable_name']) for match in data for variable in match),
                None
            )

            for variable in variables:
                variables[variable] = np.repeat(np.nan, len(data))

            for i, match in enumerate(data):
                for variable in match:
                    key = (variable['variable_name'], variable['cf_variable_name'])
                    unit = variable['variable_unit']
                    units[key] = str(unit) if unit is not None else 'UNKNOWN'
                    variables[key][i] = variable['variable_value']

            for variable in variables:
                # Create a variable for each data point
                name = variable[0]
                cf_name = variable[1]

                data_variable = self.group.createVariable(
                    cf_name if cf_name is not None and cf_name != '' else name, 'f4', ('dim',), fill_value=-32767.0)
                # Find min/max for data variables. It is possible for 'None' to
                # be in this list, so filter those out when doing the calculation.
                min_data = np.nanmin(variables[variable])
                max_data = np.nanmax(variables[variable])
                self.__enrichVariable(data_variable, min_data, max_data, has_depth=None, unit=units[variable])
                data_variable[:] = np.ma.masked_invalid(variables[variable])
                data_variable.long_name = name
                data_variable.standard_name = cf_name

    #
    # Lists may include 'None" values, to calc min these must be filtered out
    #
    @staticmethod
    def __calcMin(var):
        return min(x for x in var if x is not None)

    @staticmethod
    def __enrichVariable(var, var_min, var_max, has_depth, unit='UNKNOWN'):
        coordinates = ['lat', 'lon', 'depth', 'time']

        if not has_depth:
            coordinates = ['lat', 'lon', 'time']

        var.units = unit
        var.valid_min = var_min
        var.valid_max = var_max
        var.coordinates = ' '.join(coordinates)

    #
    # Add attributes to each variable
    #
    @staticmethod
    def __enrichLon(var, var_min, var_max):
        var.long_name = "Longitude"
        var.standard_name = "longitude"
        var.axis = "X"
        var.units = "degrees_east"
        var.valid_min = var_min
        var.valid_max = var_max

    @staticmethod
    def __enrichLat(var, var_min, var_max):
        var.long_name = "Latitude"
        var.standard_name = "latitude"
        var.axis = "Y"
        var.units = "degrees_north"
        var.valid_min = var_min
        var.valid_max = var_max

    @staticmethod
    def __enrichTime(var):
        var.long_name = "Time"
        var.standard_name = "time"
        var.axis = "T"
        var.units = "seconds since 1970-01-01 00:00:00 0:00"

    @staticmethod
    def __enrichDepth(var, var_min, var_max):
        var.valid_min = var_min
        var.valid_max = var_max
        var.units = "m"
        var.long_name = "Depth"
        var.standard_name = "depth"
        var.axis = "Z"
        var.positive = "Down"

