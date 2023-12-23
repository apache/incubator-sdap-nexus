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

import time
import types

from nexustiles.nexustiles import NexusTileService


class NexusCalcHandler(object):
    @classmethod
    def validate(cls):
        if "calc" not in cls.__dict__ or not type(cls.__dict__["calc"]) == types.FunctionType:
            raise Exception("Method 'calc' has not been declared")

        if "path" not in cls.__dict__:
            raise Exception("Property 'path' has not been defined")

        if "name" not in cls.__dict__:
            raise Exception("Property 'name' has not been defined")

        if "description" not in cls.__dict__:
            raise Exception("Property 'description' has not been defined")

        if "params" not in cls.__dict__:
            raise Exception("Property 'params' has not been defined")

    def __init__(self, tile_service_factory, **kwargs):
        self._tile_service_factory = tile_service_factory
        if 'desired_projection' in kwargs:
            self._tile_service = tile_service_factory(desired_projection=kwargs['desired_projection'])
        else:
            self._tile_service = tile_service_factory()

    def _get_tile_service(self):
        return self._tile_service

    def calc(self, computeOptions, **args):
        raise Exception("calc() not yet implemented")

    def _mergeDicts(self, x, y):
        z = x.copy()
        z.update(y)
        return z

    def _now(self):
        millis = int(round(time.time() * 1000))
        return millis

    def _mergeDataSeries(self, resultsData, dataNum, resultsMap):

        for entry in resultsData:

            # frmtdTime = datetime.fromtimestamp(entry["time"] ).strftime("%Y-%m")
            frmtdTime = entry["time"]

            if not frmtdTime in resultsMap:
                resultsMap[frmtdTime] = []
            entry["ds"] = dataNum
            resultsMap[frmtdTime].append(entry)

    def _resultsMapToList(self, resultsMap):
        resultsList = []
        for key, value in resultsMap.items():
            resultsList.append(value)

        resultsList = sorted(resultsList, key=lambda entry: entry[0]["time"])
        return resultsList

    def _mergeResults(self, resultsRaw):
        resultsMap = {}

        for i in range(0, len(resultsRaw)):
            resultsSeries = resultsRaw[i]
            resultsData = resultsSeries[0]
            self._mergeDataSeries(resultsData, i, resultsMap)

        resultsList = self._resultsMapToList(resultsMap)
        return resultsList
