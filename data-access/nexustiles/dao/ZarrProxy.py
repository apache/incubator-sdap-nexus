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

# IMPORTS HERE

import boto3
import nexusproto.DataTile_pb2 as nexusproto

import xarray as xr
import fsspec, s3fs
import numpy as np


class ZarrProxy(object):
    def __init__(self, config):
        self.config = config
        self.__s3_bucketname = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

    def fetch_nexus_tiles(self, *tile_ids):
        pass

    #TODO: Determine how Zarr arrays/chunks will be addressed (Tues 6/14 mtg)