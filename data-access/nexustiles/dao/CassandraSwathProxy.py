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
import uuid
from configparser import NoOptionError

import nexusproto.DataTile_pb2 as nexusproto
import numpy as np
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns, connection, CQLEngineException
from cassandra.cluster import NoHostAvailable
from cassandra.cqlengine.models import Model
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy
from multiprocessing.synchronize import Lock
from nexusproto.serialization import from_shaped_array

INIT_LOCK = Lock(ctx=None)

logger = logging.getLogger(__name__)


class NexusTileData(Model):
    __table_name__ = 'sea_surface_temp'
    tile_id = columns.UUID(primary_key=True)
    tile_blob = columns.Blob()

    __nexus_tile = None

    def _get_nexus_tile(self):
        if self.__nexus_tile is None:
            self.__nexus_tile = nexusproto.TileData.FromString(self.tile_blob)

        return self.__nexus_tile

    def get_raw_data_array(self):

        nexus_tile = self._get_nexus_tile()
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        return from_shaped_array(the_tile_data.variable_data)

    def get_lat_lon_time_data_meta(self, projection='grid'):
        """
        Retrieve data from data store and metadata from metadata store
        for this tile. For swath tiles, the tile shape of the data
        will match the input shape. For example, if the input was a
        30x30 tile, all variables will also be 30x30. However, if the
        tile is a gridded tile, the lat and lon arrays will be reflected
        into 2 dimensions to reflect how the equivalent data would be
        represented in swath format.

        Multi-variable tile will also include an extra dimension in the
        data array. For example, a 30 x 30 x 30 array would be
        transformed to N x 30 x 30 x 30 where N is the number of
         variables in this tile.

        latitude_data, longitude_data, np.array([grid_tile.time]), grid_tile_data, meta_data, is_multi_var

        :return: latitude data
        :return: longitude data
        :return: time data
        :return: data
        :return: meta data dictionary
        :return: boolean flag, True if this tile has more than one variable
        """
        is_multi_var = False

        if self._get_nexus_tile().HasField('grid_tile'):
            grid_tile = self._get_nexus_tile().grid_tile

            grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_tile.variable_data))
            latitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.longitude))

            reflected_lon_array = np.broadcast_to(longitude_data, (len(latitude_data), len(longitude_data)))
            reflected_lat_array = np.broadcast_to(latitude_data, (len(longitude_data), len(latitude_data)))
            reflected_lat_array = np.transpose(reflected_lat_array)

            time_array = np.broadcast_to(grid_tile.time, grid_tile_data.shape)

            # if len(grid_tile_data.shape) == 2:
            #     grid_tile_data = grid_tile_data[np.newaxis, :]

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in grid_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                if len(meta_array.shape) == 2:
                    meta_array = meta_array[np.newaxis, :]
                meta_data[name] = meta_array

            return reflected_lat_array, reflected_lon_array, time_array, grid_tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('swath_tile'):
            swath_tile = self._get_nexus_tile().swath_tile

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude))
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time))

            # Simplify the tile if the time dimension is the same value repeated
            # if np.all(time_data == np.min(time_data)):
            #     time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))
            tile_data = swath_tile_data

            # Extract the metadata
            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                meta_data[name] = actual_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        # TODO: Do we use this?
        # elif self._get_nexus_tile().HasField('time_series_tile'):
        #     time_series_tile = self._get_nexus_tile().time_series_tile
        #
        #     time_series_tile_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.variable_data))
        #     time_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.time)).reshape(-1)
        #     latitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.latitude))
        #     longitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.longitude))
        #
        #     reshaped_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
        #     idx = np.arange(len(latitude_data))
        #     reshaped_array[:, idx, idx] = time_series_tile_data
        #     tile_data = reshaped_array
        #     # Extract the meta data
        #     meta_data = {}
        #     for meta_data_obj in time_series_tile.meta_data:
        #         name = meta_data_obj.name
        #         meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
        #
        #         reshaped_meta_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
        #         idx = np.arange(len(latitude_data))
        #         reshaped_meta_array[:, idx, idx] = meta_array
        #
        #         meta_data[name] = reshaped_meta_array
        #
        #     return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('swath_multi_variable_tile'):
            swath_tile = self._get_nexus_tile().swath_multi_variable_tile
            is_multi_var = True

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude))
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time))

            # Simplify the tile if the time dimension is the same value repeated
            # if np.all(time_data == np.min(time_data)):
            #     time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))
            swath_tile_data = np.moveaxis(swath_tile_data, -1, 0)

            tile_data = []

            for variable_array in swath_tile_data:
                tile_data.append(variable_array)

            # Extract the metadata

            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                meta_data[name] = actual_meta_array

            return latitude_data, longitude_data, time_data, np.ma.array(tile_data), meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('grid_multi_variable_tile'):
            grid_multi_variable_tile = self._get_nexus_tile().grid_multi_variable_tile
            is_multi_var = True

            grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.variable_data))
            latitude_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.longitude))

            reflected_lon_array = np.broadcast_to(longitude_data, (len(latitude_data), len(longitude_data)))
            reflected_lat_array = np.broadcast_to(latitude_data, (len(longitude_data), len(latitude_data)))
            reflected_lat_array = np.transpose(reflected_lat_array)

            reflected_lat_array = np.expand_dims(reflected_lat_array, axis=0)
            reflected_lon_array = np.expand_dims(reflected_lon_array, axis=0)

            time = np.array([grid_multi_variable_tile.time])
            reflected_time_array = np.broadcast_to(time, (len(latitude_data), len(longitude_data)))

            reflected_time_array = np.expand_dims(reflected_time_array, axis=0)

            # If there are 3 dimensions, that means the time dimension
            # was squeezed. Add back in
            if len(grid_tile_data.shape) == 3:
                grid_tile_data = np.expand_dims(grid_tile_data, axis=0)
            # If there are 4 dimensions, that means the time dimension
            # is present. Move the multivar dimension.
            if len(grid_tile_data.shape) == 4:
                grid_tile_data = np.moveaxis(grid_tile_data, -1, 0)

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in grid_multi_variable_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                if len(meta_array.shape) == 2:
                    meta_array = meta_array[np.newaxis, :]
                meta_data[name] = meta_array

            return reflected_lat_array, reflected_lon_array, reflected_time_array, grid_tile_data, meta_data, is_multi_var
        else:
            raise NotImplementedError("Only supports grid_tile, swath_tile, swath_multi_variable_tile, and time_series_tile")


class CassandraSwathProxy(object):
    def __init__(self, config):
        self.config = config
        self.__cass_url = config.get("cassandra", "host")
        self.__cass_username = config.get("cassandra", "username")
        self.__cass_password = config.get("cassandra", "password")
        self.__cass_keyspace = config.get("cassandra", "keyspace")
        self.__cass_local_DC = config.get("cassandra", "local_datacenter")
        self.__cass_protocol_version = config.getint("cassandra", "protocol_version")
        self.__cass_dc_policy = config.get("cassandra", "dc_policy")

        try:
            self.__cass_port = config.getint("cassandra", "port")
        except NoOptionError:
            self.__cass_port = 9042

        with INIT_LOCK:
            try:
                connection.get_cluster()
            except CQLEngineException:
                self.__open()

    def __open(self):
        if self.__cass_dc_policy == 'DCAwareRoundRobinPolicy':
            dc_policy = DCAwareRoundRobinPolicy(self.__cass_local_DC)
            token_policy = TokenAwarePolicy(dc_policy)
        elif self.__cass_dc_policy == 'WhiteListRoundRobinPolicy':
            token_policy = WhiteListRoundRobinPolicy([self.__cass_url])

        if self.__cass_username and self.__cass_password:
            auth_provider = PlainTextAuthProvider(username=self.__cass_username, password=self.__cass_password)
        else:
            auth_provider = None
        try:
            connection.setup(
                [host for host in self.__cass_url.split(',')], self.__cass_keyspace,
                protocol_version=self.__cass_protocol_version, load_balancing_policy=token_policy,
                port=self.__cass_port,
                auth_provider=auth_provider
            )
        except NoHostAvailable as e:
            logger.error("Cassandra is not accessible, SDAP will not server local datasets", e)

    def fetch_nexus_tiles(self, *tile_ids):
        tile_ids = [uuid.UUID(str(tile_id)) for tile_id in tile_ids if
                    (isinstance(tile_id, str) or isinstance(tile_id, str))]

        res = []
        for tile_id in tile_ids:
            filterResults = NexusTileData.objects.filter(tile_id=tile_id)
            if len(filterResults) > 0:
                res.append(filterResults[0])

        return res
