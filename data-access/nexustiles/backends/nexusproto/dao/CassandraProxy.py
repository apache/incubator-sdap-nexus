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

    def get_lat_lon_time_data_meta(self):
        """
        Retrieve data from data store and metadata from metadata store
        for this tile. For gridded tiles, the tile shape of the data
        will match the input shape. For example, if the input was a
        30x30 tile, all variables will also be 30x30. However, if the
        tile is a swath tile, the data will be transformed along the
        diagonal of the data matrix. For example, a 30x30 tile would
        become 900x900 where the 900 points are along the diagonal.

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

            if len(grid_tile_data.shape) == 2:
                grid_tile_data = grid_tile_data[np.newaxis, :]

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in grid_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                if len(meta_array.shape) == 2:
                    meta_array = meta_array[np.newaxis, :]
                meta_data[name] = meta_array

            return latitude_data, longitude_data, np.array([grid_tile.time]), grid_tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('swath_tile'):
            swath_tile = self._get_nexus_tile().swath_tile

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude)).reshape(-1)
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude)).reshape(-1)
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time)).reshape(-1)

            # Simplify the tile if the time dimension is the same value repeated
            if np.all(time_data == np.min(time_data)):
                time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))

            tile_data = self._to_standard_index(swath_tile_data,
                                                (len(time_data), len(latitude_data), len(longitude_data)))

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                reshaped_meta_array = self._to_standard_index(actual_meta_array, tile_data.shape)
                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('time_series_tile'):
            time_series_tile = self._get_nexus_tile().time_series_tile

            time_series_tile_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.variable_data))
            time_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.time)).reshape(-1)
            latitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.longitude))

            reshaped_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
            idx = np.arange(len(latitude_data))
            reshaped_array[:, idx, idx] = time_series_tile_data
            tile_data = reshaped_array
            # Extract the meta data
            meta_data = {}
            for meta_data_obj in time_series_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))

                reshaped_meta_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
                idx = np.arange(len(latitude_data))
                reshaped_meta_array[:, idx, idx] = meta_array

                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('swath_multi_variable_tile'):
            swath_tile = self._get_nexus_tile().swath_multi_variable_tile
            is_multi_var = True

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude)).reshape(-1)
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude)).reshape(-1)
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time)).reshape(-1)

            # Simplify the tile if the time dimension is the same value repeated
            if np.all(time_data == np.min(time_data)):
                time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))

            desired_shape = (
                len(time_data),
                len(latitude_data),
                len(longitude_data),
            )
            tile_data = self._to_standard_index(swath_tile_data, desired_shape, is_multi_var=True)

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                reshaped_meta_array = self._to_standard_index(actual_meta_array, tile_data.shape)
                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('grid_multi_variable_tile'):
            grid_multi_variable_tile = self._get_nexus_tile().grid_multi_variable_tile
            is_multi_var = True

            grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.variable_data))
            latitude_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.longitude))

            # If there are 3 dimensions, that means the time dimension
            # was squeezed. Add back in
            if len(grid_tile_data.shape) == 3:
                grid_tile_data = np.expand_dims(grid_tile_data, axis=1)
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

            return latitude_data, longitude_data, np.array([grid_multi_variable_tile.time]), grid_tile_data, meta_data, is_multi_var
        else:
            raise NotImplementedError("Only supports grid_tile, swath_tile, swath_multi_variable_tile, and time_series_tile")

    def get_elevation_array(self):
        tile_type = self._get_nexus_tile().WhichOneof("tile_type")
        tile_data = getattr(self._get_nexus_tile(), tile_type)

        if not tile_data.HasField('elevation'):
            return None

        elevation_data = np.ma.masked_invalid(from_shaped_array(tile_data.elevation))

        if tile_type in ['swath_tile', 'swath_multi_variable_tile']:
            latitude_data = np.ma.masked_invalid(from_shaped_array(tile_data.latitude)).reshape(-1)
            longitude_data = np.ma.masked_invalid(from_shaped_array(tile_data.longitude)).reshape(-1)
            time_data = np.ma.masked_invalid(from_shaped_array(tile_data.time)).reshape(-1)

            if np.all(time_data == np.min(time_data)):
                time_data = np.array([np.min(time_data)])

            desired_shape = (
                len(time_data),
                len(latitude_data),
                len(longitude_data),
            )

            elevation_data = self._to_standard_index(
                elevation_data,
                desired_shape,
                tile_type == 'swath_multi_variable_tile'
            )

        return elevation_data


    @staticmethod
    def _to_standard_index(data_array, desired_shape, is_multi_var=False):
        """
        Transform swath data to a standard format where data runs along
        diagonal of ND matrix and the non-diagonal data points are
        masked

        :param data_array: The data array to be transformed
        :param desired_shape: The desired shape of the resulting array
        :param is_multi_var: True if this is a multi-variable tile
        :type data_array: np.array
        :type desired_shape: tuple
        :type is_multi_var: bool
        :return: Reshaped array
        :rtype: np.array
        """

        reshaped_array = []
        if is_multi_var:
            reshaped_data_array = np.moveaxis(data_array, -1, 0)
        else:
            reshaped_data_array = [data_array]

        for variable_data_array in reshaped_data_array:
            if desired_shape[0] == 1:
                variable_reshaped_array = np.ma.masked_all((desired_shape[1], desired_shape[2]))
            else:
                variable_reshaped_array = np.ma.masked_all(desired_shape)

            row, col = np.indices(variable_data_array.shape)

            variable_reshaped_array[
                np.diag_indices(desired_shape[1], len(variable_reshaped_array.shape))] = \
                variable_data_array[
                    row.flat, col.flat]
            variable_reshaped_array.mask[
                np.diag_indices(desired_shape[1], len(variable_reshaped_array.shape))] = \
                variable_data_array.mask[
                    row.flat, col.flat]

            if desired_shape[0] == 1:
                reshaped_array.append(variable_reshaped_array[np.newaxis, :])
            else:
                reshaped_array.append(variable_reshaped_array)

        if not is_multi_var:
            # If single var, squeeze extra dim out of array
            reshaped_array = reshaped_array[0]

        return reshaped_array


class CassandraProxy(object):
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
