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

import configparser
import json
import logging
import sys
from datetime import datetime
from functools import wraps, reduce

import numpy as np
import numpy.ma as ma
import pkg_resources
from pytz import timezone, UTC
from shapely.geometry import MultiPolygon, box

from .dao import CassandraProxy
from .dao import CassandraSwathProxy
from .dao import DynamoProxy
from .dao import ElasticsearchProxy
from .dao import S3Proxy
from .dao import SolrProxy
from .model.nexusmodel import Tile, BBox, TileStats, TileVariable

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger(__name__)


def tile_data(default_fetch=True):
    def tile_data_decorator(func):
        @wraps(func)
        def fetch_data_for_func(*args, **kwargs):
            metadatastore_start = datetime.now()
            metadatastore_docs = func(*args, **kwargs)
            metadatastore_duration = (datetime.now() - metadatastore_start).total_seconds()
            tiles = args[0]._metadata_store_docs_to_tiles(*metadatastore_docs)

            cassandra_duration = 0
            if ('fetch_data' in kwargs and kwargs['fetch_data']) or ('fetch_data' not in kwargs and default_fetch):
                if len(tiles) > 0:
                    cassandra_start = datetime.now()
                    args[0].fetch_data_for_tiles(*tiles, desired_projection=args[0].desired_projection)
                    cassandra_duration += (datetime.now() - cassandra_start).total_seconds()

            if 'metrics_callback' in kwargs and kwargs['metrics_callback'] is not None:
                try:
                    kwargs['metrics_callback'](cassandra=cassandra_duration,
                                               metadatastore=metadatastore_duration,
                                               num_tiles=len(tiles))
                except Exception as e:
                    logger.error("Metrics callback '{}'raised an exception. Will continue anyway. " +
                                 "The exception was: {}".format(kwargs['metrics_callback'], e))
            return tiles

        return fetch_data_for_func

    return tile_data_decorator


class NexusTileServiceException(Exception):
    pass


class NexusTileService(object):
    def __init__(self, skipDatastore=False, skipMetadatastore=False, config=None, desired_projection='grid'):
        self._datastore = None
        self._metadatastore = None

        self._config = configparser.RawConfigParser()
        self._config.read(NexusTileService._get_config_files('config/datastores.ini'))

        if desired_projection not in ['grid', 'swath']:
            raise ValueError(f'Invalid value provided for NexusTileService desired_projection: {desired_projection}')

        self.desired_projection = desired_projection

        if config:
            self.override_config(config)

        if not skipDatastore:
            datastore = self._config.get("datastore", "store")
            if datastore == "cassandra":
                if desired_projection == "grid":
                    self._datastore = CassandraProxy.CassandraProxy(self._config)
                else:
                    self._datastore = CassandraSwathProxy.CassandraSwathProxy(self._config)
            elif datastore == "s3":
                self._datastore = S3Proxy.S3Proxy(self._config)
            elif datastore == "dynamo":
                self._datastore = DynamoProxy.DynamoProxy(self._config)
            else:
                raise ValueError("Error reading datastore from config file")

        if not skipMetadatastore:
            metadatastore = self._config.get("metadatastore", "store", fallback='solr')
            if metadatastore == "solr":
                self._metadatastore = SolrProxy.SolrProxy(self._config)
            elif metadatastore == "elasticsearch":
                self._metadatastore = ElasticsearchProxy.ElasticsearchProxy(self._config)

        logger.info(f'Created new NexusTileService with data store {type(self._datastore)} and metadata '
                    f'store {type(self._metadatastore)}. Desired projection: {desired_projection}')

    def override_config(self, config):
        for section in config.sections():
            if self._config.has_section(section):  # only override preexisting section, ignores the other
                for option in config.options(section):
                    if config.get(section, option) is not None:
                        self._config.set(section, option, config.get(section, option))

    def get_dataseries_list(self, simple=False):
        if simple:
            return self._metadatastore.get_data_series_list_simple()
        else:
            return self._metadatastore.get_data_series_list()

    @tile_data()
    def find_tile_by_id(self, tile_id, **kwargs):
        return self._metadatastore.find_tile_by_id(tile_id)

    @tile_data()
    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        return self._metadatastore.find_tiles_by_id(tile_ids, ds=ds, **kwargs)

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        start = datetime.now()
        result = self._metadatastore.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, dataset, start_time,
                                                            end_time,
                                                            **kwargs)
        duration = (datetime.now() - start).total_seconds()
        if metrics_callback:
            metrics_callback(solr=duration)
        return result

    @tile_data()
    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year, **kwargs):
        """
        Given a bounding polygon, dataset, and day of year, find tiles in that dataset with the same bounding
        polygon and the closest day of year.

        For example:
            given a polygon minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; and day of year=32
            search for first tile in MY_DS with identical bbox and day_of_year <= 32 (sorted by day_of_year desc)

        Valid matches:
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 32
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 30

        Invalid matches:
            minx=1, miny=0, maxx=2, maxy=1; dataset=MY_DS; day of year = 32
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_OTHER_DS; day of year = 32
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 30 if minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 32 also exists

        :param bounding_polygon: The exact bounding polygon of tiles to search for
        :param ds: The dataset name being searched
        :param day_of_year: Tile day of year to search for, tile nearest to this day (without going over) will be returned
        :return: List of one tile from ds with bounding_polygon on or before day_of_year or raise NexusTileServiceException if no tile found
        """
        try:
            tile = self._metadatastore.find_tile_by_polygon_and_most_recent_day_of_year(bounding_polygon, ds,
                                                                                        day_of_year)
        except IndexError:
            raise NexusTileServiceException("No tile found.").with_traceback(sys.exc_info()[2])

        return tile

    @tile_data()
    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return self._metadatastore.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time,
                                                                 rows=5000,
                                                                 **kwargs)

    @tile_data()
    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        return self._metadatastore.find_all_tiles_in_polygon_at_time(bounding_polygon, dataset, time, rows=5000,
                                                                     **kwargs)

    @tile_data()
    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall in the given box in the Solr index
        if type(start_time) is datetime:
            start_time = (start_time - EPOCH).total_seconds()
        if type(end_time) is datetime:
            end_time = (end_time - EPOCH).total_seconds()
        return self._metadatastore.find_all_tiles_in_box_sorttimeasc(min_lat, max_lat, min_lon, max_lon, ds, start_time,
                                                                     end_time, **kwargs)

    @tile_data()
    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall within the polygon in the Solr index
        if 'sort' in list(kwargs.keys()):
            tiles = self._metadatastore.find_all_tiles_in_polygon(bounding_polygon, ds, start_time, end_time, **kwargs)
        else:
            tiles = self._metadatastore.find_all_tiles_in_polygon_sorttimeasc(bounding_polygon, ds, start_time,
                                                                              end_time,
                                                                              **kwargs)
        return tiles

    @tile_data()
    def find_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        """
        Return list of tiles whose metadata matches the specified metadata, start_time, end_time.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tiles
        """
        tiles = self._metadatastore.find_all_tiles_by_metadata(metadata, ds, start_time, end_time, **kwargs)

        return tiles

    def get_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        """
        Return list of tiles that matches the specified metadata, start_time, end_time with tile data outside of time
        range properly masked out.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tiles
        """
        tiles = self.find_tiles_by_metadata(metadata, ds, start_time, end_time, **kwargs)
        tiles = self.mask_tiles_to_time_range(start_time, end_time, tiles)

        return tiles

    @tile_data()
    def find_tiles_by_exact_bounds(self, bounds, ds, start_time, end_time, **kwargs):
        """
        The method will return tiles with the exact given bounds within the time range. It differs from
        find_tiles_in_polygon in that only tiles with exactly the given bounds will be returned as opposed to
        doing a polygon intersection with the given bounds.

        :param bounds: (minx, miny, maxx, maxy) bounds to search for
        :param ds: Dataset name to search
        :param start_time: Start time to search (seconds since epoch)
        :param end_time: End time to search (seconds since epoch)
        :param kwargs: fetch_data: True/False = whether or not to retrieve tile data
        :return:
        """
        tiles = self._metadatastore.find_tiles_by_exact_bounds(bounds[0], bounds[1], bounds[2], bounds[3], ds,
                                                               start_time,
                                                               end_time)
        return tiles

    @tile_data()
    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return self._metadatastore.find_all_boundary_tiles_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time,
                                                                   rows=5000,
                                                                   **kwargs)

    def get_tiles_bounded_by_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1,
                                 **kwargs):
        tiles = self.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs)
        tiles = self.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, tiles)
        if 0 <= start_time <= end_time:
            tiles = self.mask_tiles_to_time_range(start_time, end_time, tiles)

        return tiles

    def get_tiles_bounded_by_polygon(self, polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        tiles = self.find_tiles_in_polygon(polygon, ds, start_time, end_time,
                                           **kwargs)
        tiles = self.mask_tiles_to_polygon(polygon, tiles)
        if 0 <= start_time <= end_time:
            tiles = self.mask_tiles_to_time_range(start_time, end_time, tiles)

        return tiles

    def get_min_max_time_by_granule(self, ds, granule_name):
        start_time, end_time = self._metadatastore.find_min_max_date_from_granule(ds, granule_name)

        return start_time, end_time

    def get_dataset_overall_stats(self, ds):
        return self._metadatastore.get_data_series_stats(ds)

    def get_tiles_bounded_by_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        tiles = self.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs)
        tiles = self.mask_tiles_to_bbox_and_time(min_lat, max_lat, min_lon, max_lon, time, time, tiles)

        return tiles

    def get_tiles_bounded_by_polygon_at_time(self, polygon, dataset, time, **kwargs):
        tiles = self.find_all_tiles_in_polygon_at_time(polygon, dataset, time, **kwargs)
        tiles = self.mask_tiles_to_polygon_and_time(polygon, time, time, tiles)

        return tiles

    def get_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        tiles = self.find_all_boundary_tiles_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs)
        tiles = self.mask_tiles_to_bbox_and_time(min_lat, max_lat, min_lon, max_lon, time, time, tiles)

        return tiles

    def get_stats_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        tiles = self._metadatastore.find_all_tiles_within_box_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time,
                                                                      **kwargs)

        return tiles

    def get_bounding_box(self, tile_ids):
        """
        Retrieve a bounding box that encompasses all of the tiles represented by the given tile ids.
        :param tile_ids: List of tile ids
        :return: shapely.geometry.Polygon that represents the smallest bounding box that encompasses all of the tiles
        """
        tiles = self.find_tiles_by_id(tile_ids, fl=['tile_min_lat', 'tile_max_lat', 'tile_min_lon', 'tile_max_lon'],
                                      fetch_data=False, rows=len(tile_ids))
        polys = []
        for tile in tiles:
            polys.append(box(tile.bbox.min_lon, tile.bbox.min_lat, tile.bbox.max_lon, tile.bbox.max_lat))
        return box(*MultiPolygon(polys).bounds)

    def get_min_time(self, tile_ids, ds=None):
        """
        Get the minimum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        min_time = self._metadatastore.find_min_date_from_tiles(tile_ids, ds=ds)
        return int((min_time - EPOCH).total_seconds())

    def get_max_time(self, tile_ids, ds=None):
        """
        Get the maximum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        max_time = self._metadatastore.find_max_date_from_tiles(tile_ids, ds=ds)
        return int((max_time - EPOCH).total_seconds())

    def get_distinct_bounding_boxes_in_polygon(self, bounding_polygon, ds, start_time, end_time):
        """
        Get a list of distinct tile bounding boxes from all tiles within the given polygon and time range.
        :param bounding_polygon: The bounding polygon of tiles to search for
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of distinct bounding boxes (as shapely polygons) for tiles in the search polygon
        """
        bounds = self._metadatastore.find_distinct_bounding_boxes_in_polygon(bounding_polygon, ds, start_time, end_time)
        return [box(*b) for b in bounds]

    def _data_mask_logical_or(self, tile):
        # Or together the masks of the individual arrays to create the new mask
        if self.desired_projection == 'grid':
            data_mask = ma.getmaskarray(tile.times)[:, np.newaxis, np.newaxis] \
                        | ma.getmaskarray(tile.latitudes)[np.newaxis, :, np.newaxis] \
                        | ma.getmaskarray(tile.longitudes)[np.newaxis, np.newaxis, :]
        else:
            if len(tile.times.shape) == 1:
                data_mask = ma.getmaskarray(tile.times)[:, np.newaxis, np.newaxis] \
                            | ma.getmaskarray(tile.latitudes)[np.newaxis, :, :] \
                            | ma.getmaskarray(tile.longitudes)[np.newaxis, :, :]
            else:
                data_mask = ma.getmaskarray(tile.times) \
                            | ma.getmaskarray(tile.latitudes) \
                            | ma.getmaskarray(tile.longitudes)

        return data_mask

    def mask_tiles_to_bbox(self, min_lat, max_lat, min_lon, max_lon, tiles):

        for tile in tiles:
            tile.latitudes = ma.masked_outside(tile.latitudes, min_lat, max_lat)
            tile.longitudes = ma.masked_outside(tile.longitudes, min_lon, max_lon)

            data_mask = self._data_mask_logical_or(tile)

            # If this is multi-var, need to mask each variable separately.
            if tile.is_multi:
                # Combine space/time mask with existing mask on data
                # Data masks are ANDed because we want to mask out only when ALL data vars are invalid
                combined_data_mask = reduce(np.logical_and, [ma.getmaskarray(d) for d in tile.data])
                # We now OR in the bounds mask because out of bounds data must be excluded regardless of validity
                data_mask = np.logical_or(combined_data_mask, data_mask)

                num_vars = len(tile.data)
                multi_data_mask = np.repeat(data_mask[np.newaxis, ...], num_vars, axis=0)
                tile.data = ma.masked_where(multi_data_mask, tile.data)
            else:
                tile.data = ma.masked_where(data_mask, tile.data)

        tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

    def mask_tiles_to_bbox_and_time(self, min_lat, max_lat, min_lon, max_lon, start_time, end_time, tiles):
        for tile in tiles:
            tile.times = ma.masked_outside(tile.times, start_time, end_time)
            tile.latitudes = ma.masked_outside(tile.latitudes, min_lat, max_lat)
            tile.longitudes = ma.masked_outside(tile.longitudes, min_lon, max_lon)

            data_mask = self._data_mask_logical_or(tile)

            tile.data = ma.masked_where(data_mask, tile.data)

        tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

    def mask_tiles_to_polygon(self, bounding_polygon, tiles):

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        return self.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, tiles)

    def mask_tiles_to_polygon_and_time(self, bounding_polygon, start_time, end_time, tiles):
        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        return self.mask_tiles_to_bbox_and_time(min_lat, max_lat, min_lon, max_lon, start_time, end_time, tiles)

    def mask_tiles_to_time_range(self, start_time, end_time, tiles):
        """
        Masks data in tiles to specified time range.
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :param tiles: List of tiles
        :return: A list tiles with data masked to specified time range
        """
        if 0 <= start_time <= end_time:
            for tile in tiles:
                tile.times = ma.masked_outside(tile.times, start_time, end_time)

                data_mask = self._data_mask_logical_or(tile)

                # If this is multi-var, need to mask each variable separately.
                if tile.is_multi:
                    # Combine space/time mask with existing mask on data
                    data_mask = reduce(np.logical_or, [tile.data[0].mask, data_mask])

                    num_vars = len(tile.data)
                    multi_data_mask = np.repeat(data_mask[np.newaxis, ...], num_vars, axis=0)
                    tile.data = ma.masked_where(multi_data_mask, tile.data)
                else:
                    tile.data = ma.masked_where(data_mask, tile.data)

            tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

    def get_tile_count(self, ds, bounding_polygon=None, start_time=0, end_time=-1, metadata=None, **kwargs):
        """
        Return number of tiles that match search criteria.
        :param ds: The dataset name to search
        :param bounding_polygon: The polygon to search for tiles
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :return: number of tiles that match search criteria
        """
        return self._metadatastore.get_tile_count(ds, bounding_polygon, start_time, end_time, metadata, **kwargs)

    def fetch_data_for_tiles(self, *tiles, **kwargs):

        nexus_tile_ids = set([tile.tile_id for tile in tiles])
        matched_tile_data = self._datastore.fetch_nexus_tiles(*nexus_tile_ids)

        tile_data_by_id = {str(a_tile_data.tile_id): a_tile_data for a_tile_data in matched_tile_data}

        missing_data = nexus_tile_ids.difference(list(tile_data_by_id.keys()))
        if len(missing_data) > 0:
            raise Exception("Missing data for tile_id(s) %s." % missing_data)

        desired_projection = kwargs['desired_projection'] if 'desired_projection' in kwargs else self.desired_projection

        for a_tile in tiles:
            lats, lons, times, data, meta, is_multi_var = tile_data_by_id[a_tile.tile_id].get_lat_lon_time_data_meta(
                projection=desired_projection
            )

            a_tile.latitudes = lats
            a_tile.longitudes = lons
            a_tile.times = times
            a_tile.data = data
            a_tile.meta_data = meta
            a_tile.is_multi = is_multi_var
            a_tile.projection = desired_projection

            del (tile_data_by_id[a_tile.tile_id])

        return tiles

    def _metadata_store_docs_to_tiles(self, *store_docs):

        tiles = []
        for store_doc in store_docs:
            tile = Tile()
            try:
                tile.tile_id = store_doc['id']
            except KeyError:
                pass

            try:
                min_lat = store_doc['tile_min_lat']
                min_lon = store_doc['tile_min_lon']
                max_lat = store_doc['tile_max_lat']
                max_lon = store_doc['tile_max_lon']

                if isinstance(min_lat, list):
                    min_lat = min_lat[0]
                if isinstance(min_lon, list):
                    min_lon = min_lon[0]
                if isinstance(max_lat, list):
                    max_lat = max_lat[0]
                if isinstance(max_lon, list):
                    max_lon = max_lon[0]

                tile.bbox = BBox(min_lat, max_lat, min_lon, max_lon)
            except KeyError:
                pass

            try:
                tile.dataset = store_doc['dataset_s']
            except KeyError:
                pass

            try:
                tile.dataset_id = store_doc['dataset_id_s']
            except KeyError:
                pass

            try:
                tile.granule = store_doc['granule_s']
            except KeyError:
                pass

            try:
                tile.min_time = datetime.strptime(store_doc['tile_min_time_dt'], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC)
            except KeyError:
                pass

            try:
                tile.max_time = datetime.strptime(store_doc['tile_max_time_dt'], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC)
            except KeyError:
                pass

            try:
                tile.section_spec = store_doc['sectionSpec_s']
            except KeyError:
                pass

            try:
                tile.tile_stats = TileStats(
                    store_doc['tile_min_val_d'], store_doc['tile_max_val_d'],
                    store_doc['tile_avg_val_d'], store_doc['tile_count_i']
                )
            except KeyError:
                pass

            try:
                # Ensure backwards compatibility by working with old
                # tile_var_name_s and tile_standard_name_s fields to

                # will be overwritten if tile_var_name_ss is present
                # as well.
                if '[' in store_doc['tile_var_name_s']:
                    var_names = json.loads(store_doc['tile_var_name_s'])
                else:
                    var_names = [store_doc['tile_var_name_s']]

                standard_name = store_doc.get(
                        'tile_standard_name_s',
                        json.dumps([None] * len(var_names))
                )
                if '[' in standard_name:
                    standard_names = json.loads(standard_name)
                else:
                    standard_names = [standard_name]

                tile.variables = []
                for var_name, standard_name in zip(var_names, standard_names):
                    tile.variables.append(TileVariable(
                        variable_name=var_name,
                        standard_name=standard_name
                    ))
            except KeyError:
                pass


            if 'tile_var_name_ss' in store_doc:
                tile.variables = []
                for var_name in store_doc['tile_var_name_ss']:
                    standard_name_key = f'{var_name}.tile_standard_name_s'
                    standard_name = store_doc.get(standard_name_key)
                    tile.variables.append(TileVariable(
                        variable_name=var_name,
                        standard_name=standard_name
                    ))

            tiles.append(tile)

        return tiles

    def pingSolr(self):
        status = self._metadatastore.ping()
        if status and status["status"] == "OK":
            return True
        else:
            return False

    @staticmethod
    def _get_config_files(filename):
        log = logging.getLogger(__name__)
        candidates = []
        extensions = ['.default', '']
        for extension in extensions:
            try:
                candidate = pkg_resources.resource_filename(__name__, filename + extension)
                log.info('use config file {}'.format(filename + extension))
                candidates.append(candidate)
            except KeyError as ke:
                log.warning('configuration file {} not found'.format(filename + extension))

        return candidates
