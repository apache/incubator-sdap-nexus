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
import logging
import sys
import json
from datetime import datetime
from functools import wraps, reduce, partial

import numpy as np
import numpy.ma as ma
import pkg_resources
from pytz import timezone, UTC
from shapely.geometry import MultiPolygon, box
import pysolr

import threading
from time import sleep

from .backends.nexusproto.backend import NexusprotoTileService
from .backends.zarr.backend import ZarrBackend


from abc import ABC, abstractmethod

from .AbstractTileService import AbstractTileService

from .model.nexusmodel import Tile, BBox, TileStats, TileVariable
from typing import Dict, Union

from webservice.webmodel import DatasetNotFoundException, NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger("testing")


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
                    args[0].fetch_data_for_tiles(*tiles)
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


SOLR_LOCK = threading.Lock()
DS_LOCK = threading.Lock()
thread_local = threading.local()



class NexusTileService(AbstractTileService):
    backends: Dict[Union[None, str], Dict[str, Union[AbstractTileService, bool]]] = {}

    def __init__(self, config=None):
        self._config = configparser.RawConfigParser()
        self._config.read(NexusTileService._get_config_files('config/datasets.ini'))

        self._alg_config = config

        if config:
            self.override_config(config)

        NexusTileService.backends[None] = {"backend": NexusprotoTileService(False, False, config), 'up': True}
        NexusTileService.backends['__nexusproto__'] = NexusTileService.backends[None]

        def __update_datasets():
            while True:
                with DS_LOCK:
                    self._update_datasets()
                sleep(3600)

        threading.Thread(target=__update_datasets, name='dataset_update', daemon=False).start()



    @staticmethod
    def __get_backend(dataset_s) -> AbstractTileService:
        if dataset_s not in NexusTileService.backends:
            raise DatasetNotFoundException(reason=f'Dataset {dataset_s} is not currently loaded/ingested')

        b = NexusTileService.backends[dataset_s]

        if not b['up']:
            success = b['backend'].try_connect()

            if not success:
                raise NexusProcessingException(reason=f'Dataset {dataset_s} is currently unavailable')
            else:
                NexusTileService.backends[dataset_s]['up'] = True

        return b['backend']

    def _update_datasets(self):
        solr_url = self._config.get("solr", "host")
        solr_core = self._config.get("solr", "core")
        solr_kwargs = {}

        if self._config.has_option("solr", "time_out"):
            solr_kwargs["timeout"] = self._config.get("solr", "time_out")

        with SOLR_LOCK:
            solrcon = getattr(thread_local, 'solrcon', None)
            if solrcon is None:
                solr_url = '%s/solr/%s' % (solr_url, solr_core)
                solrcon = pysolr.Solr(solr_url, **solr_kwargs)
                thread_local.solrcon = solrcon

            solrcon = solrcon

            response = solrcon.search('*:*')

        present_datasets = set()

        for dataset in response.docs:
            d_id = dataset['dataset_s']
            store_type = dataset.get('store_type_s', 'nexusproto')

            present_datasets.add(d_id)

            if d_id in NexusTileService.backends:
                continue
                # is_up = NexusTileService.backends[d_id]['backend'].try_connect()

            if store_type == 'nexus_proto' or store_type == 'nexusproto':
                NexusTileService.backends[d_id] = NexusTileService.backends[None]
            elif store_type == 'zarr':
                ds_config = json.loads(dataset['config'][0])
                NexusTileService.backends[d_id] = {
                    'backend': ZarrBackend(ds_config),
                    'up': True
                }
            else:
                logger.warning(f'Unsupported backend {store_type} for dataset {d_id}')

        removed_datasets = set(NexusTileService.backends.keys()).difference(present_datasets)

        for dataset in removed_datasets:
            logger.info(f"Removing dataset {dataset}")
            del NexusTileService.backends[dataset]

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
        return NexusTileService.__get_backend('__nexusproto__').find_tile_by_id(tile_id)

    @tile_data()
    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        return NexusTileService.__get_backend('__nexusproto__').find_tiles_by_id(tile_ids, ds=ds, **kwargs)

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        return NexusTileService.__get_backend(dataset).find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon,
                                                                              dataset, start_time, end_time,
                                                                              metrics_callback, **kwargs)

    @tile_data()
    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year, **kwargs):
        return NexusTileService.__get_backend(ds).find_tile_by_polygon_and_most_recent_day_of_year(
            bounding_polygon, ds, day_of_year, **kwargs
        )

    @tile_data()
    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return NexusTileService.__get_backend(dataset).find_all_tiles_in_box_at_time(
            min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs
        )

    @tile_data()
    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        return NexusTileService.__get_backend(dataset).find_all_tiles_in_polygon_at_time(
            bounding_polygon, dataset, time, **kwargs
        )

    @tile_data()
    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall in the given box in the Solr index
        if type(start_time) is datetime:
            start_time = (start_time - EPOCH).total_seconds()
        if type(end_time) is datetime:
            end_time = (end_time - EPOCH).total_seconds()

        return NexusTileService.__get_backend(ds).find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs
        )

    @tile_data()
    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        return NexusTileService.__get_backend(ds).find_tiles_in_polygon(
            bounding_polygon, ds, start_time, end_time, **kwargs
        )

    @tile_data()
    def find_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        return NexusTileService.__get_backend(ds).find_tiles_by_metadata(
            metadata, ds, start_time, end_time, **kwargs
        )

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
        return NexusTileService.__get_backend(ds).find_tiles_by_exact_bounds(
            bounds, ds, start_time, end_time, **kwargs
        )

    @tile_data()
    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return NexusTileService.__get_backend(dataset).find_all_boundary_tiles_at_time(
            min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs
        )

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
        return NexusTileService.__get_backend(ds).get_min_max_time_by_granule(
            ds, granule_name
        )

    def get_dataset_overall_stats(self, ds):
        return NexusTileService.__get_backend(ds).get_dataset_overall_stats(ds)

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

    def fetch_data_for_tiles(self, *tiles):

        nexus_tile_ids = set([tile.tile_id for tile in tiles])
        matched_tile_data = self._datastore.fetch_nexus_tiles(*nexus_tile_ids)

        tile_data_by_id = {str(a_tile_data.tile_id): a_tile_data for a_tile_data in matched_tile_data}

        missing_data = nexus_tile_ids.difference(list(tile_data_by_id.keys()))
        if len(missing_data) > 0:
            raise Exception("Missing data for tile_id(s) %s." % missing_data)

        for a_tile in tiles:
            lats, lons, times, data, meta, is_multi_var = tile_data_by_id[a_tile.tile_id].get_lat_lon_time_data_meta()

            a_tile.latitudes = lats
            a_tile.longitudes = lons
            a_tile.times = times
            a_tile.data = data
            a_tile.meta_data = meta
            a_tile.is_multi = is_multi_var

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
