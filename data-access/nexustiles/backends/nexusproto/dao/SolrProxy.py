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

import json
import logging
import threading
import time
from datetime import datetime
from pytz import timezone, UTC

import requests
import pysolr
from shapely import wkt

SOLR_CON_LOCK = threading.Lock()
thread_local = threading.local()

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
SOLR_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


class SolrProxy(object):
    def __init__(self, config):
        self.solrUrl = config.get("solr", "host")
        self.solrCore = config.get("solr", "core")
        solr_kargs = {}
        if config.has_option("solr", "time_out"):
            solr_kargs["timeout"] = config.get("solr", "time_out")
        self.logger = logging.getLogger('nexus')

        with SOLR_CON_LOCK:
            solrcon = getattr(thread_local, 'solrcon', None)
            if solrcon is None:
                solr_url = '%s/solr/%s' % (self.solrUrl, self.solrCore)
                self.logger.info("connect to solr, url {} with option(s) = {}".format(solr_url, solr_kargs))
                solrcon = pysolr.Solr(solr_url, **solr_kargs)
                thread_local.solrcon = solrcon

            self.solrcon = solrcon

    def find_tile_by_id(self, tile_id):

        search = 'id:%s' % tile_id

        params = {
            'rows': 1
        }

        results, start, found = self.do_query(*(search, None, None, True, None), **params)

        assert len(results) == 1, "Found %s results, expected exactly 1" % len(results)
        return [results[0]]

    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):

        if ds is not None:
            search = 'dataset_s:%s' % ds
        else:
            search = '*:*'

        additionalparams = {
            'fq': [
                "{!terms f=id}%s" % ','.join(tile_ids)
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        results = self.do_query_all(*(search, None, None, False, None), **additionalparams)

        assert len(results) == len(tile_ids), "Found %s results, expected exactly %s" % (len(results), len(tile_ids))
        return results

    def find_min_date_from_tiles(self, tile_ids, ds=None, **kwargs):

        if ds is not None:
            search = 'dataset_s:%s' % ds
        else:
            search = '*:*'

        kwargs['rows'] = 1
        kwargs['fl'] = 'tile_min_time_dt'
        kwargs['sort'] = ['tile_min_time_dt asc']
        additionalparams = {
            'fq': [
                "{!terms f=id}%s" % ','.join(tile_ids) if len(tile_ids) > 0 else ''
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        results, start, found = self.do_query(*(search, None, None, True, None), **additionalparams)

        return self.convert_iso_to_datetime(results[0]['tile_min_time_dt'])

    def find_max_date_from_tiles(self, tile_ids, ds=None, **kwargs):

        if ds is not None:
            search = 'dataset_s:%s' % ds
        else:
            search = '*:*'

        kwargs['rows'] = 1
        kwargs['fl'] = 'tile_max_time_dt'
        kwargs['sort'] = ['tile_max_time_dt desc']
        additionalparams = {
            'fq': [
                "{!terms f=id}%s" % ','.join(tile_ids) if len(tile_ids) > 0 else ''
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        results, start, found = self.do_query(*(search, None, None, True, None), **additionalparams)

        return self.convert_iso_to_datetime(results[0]['tile_max_time_dt'])

    def find_min_max_date_from_granule(self, ds, granule_name, **kwargs):
        search = 'dataset_s:%s' % ds

        kwargs['rows'] = 1
        kwargs['fl'] = 'tile_min_time_dt'
        kwargs['sort'] = ['tile_min_time_dt asc']
        additionalparams = {
            'fq': [
                "granule_s:%s" % granule_name
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)
        results, start, found = self.do_query(*(search, None, None, False, None), **additionalparams)
        start_time = self.convert_iso_to_datetime(results[0]['tile_min_time_dt'])

        kwargs['fl'] = 'tile_max_time_dt'
        kwargs['sort'] = ['tile_max_time_dt desc']
        additionalparams = {
            'fq': [
                "granule_s:%s" % granule_name
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)
        results, start, found = self.do_query(*(search, None, None, False, None), **additionalparams)
        end_time = self.convert_iso_to_datetime(results[0]['tile_max_time_dt'])

        return start_time, end_time

    def get_data_series_list(self):

        datasets = self.get_data_series_list_simple()

        def get_spatial_bound(dataset, lat_lon, min_max):
            search = f'dataset_s:{dataset}'

            field = f'tile_{min_max}_{lat_lon}'

            order = 'ASC' if min_max == 'min' else 'DESC'

            params = dict(
                rows=1,
                sort=[f'{field} {order}']
            )

            results = self.do_query_raw(*(search, None, None, False, None), **params)

            v = results.docs[0][field]

            if isinstance(v, list):
                v = v[0]

            return v

        for dataset in datasets:
            min_date = self.find_min_date_from_tiles([], ds=dataset['title'])
            max_date = self.find_max_date_from_tiles([], ds=dataset['title'])
            dataset['start'] = (min_date - EPOCH).total_seconds()
            dataset['end'] = (max_date - EPOCH).total_seconds()
            dataset['iso_start'] = min_date.strftime(ISO_8601)
            dataset['iso_end'] = max_date.strftime(ISO_8601)

            min_lat = get_spatial_bound(dataset['title'], 'lat', 'min')
            max_lat = get_spatial_bound(dataset['title'], 'lat', 'max')
            min_lon = get_spatial_bound(dataset['title'], 'lon', 'min')
            max_lon = get_spatial_bound(dataset['title'], 'lon', 'max')

            dataset['spatial_extent'] = '{:0.2f},{:0.2f},{:0.2f},{:0.2f}'.format(min_lon, min_lat, max_lon, max_lat)

        return datasets

    def get_data_series_list_simple(self):
        search = "*:*"
        params = {
            'rows': 0,
            "facet": "true",
            "facet.field": "dataset_s",
            "facet.mincount": "1",
            "facet.limit": "-1"
        }


        response = self.do_query_raw(*(search, None, None, False, None), **params)
        l = []
        for g, v in zip(*[iter(response.facets["facet_fields"]["dataset_s"])]*2):
            l.append({
                "shortName": g,
                "title": g,
                "tileCount": v,
                "type": 'nexusproto'
            })
        l = sorted(l, key=lambda entry: entry["title"])
        return l

    def get_data_series_stats(self, ds):
        search = "dataset_s:%s" % ds
        params = {
            "facet": "true",
            "facet.field": ["dataset_s", "tile_max_time_dt"],
            "facet.limit": "-1",
            "facet.mincount": "1",
            "facet.pivot": "{!stats=piv1}dataset_s",
            "stats": "on",
            "stats.field": ["{!tag=piv1 min=true max=true sum=false}tile_max_time_dt","{!tag=piv1 min=true max=false sum=false}tile_min_val_d","{!tag=piv1 min=false max=true sum=false}tile_max_val_d"]
        }

        response = self.do_query_raw(*(search, None, None, False, None), **params)

        stats = {}

        for g in response.facets["facet_pivot"]["dataset_s"]:
            if g["value"] == ds:
                stats["start"] = self.convert_iso_to_timestamp(g["stats"]["stats_fields"]["tile_max_time_dt"]["min"])
                stats["end"] = self.convert_iso_to_timestamp(g["stats"]["stats_fields"]["tile_max_time_dt"]["max"])
                stats["minValue"] = g["stats"]["stats_fields"]["tile_min_val_d"]["min"]
                stats["maxValue"] = g["stats"]["stats_fields"]["tile_max_val_d"]["max"]


        stats["availableDates"] = []
        for dt in response.facets["facet_fields"]["tile_max_time_dt"][::2]:
            stats["availableDates"].append(self.convert_iso_to_timestamp(dt))

        stats["availableDates"] = sorted(stats["availableDates"])

        return stats

    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year):

        search = 'dataset_s:%s' % ds

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        params = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]",
                "day_of_year_i:[* TO %s]" % day_of_year
            ],
            'rows': 1
        }

        results, start, found = self.do_query(
            *(search, None, None, True, ('day_of_year_i desc',)), **params)

        return [results[0]]

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs):

        search = 'dataset_s:%s' % ds

        search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
        search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "{!frange l=0 u=0}ms(tile_min_time_dt,tile_max_time_dt)",
                "tile_count_i:[1 TO *]",
                "tile_min_time_dt:[%s TO %s] " % (search_start_s, search_end_s)
            ],
            'rows': 0,
            'facet': 'true',
            'facet.field': 'tile_min_time_dt',
            'facet.mincount': '1',
            'facet.limit': '-1'
        }

        self._merge_kwargs(additionalparams, **kwargs)

        response = self.do_query_raw(*(search, None, None, False, None), **additionalparams)

        daysinrangeasc = sorted(
            [(datetime.strptime(a_date, SOLR_FORMAT) - datetime.utcfromtimestamp(0)).total_seconds() for a_date
             in response.facets['facet_fields']['tile_min_time_dt'][::2]])

        return daysinrangeasc

    def find_all_tiles_in_box_sorttimeasc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time=0,
                                          end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 <= start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
            search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)
            
        min_elevation = kwargs['min_elevation'] if 'min_elevation' in kwargs else None
        max_elevation = kwargs['max_elevation'] if 'max_elevation' in kwargs else None
        
        if min_elevation and max_elevation:
            elevation_clause = "(" \
                          "tile_min_elevation_d:[%s TO %s] " \
                          "OR tile_max_elevation_d:[%s TO %s] " \
                          "OR (tile_min_elevation_d:[* TO %s] AND tile_max_elevation_d:[%s TO *])" \
                          ")" % (
                              min_elevation, max_elevation,
                              min_elevation, max_elevation,
                              min_elevation, max_elevation
                          )
            additionalparams['fq'].append(elevation_clause)
        elif min_elevation:
            elevation_clause = "(" \
                           "tile_min_elevation_d:[%s TO *] " \
                           "OR tile_max_elevation_d:[%s TO *] " \
                           ")" % (
                               min_elevation,
                               min_elevation
                           )
            additionalparams['fq'].append(elevation_clause)
        elif max_elevation:
            elevation_clause = "(" \
                           "tile_min_elevation_d:[* TO %s] " \
                           "OR tile_max_elevation_d:[* TO %s] " \
                           ")" % (
                               max_elevation,
                               max_elevation
                           )
            additionalparams['fq'].append(elevation_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, 'tile_min_time_dt asc, tile_max_time_dt asc'),
            **additionalparams)

    def find_all_tiles_in_polygon_sorttimeasc(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 <= start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
            search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, 'tile_min_time_dt asc, tile_max_time_dt asc'),
            **additionalparams)

    def find_all_tiles_in_polygon(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 <= start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
            search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, None),
            **additionalparams)

    def find_distinct_bounding_boxes_in_polygon(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]"
            ],
            'rows': 0,
            'facet': 'true',
            'facet.field': 'geo_s',
            'facet.limit': -1,
            'facet.mincount': 1
        }

        if 0 <= start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
            search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        response = self.do_query_raw(*(search, None, None, False, None), **additionalparams)

        distinct_bounds = [wkt.loads(key).bounds for key in response.facets["facet_fields"]["geo_s"][::2]]

        return distinct_bounds

    def find_tiles_by_exact_bounds(self, minx, miny, maxx, maxy, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "tile_min_lon:\"%s\"" % minx,
                "tile_min_lat:\"%s\"" % miny,
                "tile_max_lon:\"%s\"" % maxx,
                "tile_max_lat:\"%s\"" % maxy,
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 <= start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
            search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, None),
            **additionalparams)

    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, search_time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(search_time).strftime(SOLR_FORMAT)
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, ds, search_time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(search_time).strftime(SOLR_FORMAT)
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def find_all_tiles_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime(SOLR_FORMAT)
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:\"Within(ENVELOPE(%s,%s,%s,%s))\"" % (min_lon, max_lon, max_lat, min_lat),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, "product(tile_avg_val_d, tile_count_i),*", None, False, None),
                                 **additionalparams)

    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime(SOLR_FORMAT)
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:\"Intersects(MultiLineString((%s %s, %s %s),(%s %s, %s %s),(%s %s, %s %s),(%s %s, %s %s)))\"" % (
                    min_lon, max_lat, max_lon, max_lat, min_lon, max_lat, min_lon, min_lat, max_lon, max_lat, max_lon,
                    min_lat, min_lon, min_lat, max_lon, min_lat),
                "-geo:\"Within(ENVELOPE(%s,%s,%s,%s))\"" % (min_lon, max_lon, max_lat, min_lat),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def find_all_tiles_by_metadata(self, metadata, ds, start_time=0, end_time=-1, **kwargs):
        """
        Get a list of tile metadata that matches the specified metadata, start_time, end_time.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tile metadata
        """
        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': metadata
        }

        if 0 <= start_time <= end_time:
            additionalparams['fq'].append(self.get_formatted_time_clause(start_time, end_time))

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, None),
            **additionalparams)

    def get_formatted_time_clause(self, start_time, end_time):
        search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
        search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

        time_clause = "(" \
                      "tile_min_time_dt:[%s TO %s] " \
                      "OR tile_max_time_dt:[%s TO %s] " \
                      "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                      ")" % (
                          search_start_s, search_end_s,
                          search_start_s, search_end_s,
                          search_start_s, search_end_s
                          )
        return time_clause

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
        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "tile_count_i:[1 TO *]"
            ],
            'rows': 0
        }

        if bounding_polygon:
            min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds
            additionalparams['fq'].append("geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon))

        if 0 <= start_time <= end_time:
            additionalparams['fq'].append(self.get_formatted_time_clause(start_time, end_time))

        if metadata:
            additionalparams['fq'].extend(metadata)

        self._merge_kwargs(additionalparams, **kwargs)

        results, start, found = self.do_query(*(search, None, None, True, None), **additionalparams)

        return found

    def do_query(self, *args, **params):

        response = self.do_query_raw(*args, **params)

        return response.docs, response.raw_response['response']['start'], response.hits

    def do_query_raw(self, *args, **params):

        if 'fl' not in list(params.keys()) and args[1]:
            params['fl'] = args[1]

        if 'sort' not in list(params.keys()) and args[4]:
            params['sort'] = args[4]

        # If dataset_s is specified as the search term,
        # add the _route_ parameter to limit the search to the correct shard
        if 'dataset_s:' in args[0]:
            ds = args[0].split(':')[-1]
            params['shard_keys'] = ds + '!'

        with SOLR_CON_LOCK:
            response = self.solrcon.search(args[0], **params)

        return response


    def do_query_all(self, *args, **params):

        results = []

        response = self.do_query_raw(*args, **params)
        results.extend(response.docs)

        limit = min(params.get('limit', float('inf')), response.hits)

        while len(results) < limit:
            params['start'] = len(results)
            response = self.do_query_raw(*args, **params)
            results.extend(response.docs)

        assert len(results) == limit

        return results

    def convert_iso_to_datetime(self, date):
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)

    def convert_iso_to_timestamp(self, date):
        return (self.convert_iso_to_datetime(date) - EPOCH).total_seconds()

    def ping(self):
        solrAdminPing = '%s/solr/%s/admin/ping' % (self.solrUrl, self.solrCore)
        try:
            r = requests.get(solrAdminPing, params={'wt': 'json'})
            results = json.loads(r.text)
            return results
        except:
            return None

    @staticmethod
    def _merge_kwargs(additionalparams, **kwargs):
        # Only Solr-specific kwargs are parsed
        # And the special 'limit'
        try:
            additionalparams['limit'] = kwargs['limit']
        except KeyError:
            pass

        try:
            additionalparams['_route_'] = kwargs['_route_']
        except KeyError:
            pass

        try:
            additionalparams['rows'] = kwargs['rows']
        except KeyError:
            pass

        try:
            additionalparams['start'] = kwargs['start']
        except KeyError:
            pass

        try:
            kwfq = kwargs['fq'] if isinstance(kwargs['fq'], list) else list(kwargs['fq'])
        except KeyError:
            kwfq = []

        try:
            additionalparams['fq'].extend(kwfq)
        except KeyError:
            additionalparams['fq'] = kwfq

        try:
            kwfl = kwargs['fl'] if isinstance(kwargs['fl'], list) else [kwargs['fl']]
        except KeyError:
            kwfl = []

        try:
            additionalparams['fl'].extend(kwfl)
        except KeyError:
            additionalparams['fl'] = kwfl

        try:
            s = kwargs['sort'] if isinstance(kwargs['sort'], list) else [kwargs['sort']]
        except KeyError:
            s = None

        try:
            additionalparams['sort'].extend(s)
        except KeyError:
            if s is not None:
                additionalparams['sort'] = s
