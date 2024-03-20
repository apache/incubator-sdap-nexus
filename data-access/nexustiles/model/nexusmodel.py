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

from collections import namedtuple

import numpy as np
from dataclasses import dataclass
from functools import reduce

NexusPoint = namedtuple('NexusPoint', 'latitude longitude depth time index data_vals')
BBox = namedtuple('BBox', 'min_lat max_lat min_lon max_lon')
TileStats = namedtuple('TileStats', 'min max mean count')


@dataclass
class TileVariable:
    """
    TileVariable class representing a single variable. This contains
    both the name of the variable and the CF standard name.

    :attribute variable_name: Name of the variable in the tile. This is
    the name from the satellite data file.
    :attribute standard_name: CF Standard name of the variable in the
    tile. This is the 'standard_name' attribute from the satellite data file. This might be null in the case where the source variable does not contain a standard_name attribute.
    """
    variable_name: str = None
    standard_name: str = None


@dataclass
class Tile(object):
    """
    Tile class representing the contents of a tile. The tile contents
    are populated using the metadata store and the data store.

    :attribute tile_id: Unique UUID tile ID, also used in data store and
        metadata store to distinguish this tile
    :attribute dataset_id: Unique dataset ID this tile belongs to
    :attribute section_spec: A summary of the indices used from the source
        granule to create this tile. Format is
        dimension:min_index:max_index,dimension:min_index:max_index,...
    :attribute dataset: The name of the dataset this tile belongs to
    :attribute granule: The name of the granule this tile is sourced from
    :attribute bbox: Comma-separated string representing the spatial bounds
        of this tile. The format is min_lon, min_lat, max_lon, max_lat
    :attribute min_time: ISO 8601 formatted timestamp representing the
        temporal minimum of this tile
    :attribute max_time: ISO 8601 formatted timestamp representing the
        temporal minimum of this tile
    :attribute tile_stats: Dictionary representing the min, max, mean, and
        count of this tile
    :attribute variables: A list of size N where N == the number of vars
        this tile represents. The list type is TileVariable.
    :attribute latitudes: 1-d ndarray representing the latitude values of
        this tile
    :attribute longitudes: 1-d ndarray representing the longitude values of
        this tile
    :attribute times: 1-d ndarray representing the longitude values of
        this tile
    :attribute data: This should be an ndarray with shape len(times) x
        len(latitudes) x len(longitudes) x num_vars
    :attribute is_multi: 'True' if this is a multi-var tile
    :attribute meta_data: dict of the form {'meta_data_name':
        [[[ndarray]]]}. Each ndarray should be the same shape as data.
    """
    tile_id: str = None
    dataset_id: str = None
    section_spec: str = None
    dataset: str = None
    granule: str = None
    bbox: str = None
    min_time: str = None
    max_time: str = None
    tile_stats: dict = None
    variables: list = None
    latitudes: np.array = None
    longitudes: np.array = None
    elevation: np.array = None
    times: np.array = None
    data: np.array = None
    is_multi: bool = None
    meta_data: dict = None

    def __str__(self):
        return str(self.get_summary())

    def get_summary(self):
        summary = dict(self.__dict__)

        try:
            summary['latitudes'] = self.latitudes.shape
        except AttributeError:
            summary['latitudes'] = 'None'

        try:
            summary['longitudes'] = self.longitudes.shape
        except AttributeError:
            summary['longitudes'] = 'None'

        try:
            summary['times'] = self.times.shape
        except AttributeError:
            summary['times'] = 'None'

        try:
            summary['data'] = self.data.shape
        except AttributeError:
            summary['data'] = 'None'

        try:
            summary['meta_data'] = {meta_name: meta_array.shape for meta_name, meta_array in self.meta_data.items()}
        except AttributeError:
            summary['meta_data'] = 'None'

        return summary

    def nexus_point_generator(self, include_nan=False):
        indices = self.get_indices(include_nan)

        if include_nan:
            for index in indices:
                time = self.times[index[0]]
                lat = self.latitudes[index[1]]
                lon = self.longitudes[index[2]]
                if self.is_multi:
                    data_vals = [data[index] for data in self.data]
                else:
                    data_vals = self.data[index]

                if self.elevation is not None:
                    elevation = self.elevation[index]
                else:
                    elevation = np.nan

                point = NexusPoint(lat, lon, elevation, time, index, data_vals)
                yield point
        else:
            for index in indices:
                index = tuple(index)
                time = self.times[index[0]]
                lat = self.latitudes[index[1]]
                lon = self.longitudes[index[2]]
                if self.is_multi:
                    data_vals = [data[index] for data in self.data]
                else:
                    data_vals = self.data[index]

                if self.elevation is not None:
                    elevation = self.elevation[index]
                else:
                    elevation = np.nan

                point = NexusPoint(lat, lon, elevation, time, index, data_vals)
                yield point

    def get_indices(self, include_nan=False):
        if include_nan:
            return list(np.ndindex(self.data.shape))
        if self.is_multi:
            combined_data_inv_mask = reduce(np.logical_and, [data.mask for data in self.data])
            return np.argwhere(np.logical_not(combined_data_inv_mask))
        else:
            return np.transpose(np.where(np.ma.getmaskarray(self.data) == False)).tolist()

    def contains_point(self, lat, lon):

        return contains_point(self.latitudes, self.longitudes, lat, lon)

    def update_stats(self):

        t_min = np.nanmin(self.data).item()
        t_max = np.nanmax(self.data).item()
        t_mean = np.ma.average(np.ma.masked_invalid(self.data).flatten(),
                               weights=np.cos(np.radians(np.repeat(self.latitudes, len(self.longitudes)))))
        t_count = self.data.size - np.count_nonzero(np.isnan(self.data))
        self.tile_stats = TileStats(t_min, t_max, t_mean, t_count)


def contains_point(latitudes, longitudes, lat, lon):
    minx, miny, maxx, maxy = np.ma.min(longitudes), np.ma.min(latitudes), np.ma.max(
        longitudes), np.ma.max(latitudes)
    return (
               (miny < lat or np.isclose(miny, lat)) and
               (lat < maxy or np.isclose(lat, maxy))
           ) and (
               (minx < lon or np.isclose(minx, lon)) and
               (lon < maxx or np.isclose(lon, maxx))
           )


def merge_tiles(tile_list):
    a = np.array([tile.times for tile in tile_list])
    assert np.ma.max(a) == np.ma.min(a)

    merged_times = tile_list[0].times
    merged_lats = np.ndarray((0,), dtype=np.float32)
    merged_lons = np.ndarray((0,), dtype=np.float32)
    merged_data = np.ndarray((0, 0), dtype=np.float32)

    for tile in tile_list:
        if np.ma.in1d(tile.latitudes, merged_lats).all() and not np.ma.in1d(tile.longitudes, merged_lons).all():
            merged_lons = np.ma.concatenate([merged_lons, tile.longitudes])
            merged_data = np.ma.hstack((merged_data, np.ma.squeeze(tile.data)))
        elif not np.ma.in1d(tile.latitudes, merged_lats).all() and np.ma.in1d(tile.longitudes, merged_lons).all():
            merged_lats = np.ma.concatenate([merged_lats, tile.latitudes])
            merged_data = np.ma.vstack((merged_data, np.ma.squeeze(tile.data)))
        elif not np.ma.in1d(tile.latitudes, merged_lats).all() and not np.ma.in1d(tile.longitudes, merged_lons).all():
            merged_lats = np.ma.concatenate([merged_lats, tile.latitudes])
            merged_lons = np.ma.concatenate([merged_lons, tile.longitudes])
            merged_data = block_diag(*[merged_data, np.ma.squeeze(tile.data)])
        else:
            raise Exception("Can't handle overlapping tiles")

    merged_data = merged_data[np.ma.argsort(merged_lats), :]
    merged_data = merged_data[:, np.ma.argsort(merged_lons)]
    merged_lats = merged_lats[np.ma.argsort(merged_lats),]
    merged_lons = merged_lons[np.ma.argsort(merged_lons),]

    merged_data = merged_data[np.newaxis, :]

    return merged_times, merged_lats, merged_lons, merged_data


def block_diag(*arrs):
    """Create a block diagonal matrix from the provided arrays.

    Given the inputs `A`, `B` and `C`, the output will have these
    arrays arranged on the diagonal::

        [[A, 0, 0],
         [0, B, 0],
         [0, 0, C]]

    If all the input arrays are square, the output is known as a
    block diagonal matrix.

    Parameters
    ----------
    A, B, C, ... : array-like, up to 2D
        Input arrays.  A 1D array or array-like sequence with length n is
        treated as a 2D array with shape (1,n).

    Returns
    -------
    D : ndarray
        Array with `A`, `B`, `C`, ... on the diagonal.  `D` has the
        same dtype as `A`.

    References
    ----------
    .. [1] Wikipedia, "Block matrix",
           http://en.wikipedia.org/wiki/Block_diagonal_matrix

    Examples
    --------
    >>> A = [[1, 0],
    ...      [0, 1]]
    >>> B = [[3, 4, 5],
    ...      [6, 7, 8]]
    >>> C = [[7]]
    >>> print(block_diag(A, B, C))
    [[1 0 0 0 0 0]
     [0 1 0 0 0 0]
     [0 0 3 4 5 0]
     [0 0 6 7 8 0]
     [0 0 0 0 0 7]]
    >>> block_diag(1.0, [2, 3], [[4, 5], [6, 7]])
    array([[ 1.,  0.,  0.,  0.,  0.],
           [ 0.,  2.,  3.,  0.,  0.],
           [ 0.,  0.,  0.,  4.,  5.],
           [ 0.,  0.,  0.,  6.,  7.]])

    """
    if arrs == ():
        arrs = ([],)
    arrs = [np.atleast_2d(a) for a in arrs]

    bad_args = [k for k in range(len(arrs)) if arrs[k].ndim > 2]
    if bad_args:
        raise ValueError("arguments in the following positions have dimension "
                         "greater than 2: %s" % bad_args)

    shapes = np.array([a.shape for a in arrs])
    out = np.ma.masked_all(np.sum(shapes, axis=0), dtype=arrs[0].dtype)

    r, c = 0, 0
    for i, (rr, cc) in enumerate(shapes):
        out[r:r + rr, c:c + cc] = arrs[i]
        r += rr
        c += cc
    return out


def find_nearest(array, value):
    idx = (np.abs(array - value)).argmin()
    return array[idx]


def get_approximate_value_for_lat_lon(tile_list, lat, lon):
    """
    This function pulls the value out of one of the tiles in tile_list that is the closest to the given
    lat, lon point.

    :returns float value closest to lat lon point or float('Nan') if the point is masked or not contained in any tile
    """

    try:
        times, lats, longs, data = merge_tiles(tile_list)
        if not contains_point(lats, longs, lat, lon):
            # Lat, Lon is out of bounds for these tiles
            return float('NaN')
    except AssertionError:
        # Tiles are not all at the same time
        return float('NaN')

    nearest_lat = find_nearest(lats, lat)
    nearest_long = find_nearest(longs, lon)

    data_val = data[0][(np.abs(lats - lat)).argmin()][(np.abs(longs - lon)).argmin()]

    return data_val.item() if (data_val is not np.ma.masked) and data_val.size == 1 else float('Nan')
