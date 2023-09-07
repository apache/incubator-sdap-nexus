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

"""
Module for querying CDMS In-Situ API
"""
import logging
import requests
from datetime import datetime
from webservice.algorithms.doms import config as insitu_endpoints
from urllib.parse import urlencode
from webservice.webmodel import NexusProcessingException


CONNECT_TIMEOUT = 9.05     # Recommended to be just above a multiple of 3 seconds
READ_TIMEOUT = 303          # Just above current gateway timeout
TIMEOUTS = (CONNECT_TIMEOUT, READ_TIMEOUT)

logger = logging.getLogger(__name__)


def query_insitu_schema():
    """
    Query the "cdms_schema" insitu endpoint. This will return the JSON
    schema used to construct the data, which will contain useful
    metadata
    """
    schema_endpoint = insitu_endpoints.getSchemaEndpoint()
    logger.info("Querying schema")
    try:
        response = requests.get(schema_endpoint, timeout=TIMEOUTS)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
        raise NexusProcessingException(code=504, reason=f'Insitu schema request timed out')

    response.raise_for_status()
    return response.json()


def query_insitu(dataset, variable, start_time, end_time, bbox, platform, depth_min, depth_max,
               items_per_page=20000, session=None):
    """
    Query insitu API, page through results, and aggregate
    """
    try:
        start_time = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        end_time = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    provider = insitu_endpoints.get_provider_name(dataset)
    project = insitu_endpoints.get_project_name(dataset)

    params = {
        'itemsPerPage': items_per_page,
        'startTime': start_time,
        'endTime': end_time,
        'bbox': bbox,
        'minDepth': depth_min,
        'maxDepth': depth_max,
        'provider': provider,
        'project': project,
        'platform': platform,
    }

    if variable is not None:
        params['variable'] = variable

    insitu_response = {}

    # Page through all insitu results
    next_page_url = insitu_endpoints.getEndpoint(provider, dataset)
    while next_page_url is not None and next_page_url != 'NA':
        thetime = datetime.now()
        if params == {}:
            logger.info(f"Starting insitu request: {next_page_url}")
        else:
            logger.info(f"Starting insitu request: {next_page_url}?{urlencode(params)}")


        try:
            if session is not None:
                response = session.get(next_page_url, params=params, timeout=TIMEOUTS)
            else:
                response = requests.get(next_page_url, params=params, timeout=TIMEOUTS)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
            raise NexusProcessingException(code=504, reason=f'Insitu request timed out after {str(datetime.now() - thetime)} seconds')

        logger.info(f'Insitu request {response.url} finished. Code: {response.status_code} Time: {str(datetime.now() - thetime)}')

        response.raise_for_status()
        insitu_page_response = response.json()

        if not insitu_response:
            insitu_response = insitu_page_response
        else:
            insitu_response['results'].extend(insitu_page_response['results'])

        next_page_url = insitu_page_response.get('next', None)
        params = {}  # Remove params, they are already included in above URL

    logger.info(f"Insitu query completed, returning {len(insitu_response['results']):,} points")

    return insitu_response
