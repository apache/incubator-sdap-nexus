#!/usr/local/bin/python -u

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

import os
import requests
import requests.exceptions
import json
import json.decoder
import time
import sys
import logging
from kazoo.client import KazooClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)

MAX_RETRIES = int(os.environ["MAX_RETRIES"])
SDAP_ZK_SOLR = os.environ["SDAP_ZK_SOLR"]
SDAP_SOLR_URL = os.environ["SDAP_SOLR_URL"]
ZK_LOCK_GUID = os.environ["ZK_LOCK_GUID"]
MINIMUM_NODES = int(os.environ["MINIMUM_NODES"])
CREATE_COLLECTION_PARAMS = os.environ["CREATE_COLLECTION_PARAMS"]


def get_cluster_status():
    try:
        return requests.get("{}admin/collections?action=CLUSTERSTATUS".format(SDAP_SOLR_URL)).json()
    except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
        return False


logging.info("Attempting to aquire lock from {}".format(SDAP_ZK_SOLR))
zk_host, zk_chroot = SDAP_ZK_SOLR.split('/')
zk = KazooClient(hosts=zk_host)
zk.start()
zk.ensure_path(zk_chroot)
zk.chroot = zk_chroot
lock = zk.Lock("/collection-creator", ZK_LOCK_GUID)
try:
    with lock:  # blocks waiting for lock acquisition
        logging.info("Lock aquired. Checking for SolrCloud at {}".format(SDAP_SOLR_URL))
        # Wait for MAX_RETRIES for the entire Solr cluster to be available.
        attempts = 0
        status = None
        collection_exists = False
        while attempts <= MAX_RETRIES:
            status = get_cluster_status()
            if not status:
                # If we can't get the cluster status, my Solr node is not running
                attempts += 1
                logging.info("Waiting for Solr at {}".format(SDAP_SOLR_URL))
                time.sleep(1)
                continue
            else:
                # If we can get the cluster status, at least my Solr node is running
                # We can check if the collection exists already now
                if 'collections' in status['cluster'] and 'nexustiles' in status['cluster']['collections']:
                    # Collection already exists. Break out of the while loop
                    collection_exists = True
                    logging.info("nexustiles collection already exists.")
                    break
                else:
                    # Collection does not exist, but need to make sure number of expected nodes are running
                    live_nodes = status['cluster']['live_nodes']
                    if len(live_nodes) < MINIMUM_NODES:
                        # Not enough live nodes
                        logging.info("Found {} live node(s). Expected at least {}. Live nodes: {}".format(len(live_nodes), MINIMUM_NODES, live_nodes))
                        attempts += 1
                        time.sleep(1)
                        continue
                    else:
                        # We now have a full cluster, ready to create collection.
                        logging.info("Detected full cluster of at least {} nodes. Checking for nexustiles collection".format(MINIMUM_NODES))
                        break

        # Make sure we didn't exhaust our retries
        if attempts > MAX_RETRIES:
            raise RuntimeError("Exceeded {} retries while waiting for at least {} nodes to become live for {}".format(MAX_RETRIES, MINIMUM_NODES, SDAP_SOLR_URL))

        # Full cluster, did not exceed retries. Check if collection already exists
        if not collection_exists:
            # Collection does not exist, create it.
            create_command = "{}admin/collections?action=CREATE&{}".format(SDAP_SOLR_URL, CREATE_COLLECTION_PARAMS)
            logging.info("Creating collection with command {}".format(create_command))
            create_response = requests.get(create_command).json()
            if 'failure' not in create_response:
                # Collection created, we're done.
                logging.info("Collection created. {}".format(create_response))
                pass
            else:
                # Some error occured while creating the collection
                raise RuntimeError("Could not create collection. Received response: {}".format(create_response))

            schema_api = "{}nexustiles/schema".format(SDAP_SOLR_URL)

            field_type_payload = json.dumps({
                "add-field-type": {
                    "name": "geo",
                    "class": "solr.SpatialRecursivePrefixTreeFieldType",
                    "geo": "true",
                    "precisionModel": "fixed",
                    "maxDistErr": "0.000009",
                    "spatialContextFactory": "com.spatial4j.core.context.jts.JtsSpatialContextFactory",
                    "precisionScale": "1000",
                    "distErrPct": "0.025",
                    "distanceUnits": "degrees"}})

            logging.info("Creating field-type 'geo'...")
            field_type_response = requests.post(url=schema_api, data=field_type_payload)
            if field_type_response.status_code < 400:
                logging.info("Success.")
            else:
                logging.error("Error creating field type 'geo': {}".format(field_type_response.text))

            def add_field(schema_api, name, type):
                field_payload = json.dumps({
                    "add-field": {
                        "name": name,
                        "type": type}})
                logging.info(f"Creating {type} field '{name}'...")
                field_response = requests.post(url=schema_api, data=field_payload)
                if field_response.status_code < 400:
                    logging.info("Success.")
                else:
                    logging.error(f"Error creating field '{name}': {field_response.text}")

            add_field(schema_api, 'geo', 'geo')
            add_field(schema_api, 'tile_max_lat', 'pdouble')
            add_field(schema_api, 'tile_min_lat', 'pdouble')
            add_field(schema_api, 'tile_max_lon', 'pdouble')
            add_field(schema_api, 'tile_min_lon', 'pdouble')
            add_field(schema_api, 'tile_min_elevation_d', 'pdouble')
            add_field(schema_api, 'tile_max_elevation_d', 'pdouble')

finally:
    zk.stop()
    zk.close()

# We're done, do nothing forever.
logging.info("Done.")
while True:
    time.sleep(987654321)
