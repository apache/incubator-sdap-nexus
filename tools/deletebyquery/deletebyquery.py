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


import argparse
import json
import logging
import uuid
from random import sample

from cassandra.auth import PlainTextAuthProvider
import cassandra.concurrent
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from solrcloudpy import SolrConnection, SearchOptions

from six.moves import input

solr_connection = None
solr_collection = None
SOLR_UNIQUE_KEY = None
solr_delete_from_all_collections = None

cassandra_cluster = None
cassandra_session = None
cassandra_table = None

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().handlers[0].setFormatter(
    logging.Formatter(fmt="%(asctime)s %(levelname)s:%(name)s:  %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))

logging.getLogger('cassandra').setLevel(logging.CRITICAL)


def init(args):
    global solr_connection
    solr_connection = SolrConnection(args.solr)

    global solr_collection
    solr_collection = solr_connection[args.collection]

    global SOLR_UNIQUE_KEY
    SOLR_UNIQUE_KEY = args.solrIdField

    global solr_delete_from_all_collections
    solr_delete_from_all_collections = args.deleteFromAllCollections

    dc_policy = RoundRobinPolicy()
    token_policy = TokenAwarePolicy(dc_policy)

    if args.cassandraUsername and args.cassandraPassword:
        auth_provider = PlainTextAuthProvider(username=args.cassandraUsername, password=args.cassandraPassword)
    else:
        auth_provider = None

    global cassandra_cluster
    cassandra_cluster = Cluster(contact_points=args.cassandra, port=args.cassandraPort,
                                protocol_version=int(args.cassandraProtocolVersion),
                                load_balancing_policy=token_policy,
                                auth_provider=auth_provider)
    global cassandra_session
    cassandra_session = cassandra_cluster.connect(keyspace=args.cassandraKeyspace)

    global cassandra_table
    cassandra_table = args.cassandraTable


def delete_by_query(args):
    if args.query:
        se = SearchOptions()
        se.commonparams.q(args.query) \
            .fl(SOLR_UNIQUE_KEY) \
            .fl('id')

        for fq in args.filterquery if args.filterquery is not None else []:
            se.commonparams.fq(fq)

        query = se
    elif args.jsonparams:
        se = SearchOptions(**json.loads(args.jsonparams))
        se.commonparams.fl(SOLR_UNIQUE_KEY) \
            .fl('id')
        query = se
    else:
        raise RuntimeError("either query or jsonparams is required")

    query.commonparams.rows(args.rows)

    if check_query(query):
        logging.info("Collecting tiles ....")
        solr_docs = do_solr_query(query)

        if confirm_delete(len(solr_docs)):
            deleted_ids = do_delete(solr_docs, query)
            logging.info("Deleted tile IDs %s" % json.dumps([str(doc_id) for doc_id in deleted_ids], indent=2))
        else:
            logging.info("Exiting")
            return
    else:
        logging.info("Exiting")
        return


def confirm_delete(num_found):
    do_continue = input(
        "This action will delete %s record(s) from SOLR and Cassandra. Are you sure you want to Continue? y/n: " % num_found)

    while do_continue not in ['y', 'n']:
        do_continue = input(
            "This action will delete %s record(s) from SOLR and Cassandra. Are you sure you want to Continue? y/n: " % num_found)

    return do_continue == 'y'


def check_query(query):
    solr_response = solr_collection.search(query)

    num_found = solr_response.result.response.numFound

    if num_found == 0:
        logging.info("Query returned 0 results")
        return False

    do_continue = input("Query found %s matching documents. Continue? [y]/n/(s)ample: " % num_found)

    while do_continue not in ['y', 'n', 's', '']:
        do_continue = input("Query found %s matching documents. Continue? [y]/n/(s)ample: " % num_found)

    if do_continue == 'y' or do_continue == '':
        return True
    elif do_continue == 'n':
        return False
    else:
        se = SearchOptions()
        se.commonparams.q('%s:%s' % (SOLR_UNIQUE_KEY, sample(solr_response.result.response.docs, 1)[0][SOLR_UNIQUE_KEY]))
        logging.info(json.dumps(solr_collection.search(se).result.response.docs[0], indent=2))
        return check_query(query)


def do_solr_query(query):
    doc_ids = []

    next_cursor_mark = "*"
    query.commonparams.sort('%s asc' % SOLR_UNIQUE_KEY)
    while True:
        query.commonparams.remove_param('cursorMark')
        query.commonparams.add_params(cursorMark=next_cursor_mark)
        solr_response = solr_collection.search(query)

        try:
            result_next_cursor_mark = solr_response.result.nextCursorMark
        except AttributeError:
            # No Results
            return []

        if result_next_cursor_mark == next_cursor_mark:
            break
        else:
            next_cursor_mark = solr_response.result.nextCursorMark

        doc_ids.extend([uuid.UUID(doc['id']) for doc in solr_response.result.response.docs])

    return doc_ids


def do_delete(doc_ids, query):
    logging.info("Executing Cassandra delete...")
    delete_from_cassandra(doc_ids)
    logging.info("Executing Solr delete...")
    delete_from_solr(query)
    return doc_ids


def delete_from_cassandra(doc_ids):
    statement = cassandra_session.prepare("DELETE FROM %s WHERE tile_id=?" % cassandra_table)

    results = cassandra.concurrent.execute_concurrent_with_args(cassandra_session, statement,
                                                                [(doc_id,) for doc_id in doc_ids])

    for (success, result) in results:
        if not success:
            logging.warning("Could not delete tile %s" % result)


def delete_from_solr(query):
    global solr_collection
    if not solr_delete_from_all_collections:
        solr_collection.delete(query, commit=False)
        solr_collection.commit()
    else:
        for collection in dir(solr_connection):
            solr_collection = solr_connection[collection]
            solr_collection.delete(query, commit=False)
            solr_collection.commit()


def parse_args():
    parser = argparse.ArgumentParser(description='Delete data from NEXUS using a Solr Query',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--solr',
                        help='The url of the SOLR server.',
                        required=True,
                        metavar='127.0.0.1:8983')

    parser.add_argument('--collection',
                        help='The name of the SOLR collection.',
                        required=False,
                        default='nexustiles',
                        metavar='nexustiles')

    parser.add_argument('--solrIdField',
                        help='The name of the unique ID field for this collection.',
                        required=False,
                        default='id',
                        metavar='id')
    
    parser.add_argument('--deleteFromAllCollections',
                        help='Delete from all collections in SOLR, not limiting ' \
                             'to the collection specified in collection option',
                        required=False,
                        default=False,
                        action='store_true')

    parser.add_argument('--cassandra',
                        help='The hostname(s) or IP(s) of the Cassandra server(s).',
                        required=True,
                        nargs='+',
                        metavar=('127.0.0.100', '127.0.0.101'))

    parser.add_argument('-k', '--cassandraKeyspace',
                        help='The Cassandra keyspace.',
                        default='nexustiles',
                        required=False,
                        metavar='nexustiles')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-q', '--query',
                       help='The ''q'' parameter passed to SOLR Search',
                       metavar='*:*')

    group.add_argument('--jsonparams',
                       help='Full query prameters formatted as JSON')

    parser.add_argument('-fq', '--filterquery',
                        help='The ''fq'' parameter passed to SOLR Search. Only used if --jsonparams is not provided',
                        required=False,
                        nargs='+')

    parser.add_argument('-t', '--cassandraTable',
                        help='The name of the cassandra table.',
                        required=False,
                        default='sea_surface_temp')

    parser.add_argument('-p', '--cassandraPort',
                        help='The port used to connect to Cassandra.',
                        type=int,
                        required=False,
                        default=9042)

    parser.add_argument('--cassandraUsername',
                        help='The username used to connect to Cassandra.',
                        required=False)

    parser.add_argument('--cassandraPassword',
                        help='The password used to connect to Cassandra.',
                        required=False)

    parser.add_argument('-pv', '--cassandraProtocolVersion',
                        help='The version of the Cassandra protocol the driver should use.',
                        required=False,
                        choices=['1', '2', '3', '4', '5'],
                        default='3')

    parser.add_argument('--solr-rows',
                        help='Number of rows to fetch with each Solr query to build the list of tiles to delete',
                        required=False,
                        dest='rows',
                        default=1000,
                        type=int)

    return parser.parse_args()


if __name__ == "__main__":
    the_args = parse_args()
    init(the_args)
    delete_by_query(the_args)
