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

import sys
import traceback
import json
import logging

logger = logging.getLogger(__name__)


class NexusJSONRenderer(object):
    def __init__(self, nexus_request):
        self.request = nexus_request

    def render(self, tornado_handler, result):
        logger.info('Rendering JSON result')

        tornado_handler.set_header("Content-Type", "application/json")
        try:
            result_str = result.toJson()

            if isinstance(result_str, bytes):
                tornado_handler.set_header("Content-Type", "application/gzip")
                tornado_handler.set_header("Content-Disposition",
                                           "attachment; filename=\"%s\"" % self.request.get_argument('filename',
                                                                                                     "subset.gz"))
            logger.info('Writing result')

            tornado_handler.write(result_str)
            tornado_handler.finish()
        except AttributeError:
            traceback.print_exc(file=sys.stdout)
            tornado_handler.write(json.dumps(result, indent=4))
            tornado_handler.finish()
