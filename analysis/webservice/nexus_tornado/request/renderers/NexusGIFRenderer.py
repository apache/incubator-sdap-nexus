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
from webservice.webmodel import NexusProcessingException


class NexusGIFRenderer(object):
    def __init__(self, nexus_request):
        self._request = nexus_request

    def render(self, tornado_handler, result):
        tornado_handler.set_header("Content-Type", "image/gif")
        try:
            tornado_handler.write(result.toGif())
            tornado_handler.finish()
        except AttributeError:
            traceback.print_exc(file=sys.stdout)
            raise NexusProcessingException(reason="Unable to convert results to a GIF.")