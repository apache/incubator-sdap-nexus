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

from webservice.webmodel import NexusProcessingException

class NexusRendererFactory(object):
    content_types = ["CSV", "JSON", "XML", "PNG", "NETCDF", "ZIP", "GIF"]
    module = __import__(__name__)

    @classmethod
    def get_renderer(cls, request):
        content_type = request.get_content_type().upper()
        if content_type in cls.content_types:
            renderer_name = 'Nexus' + content_type + 'Renderer'
            renderer = getattr(cls.module.nexus_tornado.request.renderers, renderer_name)
            return renderer(request)
        else:
            raise NexusProcessingException(
                reason=f'Invalid output format {content_type}', code=400
            )





