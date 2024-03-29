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

from webservice.webmodel.StandardNexusErrors import StandardNexusErrors


class NexusProcessingException(Exception):
    def __init__(self, error=StandardNexusErrors.UNKNOWN, reason="", code=500):
        self.error = error
        self.reason = reason
        self.code = code
        Exception.__init__(self, reason)


class NoDataException(NexusProcessingException):
    def __init__(self, reason="No data found for the selected timeframe"):
        NexusProcessingException.__init__(self, StandardNexusErrors.NO_DATA, reason, 400)


class DatasetNotFoundException(NexusProcessingException):
    def __init__(self, reason="Dataset not found"):
        NexusProcessingException.__init__(self, StandardNexusErrors.DATASET_MISSING, reason, code=404)