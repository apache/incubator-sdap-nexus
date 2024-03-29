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

from datetime import datetime
import pytz
import calendar


def normalize_date(time_in_seconds):
    dt = datetime.utcfromtimestamp(time_in_seconds)
    normalized_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)
    return int(calendar.timegm(normalized_dt.timetuple()))
