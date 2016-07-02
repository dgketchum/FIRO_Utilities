# ===============================================================================
# Copyright 2016 dgketchum
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

from datetime import datetime, timedelta
import os
from numpy import array, column_stack, transpose


class GaugeReader:

    _comma_delimiter = ','
    _tab_delimiter = '\t'

    def __init__(self):
        pass

    def read_gauge(self, root, filenames):
        raise NotImplementedError

    # private
    def _read_gauge(self, root, filenames):
        raise NotImplementedError

    def _read_table_rows(self, root, name):
        with open(os.path.join(root, name), 'r') as rfile:
            try:
                print 'comma delimited file'
                return [line.rstrip().split(self._comma_delimiter) for line in rfile]
            except ValueError:
                print 'tab delimited file'
                return [line.rstrip().split(self._tab_delimiter) for line in rfile]

    def _get_table_rows(self, root, fns):
        return (self._read_table_rows(root, fi) for fi in fns if fi.endswith('.txt') and fi != 'readme.txt')

# ============= EOF =============================================
