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

from datetime import datetime
import os
from numpy import array
from gauge_reader import GaugeReader


class USGSGaugeReader(GaugeReader):
    delimiter = '\t'

    def __init__(self):
        pass

    def read_gauge(self, root, file_names):
        """

        :param root: root directory of input data
        :param file_names: list of files to read
        :return: 2-tuple or 4-tuple of lists

        """
        old_base = []
        recs = []
        abc = []
        for rows in self._get_table_rows(root, file_names):
            base = os.path.basename(root).replace('usgs ', '')
            if base != old_base:
                # print 'first file'
                for line in rows:
                    if line[0] in ['USGS', base]:
                        if line[2] in ['PST', 'PDT']:
                            recs.append([datetime.strptime(line[1], '%Y%m%d%H%M%S'), line[5]])
                            abc.append('a')
                        elif line[3] in ['PST', 'PDT']:
                            try:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                                abc.append('b')
                            except ValueError:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4]])
                                abc.append('x')

                        else:
                            try:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d'), line[3]])
                                abc.append('c')
                            except ValueError:
                                abc.append('w')

                    else:
                        abc.append('y')

            else:
                # print 'continuing time series'
                for line in rows:
                    if line[0] in ['USGS', base]:
                        try:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                            abc.append('d')
                        except ValueError:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4]])
                            abc.append('v')

                    else:
                        abc.append('z')

            old_base = base

        abc = array(['a: {}'.format(abc.count('a')), 'b: {}'.format(abc.count('b')), 'c: {}'.format(abc.count('c')),
                     'd: {}'.format(abc.count('d')), 'v: {}'.format(abc.count('v')), 'w: {}'.format(abc.count('w')),
                     'x: {}'.format(abc.count('x')), 'y: {}'.format(abc.count('y')), 'z: {}'.format(abc.count('z'))])

        return recs, abc

# ============= EOF =============================================
