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
from numpy import array, column_stack


# parent
class GaugeReader:
    def __init__(self):
        self._delimiter = ','

    def read_gauge(self, root, filenames):
        raise NotImplementedError

    def _read_table_rows(self, root, name):
        with open(os.path.join(root, name), 'r') as rfile:

            # if self is a GaugeReader or OtherGaugeReader then self._delimiter == ','
            # if self is a USGSGaugeReader self._delimiter = '\t'

            return [line.rstrip().split(self._delimiter) for line in rfile]

    def _get_table_rows(self, root, fns):
        return [self._read_table_rows(root, fi) for fi in fns if fi.endswith('.txt') and fi != 'readme.txt']


# child
class OtherGaugeReader(GaugeReader):
    def read_gauge(self, root, file_names):
        """

        :param root: root directory of input data
        :param file_names: list of files to read
        :return: 2-tuple or 4-tuple of numpy arrays

        """
        q_recs = []
        s_recs = []
        out_q = []
        stor = []

        for rows in self._get_table_rows(root, file_names):
            for line in rows:
                # '20'  discharge, '76' reservoir inflow
                if line[0] in ['CLV', 'COY'] and line[1] in ['20', '76']:
                    q_recs.append(self._fill_hours(line))

                # '1' is river stage, '6' reservoir stage
                if line[0] in ['CLV', 'COY'] and line[1] in ['1', '6']:
                    s_recs.append(self._fill_hours(line))

                if line[0] == 'COY' and line[1] == '15':  # '15' is res. storage
                    stor.append(self._fill_hours(line))

                if line[0] == 'COY' and line[1] == '23':  # '23' is res. outflow
                    out_q.append(self._fill_hours(line))

        q_recs = [item for sublist in q_recs for item in sublist]
        s_recs = [item for sublist in s_recs for item in sublist]

        if not stor:
            q_data, s_data = array(q_recs), array(s_recs)
            q_data, s_data = column_stack(q_data), column_stack(s_data)
            q_data, s_data = q_data.transpose(), s_data.transpose()
            return q_data, s_data
        else:
            stor = [item for sublist in stor for item in sublist]
            out_q = [item for sublist in out_q for item in sublist]
            q_data, s_data, stor_data, out_q_data = array(q_recs), array(s_recs), array(stor), array(out_q)
            return q_data, s_data, stor_data, out_q_data

    # private
    def _fill_hours(self, line):
        line_data = []
        for xx in xrange(0, 24):
            if line[xx + 3] not in ['m', 'm\n']:
                day = datetime.strptime(line[2], '%Y%m%d')
                day_hour = day + timedelta(hours=xx)
                line_data.append([day_hour, line[xx + 3]])
        return line_data


# child I inherit attributes and methods from GaugeReader
class USGSGaugeReader(GaugeReader):

    def __init__(self):
        GaugeReader.__init__(self)
        self._delimiter = '\t'

    def read_gauge(self, root, filenames):
        start = datetime.now()
        old_base = []
        recs = []
        abc = []
        for rows in self._get_table_rows(root, filenames):
            base = os.path.basename(root).replace('usgs ', '')
            if base != old_base:
                # print 'first file'
                for line in rows:
                    if line[0] in ['USGS', base]:
                        if line[2] in ['PST', 'PDT']:
                            recs.append([datetime.strptime(line[1], '%Y%m%d%H%M%S'), line[5]])
                        elif line[3] in ['PST', 'PDT']:
                            try:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                                abc.append('a')
                            except ValueError:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4]])
                                abc.append('b')

                        else:
                            try:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d'), line[3]])
                                abc.append('c')
                            except ValueError:

                                try:
                                    recs.append([datetime.strptime(line[2], '%Y-%m-%d'), line[3]])
                                    abc.append('d')
                                except ValueError:
                                    abc.append('x')

                    else:
                        abc.append('y')

            else:
                # print 'continuing time series'
                for line in rows:
                    if line[0] in ['USGS', base]:
                        try:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                            abc.append('e')
                        except ValueError:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4]])
                            abc.append('f')

                    else:
                        abc.append('z')

            old_base = base

        end = datetime.now()
        elapsed = end - start
        print 'time for usgs read gauge on {} was {}'.format(base, elapsed)

        nabc = array(['{}: {}'.format(attr, abc.count(attr)) for attr in 'abcdefxyz'])
        return recs, nabc

# ============= EOF =============================================
