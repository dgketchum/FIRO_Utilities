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


class USACEGaugeReader:

    def __init__(self):
        pass

    def read_usace_gauge(self, root, file_names):
        """

        :param root: root directory of input data
        :param file_names: list of files to read
        :return: 2-tuple or 4-tuple of lists

        """
        q_recs = []
        s_recs = []
        out_q = []
        stor = []

        for filename in file_names:

            if filename.endswith('.txt') and filename != 'readme.txt':

                rows = self._read_table_rows(root, filename)

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

        if not stor:
            data = (q_recs, s_recs)
            return data
        else:
            data = (q_recs, s_recs, stor, out_q)
            return data

    # private
    def _read_table_rows(self, root, name):
        with open(os.path.join(root, name), 'r') as rfile:
            return [line.split(',') for line in rfile]

    def _fill_hours(self, line):
        for xx in xrange(0, 24):
            if line[xx + 3] not in ['m', 'm\n']:
                day = datetime.strptime(line[2], '%Y%m%d')
                day_hour = day + timedelta(hours=xx)
                line_data = ([day_hour, line[xx + 3]])
                return line_data

    def __str__(self):
        return 'Calculator class'

# ============= EOF =============================================
