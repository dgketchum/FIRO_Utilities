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
from numpy import array, column_stack, nan


class GaugeReader(object):
    def __init__(self):
        self._delimiter = ','

    def read_gauge(self, root, filenames):
        raise NotImplementedError

    def _read_table_rows(self, root, name):
        with open(os.path.join(root, name), 'r') as rfile:
            return [line.rstrip().split(self._delimiter) for line in rfile]

    def _get_table_rows(self, root, fns):
        return [self._read_table_rows(root, fi) for fi in fns if fi.endswith('.txt') and fi != 'readme.txt']


class OtherGaugeReader(GaugeReader):
    def read_gauge(self, root, file_names):
        """  Reads the alternate gauges (i.e. CA and USACE) located near (or at) L. Mendecino

        :param root: root directory of input data
        :param file_names: list of files to read
        :return: 2-tuple or 4-tuple of numpy arrays

        """
        # initiate empty lists
        q_recs = []
        s_recs = []
        out_q = []
        stor = []

        # read rows of CLV and COY-type data into lists (of daily lists)
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

        # transfor list of daily lists into one list
        q_recs = [item for sublist in q_recs for item in sublist]
        s_recs = [item for sublist in s_recs for item in sublist]

        # no storage or out_q for CLV
        if not stor:
            q_data, s_data = array(q_recs), array(s_recs)
            q_data, s_data = column_stack(q_data), column_stack(s_data)
            q_data, s_data = q_data.transpose(), s_data.transpose()
            return q_data, s_data
        # COY has all four fields
        else:
            stor = [item for sublist in stor for item in sublist]
            out_q = [item for sublist in out_q for item in sublist]
            q_data, s_data, stor_data, out_q_data = array(q_recs), array(s_recs), array(stor), array(out_q)
            return q_data, s_data, stor_data, out_q_data

    # private
    # fill hours runs through the 24 hours in each line of the CLV and COY-type data, returns list
    def _fill_hours(self, line):
        line_data = []
        for xx in xrange(0, 24):
            if line[xx + 3] not in ['m', 'm\n']:  # no-data is 'm', plus newline char at 24th hour
                day = datetime.strptime(line[2], '%Y%m%d')
                day_hour = day + timedelta(hours=xx)
                line_data.append([day_hour, line[xx + 3]])
        return line_data


class USGSGaugeReader(GaugeReader):

    def __init__(self):
        # change delimiter to tab
        super(USGSGaugeReader, self).__init__()
        self._delimiter = '\t'

    def read_gauge(self, root, filenames):
        """  Reads the USGS gauges in and around FIRO watersheds (i.e. Russian River and tribs)

        :param root: root directory of input data
        :param filenames: list of files to read
        :return: 2-tuple or 4-tuple of numpy arrays

        """
        start = datetime.now()  # time process
        recs = []
        abc = []
        file_ct = 0
        x = 0
        for rows in self._get_table_rows(root, filenames):
            base = self._get_base(root)
            for line in rows:
                if line[0] in ['USGS', base[:8]]:
                    if base in ['11461000 15 minute', '11461500 15 minute', '11462000 15 minute',
                                '11462500 15 minute']:
                        if file_ct == 0:
                            recs.append([datetime.strptime(line[1], '%Y%m%d%H%M%S'), line[5], nan])
                            abc.append('a')
                        else:
                            try:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                                abc.append('b')
                            except ValueError:
                                abc.append('y')
                            except IndexError:
                                try:
                                    recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], nan])
                                    abc.append('b')
                                except IndexError:
                                    if x < 10:
                                        print line
                                        x += 1
                                    abc.append('x')

                    elif base == '11462080 15 minute':
                        try:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[6], line[4]])
                            abc.append('c')
                        except IndexError:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), nan, nan])
                            abc.append('x')

                    elif base in ['11460940 daily', '11461400 daily', '11461501 daily', '11462080 daily',
                                  '11471000 daily', '11471099 daily', '11471100 daily', '11471105 daily',
                                  '11471106 daily', '11461000 daily']:
                        try:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d'), line[3], nan])
                            abc.append('d')
                        except IndexError:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d'), nan, nan])
                            abc.append('x')

                    elif base == '11462000 daily':
                        try:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d'), line[9], nan])
                            abc.append('e')
                        except IndexError:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d'), nan, nan])
                            abc.append('x')

                    elif base == '11462500 daily':
                        try:
                            recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[6], line[8]])
                            abc.append('f')
                        except ValueError:
                            abc.append('y')
            file_ct += 1
        end = datetime.now()
        elapsed = end - start
        if recs:
            print 'time for usgs read gauge on {} was {}'.format(base, elapsed)
            nabc = array(['{}: {}'.format(attr, abc.count(attr)) for attr in 'abcdefxy'])
            return recs, nabc, base

    def _get_base(self, root):
        base = os.path.basename(root).replace('usgs ', '')
        if base in 'observations':
            parent = os.path.abspath(os.path.join(root, os.pardir))
            freq = os.path.abspath(os.path.join(parent, os.pardir))
            freq = os.path.basename(freq)
            base = os.path.basename(parent).replace('usgs ', '')
            base = '{} {}'.format(base, freq)
        else:
            parent = os.path.abspath(os.path.join(root, os.pardir))
            freq = os.path.basename(parent)
            base = '{} {}'.format(base, freq)
        print 'base: {}'.format(base)
        return base


class PrecipGaugeReader(GaugeReader):

    def read_in_precip_gauge(self, root, ppt_file):
        print 'read in precip'
        first = True
        recs = []
        abc = []
        for i, row in enumerate(self._read_table_rows(root, ppt_file)):
            # if i < 20:
            #     print row
            if first:
                first = False
            else:
                date = str(row[0]) + str(row[1]) + str(row[2]) + str(row[3]) + str(row[4])
                recs.append([datetime.strptime(date, '%Y%m%d%H%M'), row[5]])
                abc.append('a')
        nabc = array(['{}: {}'.format(attr, abc.count(attr)) for attr in 'ax'])
        return recs, nabc


class RatingCurveReader(GaugeReader):

    def __init__(self):
        # change delimiter to tab
        super(RatingCurveReader, self).__init__()
        self._delimiter = '\t'

    def read_gauge_rating(self, root, name):
        print 'read in rating curve'
        recs = []
        for i, row in enumerate(self._read_table_rows(root, name)):
            try:
                recs.append([float(row[0]), float(row[1]), float(row[2])])
            except IndexError:
                pass
            except ValueError:
                pass
        recs = array(recs)
        return recs


# ============= EOF =============================================
