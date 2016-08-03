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

import csv
from datetime import datetime

from Utilities.gauge_reader import PrecipGaugeReader

class CSVParser:
    def __init__(self):
        pass

    def csv_to_dict(self, source_path, type='stream_gauges'):
        """
        converts  csv file to a dictionary

        :param source_path:
        :param type:
        :return:
        """
        print 'parsing to dict'
        delimiter = ','
        working_dict = {}
        result = {}
        with open(source_path, 'r') as data_file:
            data = csv.reader(data_file, delimiter=delimiter)
            headers = next(data)[1:]
            for row in data:
                temp_dict = {}
                id = row[0]
                values = []
                if type == 'stream_gauges':
                    name = '{} {}'.format(row[0], row[3])
                    for x in row[1:]:
                        # print row
                        try:
                            values.append(str(x))
                            # print str(x)
                        except ValueError:
                            try:
                                values.append(int(x))
                                # print int(x)
                            except ValueError:
                                try:
                                    values.append(float(x))
                                    # print float(x)
                                except ValueError:
                                    print("Skipping value '{}' that cannot be converted " +
                                          "to a number - see following row: {}"
                                          .format(x, delimiter.join(row)))
                                    values.append(0)
                elif type == 'floods':
                    name = 'Rank {}'.format(row[0])
                    print 'name: {}'.format(name)
                    for i, x in enumerate(row[1:]):
                        if i == 0:
                            values.append(datetime.strptime(x, '%m/%d/%Y'))
                        else:
                            values.append(int(x))

                for i in range(len(values)):
                    if values[i]:
                        temp_dict[headers[i]] = values[i]

                result[name] = temp_dict

                if type == 'stream_gauges':
                    temp_dict['Dataframe'] = None
                    temp_dict['Rating'] = None
                    temp_dict['ID'] = id
                elif type == 'floods':
                    pass
                working_dict.update({name: temp_dict})
        return working_dict

    def make_ppt_dict(self, ppt_path, alt_ppt_path, gauge_list):
        ppt_reader = PrecipGaugeReader()
        ppt_dict = {}
        for ppt_gauge in gauge_list:
            print 'precip gauge {}'.format(ppt_gauge)
            try:
                ppt, check = ppt_reader.read_in_precip_gauge(ppt_path, ppt_gauge)
            except IOError:
                ppt, check = ppt_reader.read_in_precip_gauge(alt_ppt_path, ppt_gauge)
            print 'length of precip list: {}'.format(len(ppt))
            ppt_dict.update({ppt_gauge.replace('.csv', ''): ppt})
        return ppt_dict

# ============= EOF =============================================
