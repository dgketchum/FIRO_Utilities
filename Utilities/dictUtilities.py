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

class CSVParser:
    def __init__(self):
        pass

    def csv_to_dict(self, source_path):
        """
        converts  csv file to a dictionary

        :param source_path:
        :param headers:
        :param has_header:
        :return:
        """
        print 'parsing gauges to dict'
        delimiter = ','
        gauges = {}
        result = {}
        with open(source_path, 'r') as data_file:
            data = csv.reader(data_file, delimiter=delimiter)
            headers = next(data)[1:]
            for row in data:
                temp_dict = {}
                name = '{} {}'.format(row[0], row[3])
                id = row[0]
                values = []
                for x in row[1:]:
                    try:
                        values.append(str(x))
                    except ValueError:
                        try:
                            values.append(int(x))
                        except ValueError:
                            try:
                                values.append(float(x))
                            except ValueError:
                                print("Skipping value '{}' that cannot be converted " +
                                      "to a number - see following row: {}"
                                      .format(x, delimiter.join(row)))
                                values.append(0)

                for i in range(len(values)):
                    if values[i]:
                        temp_dict[headers[i]] = values[i]
                result[name] = temp_dict
                temp_dict['Dataframe'] = None
                temp_dict['ID'] = id
                gauges.update({name: temp_dict})
        return gauges

# ============= EOF =============================================
