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


class CSVParser:
    def __init__(self):
        pass

    def csv_to_dict(self, source_path, headers=None,  has_header=True):
        """
        converts  csv file to a dictionary

        :param source_path:
        :param headers:
        :param has_header:
        :return:
        """
        import csv
        from collections import defaultdict
        project = defaultdict(dict)
        with open(source_path, 'rb') as fp:
            if has_header:
                fp.next()
            reader = csv.DictReader(fp, fieldnames=headers, dialect='excel',
                                    skipinitialspace=True)
            for rowdict in reader:
                try:
                    station_id = rowdict['StationID']
                    name = rowdict['Name']
                except KeyError:
                    continue

                project[station_id][name] = rowdict

        return project

# ============= EOF =============================================
