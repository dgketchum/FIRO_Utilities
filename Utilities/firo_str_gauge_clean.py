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
"""
The purpose of this module is to read in a csv with names and geographic coordinates of stream gauging stations,
and to read in the guage data. dgketchum 1 JUL 2016
"""
import os
import numpy as np
from pandas import Panel
from Utilities.other_gauge_reader import ReadOtherGauge
from Utilities.dictUtilities import CSVParser
from Utilities.firo_pandas_utils import PanelManagement
from Utilities.usgs_gauge_reader import ReadUSGSGauge

np.set_printoptions(threshold=3000, edgeitems=500)

home = os.path.expanduser('~')
print 'home: {}'.format(home)
path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gages', 'test')

csv_parser = CSVParser()
other_gauge_reader = ReadOtherGauge()
panel_generator = PanelManagement()
usgs_gauge_reader = ReadUSGSGauge()

gauge_path = os.path.join(path, 'tables', 'FIRO_gaugeDict.csv')
gauge_headers = ['StationID', 'Name', 'Latitude', 'Longitude']
gauge_dict = csv_parser.csv_to_dict(gauge_path, headers=gauge_headers)

os.chdir(path)

dfDict = {}

for (dirpath, dirnames, filenames) in os.walk(path):

    if os.path.basename(dirpath) in ['output', 'statistics', 'usgs 11462125', 'tables']:
        print os.path.basename(dirpath), 'left off from data collection'
        pass

    elif not filenames:
        print 'empty filelist in {}'.format(dirpath)
        pass

    elif dirpath in [r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\CLV - Russian River at Cloverdale',
                     r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\COY - Coyote']:

        print dirpath
        data = other_gauge_reader.read_other_gauge(dirpath, filenames)
        other_panels = panel_generator.other_array_to_dataframe(data)
        base = os.path.basename(dirpath)

        if dfDict == {}:
            dfDict.update({base: other_panels})
            print dfDict
            gauge_panels = Panel.from_dict(dfDict, orient='items')
        else:
            print other_panels
            gauge_panels[base] = other_panels
            print gauge_panels

    else:
        base = os.path.basename(dirpath)
        print ''
        print dirpath

        recs, check = usgs_gauge_reader.read_usgs_gauge(dirpath, filenames)
        print check

        new_panel = panel_generator.usgs_array_to_dataframe(recs, base)
        print new_panel

        gauge_panels[base] = new_panel
        print gauge_panels



# ============= EOF =============================================
