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
and to read in the guage data.

this module provides (1) function -- gauge_clean.
gauge_clean does all the work

dgketchum 1 JUL 2016
"""

import os
import numpy as np
from pandas import Panel
from Utilities.other_gauge_reader import OtherGaugeReader
from Utilities.firo_pandas_utils import PanelManagement
from Utilities.usgs_gauge_reader import USGSGaugeReader


# np.set_printoptions(threshold=3000, edgeitems=500)


def gauge_clean(root, alt_dirs):
    """
    describe what gauge_clean does

    :param root:
    :return:
    """

    other_gauge_reader = OtherGaugeReader()
    panel_generator = PanelManagement()
    usgs_gauge_reader = USGSGaugeReader()

    # this serves no purpose. gauge_dict is never used subsequently
    # gauge_headers = ['StationID', 'Name', 'Latitude', 'Longitude']
    # csv_parser = CSVParser()
    # gauge_dict = csv_parser.csv_to_dict(gauge_path, headers=gauge_headers)

    gauge_panels = None
    for (dirpath, dirnames, filenames) in os.walk(root):

        if os.path.basename(dirpath) in ['output', 'statistics', 'usgs 11462125', 'tables']:
            print os.path.basename(dirpath), 'left off from data collection'

        elif not filenames:
            print 'empty filelist in {}'.format(dirpath)

        elif dirpath in alt_dirs:

            print dirpath
            data = other_gauge_reader.read_gauge(dirpath, filenames)
            other_panels = panel_generator.other_array_to_dataframe(data)
            base = os.path.basename(dirpath)

            if not gauge_panels:
                df = {base: other_panels}
                print df
                gauge_panels = Panel.from_dict(gauge_panels, orient='items')
            else:
                print other_panels
                gauge_panels[base] = other_panels
                print gauge_panels

        else:
            base = os.path.basename(dirpath)
            print ''
            print dirpath

            recs, check = usgs_gauge_reader.read_gauge(dirpath, filenames)
            print check

            new_panel = panel_generator.usgs_array_to_dataframe(recs, base)
            print new_panel

            gauge_panels[base] = new_panel
            print gauge_panels


if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gages', 'test')
    ad = [r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\CLV - Russian River at Cloverdale',
          r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\COY - Coyote']
    # gpath = os.path.join(path, 'tables', 'FIRO_gaugeDict.csv')
    gauge_clean(path, ad)

# ============= EOF =============================================
