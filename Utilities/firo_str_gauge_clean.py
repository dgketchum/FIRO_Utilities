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
from Utilities.dictUtilities import CSVParser
from Utilities.gauge_reader import OtherGaugeReader, USGSGaugeReader
from Utilities.firo_pandas_utils import DataframeManagement
from Utilities.firo_gauge_plotter import PlotGauges


np.set_printoptions(threshold=3000, edgeitems=500)


def gauge_clean(root, alt_dirs, gpath):
    """
    describe what gauge_clean does

    :param alt_dirs:
    :param root:
    :return:
    """

    other_gauge_reader = OtherGaugeReader()
    df_generator = DataframeManagement()
    usgs_gauge_reader = USGSGaugeReader()
    gauge_plotter = PlotGauges()
    csv_parser = CSVParser()

    gauge_headers = ['StationID', 'Name', 'Latitude', 'Longitude']
    gauge_dict = csv_parser.csv_to_dict(gpath, headers=gauge_headers)

    df_dict = {}
    for (dirpath, dirnames, filenames) in os.walk(root):

        if os.path.basename(dirpath) in ['output', 'statistics', 'usgs 11462125', 'tables']:
            print os.path.basename(dirpath), 'left off from data collection'

        elif not filenames:
            print 'empty filelist in {}'.format(dirpath)

        elif dirpath in alt_dirs:
            pass
            # print dirpath
            # data = other_gauge_reader.read_gauge(dirpath, file_names=filenames)
            # other_panels = df_generator.other_array_to_dataframe(data)
            # base = os.path.basename(dirpath)
            #
            # if not df_dict:
            #     df_dict.update({base: other_panels})
            # else:
            #     df_dict.update({base: other_panels})
            # print df_dict

        else:
            base = os.path.basename(dirpath).replace('usgs ', '')
            if base == 'observations':
                parent = os.path.abspath(os.path.join(dirpath, os.pardir))
                base = os.path.basename(parent).replace('usgs ', '')
            print ''
            print base
            print dirpath

            recs, check = usgs_gauge_reader.read_gauge(dirpath, file_names=filenames)
            print check

            print recs
            new_df = df_generator.usgs_array_to_dataframe(recs, base)
            df_dict.update({base: new_df})
            print new_df

    clean_guage_dfs = df_generator.clean_dataframe(df_dict)

    gauge_plotter.plot_discharge(clean_guage_dfs)

    def data_to_shapefile(gauge_dict):
        pass

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    root = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gages', 'test')
    ad = [r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\CLV - Cloverdale',
          r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\COY - Coyote']
    alt_dirs = ad
    gpath = os.path.join(root, 'tables', 'FIRO_gaugeDict.csv')
    gauge_clean(root, ad, gpath)

# ============= EOF =============================================
