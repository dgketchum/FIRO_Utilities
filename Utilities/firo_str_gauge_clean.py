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
from Utilities.dictUtilities import CSVParser
from Utilities.gauge_reader import OtherGaugeReader, USGSGaugeReader
from Utilities.gauge_data_clean import DataframeManagement
from Utilities.firo_gauge_plotter import PlotGauges

# print options can be set long to see more array data at once
# np.set_printoptions(threshold=3000, edgeitems=500)
# set_option('display.height', 500)
# set_option('display.max_rows', 500)

def gauge_clean(root, alt_dirs, gpath):
    """ Takes various types of gauge data files in .txt format and converts them to hydrographs (e.g. flow vs. time)

    :param alt_dirs:
    :param root:
    :return:
    """

    # instantiate the reader classes
    other_gauge_reader = OtherGaugeReader()
    df_generator = DataframeManagement()
    usgs_gauge_reader = USGSGaugeReader()
    gauge_plotter = PlotGauges()
    csv_parser = CSVParser()

    # read in a csv with coordinates as dict with gauge numbers as keys, to perhaps automatically
    # create a .shp with png or svg point symbols showing data/metadata
    gauge_headers = ['StationID', 'Name', 'Latitude', 'Longitude']
    gauge_dict = csv_parser.csv_to_dict(gpath, headers=gauge_headers)

    # loop through all files in dirpath, reading gauge data and appending
    # to a dict of pandas dataframes
    df_dict = {}
    for (dirpath, dirnames, filenames) in os.walk(root):

        # don't read in parent dirs
        print 'working on {}, basename {}'.format(dirpath, os.path.basename(dirpath))
        if os.path.basename(dirpath) in ['output', 'statistics', 'usgs 11462125', 'tables',
                                         'figures', 'stream_gages', 'extras', 'CSVs']:

            print os.path.basename(dirpath), 'left off from data collection'

        elif not filenames:
            print 'empty filelist in {}'.format(dirpath)

        # these are the CLV and COY data which is in comma delimited
        # CLV, sensor, dateime(Y/m/day), data.hour00, data.hour01, etc
        elif dirpath in alt_dirs:
            print dirpath
            data = other_gauge_reader.read_gauge(dirpath, file_names=filenames)
            other_panels = df_generator.other_array_to_dataframe(data)
            base = os.path.basename(dirpath)

            # append data to dict of dfs
            if not df_dict:
                df_dict.update({base: other_panels})
            else:
                df_dict.update({base: other_panels})
            # print df_dict

        else:
            # handle all USGS data tables, tab delimited
            base = os.path.basename(dirpath).replace('usgs ', '')
            if base == 'observations':
                parent = os.path.abspath(os.path.join(dirpath, os.pardir))
                base = os.path.basename(parent).replace('usgs ', '')

            print ''
            print 'base: {}'.format(base)
            print 'dirpath: {}'.format(dirpath)

            recs, check = usgs_gauge_reader.read_gauge(dirpath, filenames=filenames)
            print check

            # put lists of data into dataframes
            new_df = df_generator.usgs_array_to_dataframe(recs, base)
            df_dict.update({base: new_df})

    # clean the data of erroneous values
    # can be set to clean less than and/or greater than 3-sigma
    # and/or can clean using a rolling condition defined in firo_pandas_utils.py
    clean_guage_dfs = df_generator.clean_dataframe(df_dict, save_cleaned_df=True, save_path=csv_save)
    # print clean_guage_dfs

    # plot simple time vs discharge, stage, etc
    gauge_plotter.plot_discharge(clean_guage_dfs, save_figure=True, save_path=fig_save)

    # plot a horizontal bar for each gauge's time coverage
    gauge_plotter.plot_time_coverage_bar(clean_guage_dfs, save_figure=True, save_path=fig_save)

    # (not done) apply data plots or stats to .shp attribute table for display in GIS or cartography
    def data_to_shapefile(gauge_dict):
        pass

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    root = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gages')
    ad = [r'C:\Users\David\Documents\USACE\FIRO\stream_gages\hourly\CLV - Cloverdale',
          r'C:\Users\David\Documents\USACE\FIRO\stream_gages\hourly\COY - Coyote']
    fig_save = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gages', 'extras', 'figures')
    csv_save = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gages', 'extras', 'CSVs')
    alt_dirs = ad
    gpath = os.path.join(root, 'FIRO_gaugeDict.csv')
    gauge_clean(root, ad, gpath)

# ============= EOF =============================================
