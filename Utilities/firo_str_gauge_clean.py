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
from pandas import set_option
from numpy import set_printoptions

# print options can be set long to see more array data at once
set_printoptions(threshold=3000, edgeitems=500)
set_option('display.height', 500)
set_option('display.max_rows', 500)

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

    # read in a csv with coordinates as dict with gauge numbers as keys,
    # this will be filled with gauge data below
    gauge_dict = csv_parser.csv_to_dict(gpath)

    # loop through all files in dirpath, reading gauge data and appending
    # to a dict of pandas dataframes
    for (dirpath, dirnames, filenames) in os.walk(root):

        # don't read in parent dirs
        # print 'working on {}, basename {}'.format(dirpath, os.path.basename(dirpath))
        if os.path.basename(dirpath) in ['output', 'statistics', 'usgs 11462125', 'tables', 'uncleaned',
                                         'figures', 'stream_gages', 'extras', 'CSVs', 'code', 'older']:

            print os.path.basename(dirpath), 'left off from data collection'

        elif not filenames:
            print 'empty filelist in {}'.format(dirpath)

        # these are the CLV and COY data which is in comma delimited
        # CLV, sensor, dateime(Y/m/day), data.hour00, data.hour01, etc
        elif dirpath in alt_dirs:
            print dirpath
            base = os.path.basename(dirpath)
            base += ' hourly'
            data = other_gauge_reader.read_gauge(dirpath, file_names=filenames)
            other_panels = df_generator.other_array_to_dataframe(data)
            gauge_dict[base].update({'Dataframe': other_panels})

        else:
            print dirpath
            # handle all USGS data tables, tab delimited
            recs, check, base = usgs_gauge_reader.read_gauge(dirpath, filenames=filenames)
            print check
            print ''

            # put lists of data into dataframes
            new_df = df_generator.usgs_array_to_dataframe(recs, base)
            gauge_dict[base].update({'Dataframe': new_df})

    # clean the data of erroneous values
    # can be set to clean less than and/or greater than 3-sigma
    # and/or can clean using a rolling condition defined in firo_pandas_utils.py
    clean_guage_dfs = df_generator.clean_dataframe(gauge_dict, clean_before_three_sigma=False,
                                                   impose_rolling_condition=False,
                                                   save_cleaned_df=False, save_path=csv_save)

    # plot simple time vs discharge, stage, etc
    # gauge_plotter.plot_discharge(clean_guage_dfs, save_figure=True, save_path=fig_save)

    # plot a horizontal bar for each gauge's time coverage
    gauge_plotter.plot_time_coverage_bar(clean_guage_dfs, stage_plot=True, zone='downstream', save_figure=False,
                                         save_path=fig_save, save_format='svg')

    # gauge_plotter.plot_hyd_subplots(clean_guage_dfs)

    # this isn't fully implemented, once we know what stats we need we can expand it
    # df_stats = df_generator.save_cleaned_stats(clean_guage_dfs, gauge_dict, csv_save, save_cleaned_states=True)

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
