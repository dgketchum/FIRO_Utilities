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

import os
from Utilities.dictUtilities import CSVParser
from Utilities.gauge_reader import OtherGaugeReader, PrecipGaugeReader, USGSGaugeReader
from Utilities.gauge_data_clean import DataframeManagement
from Utilities.firo_gauge_plotter import PlotGauges
from pandas import set_option, Series, Timedelta, to_datetime, to_numeric, rolling_window
from numpy import set_printoptions, array
from matplotlib import pyplot as plt

# print options can be set long to see more array data at once
set_printoptions(threshold=3000, edgeitems=500)
set_option('display.height', 500)
set_option('display.max_rows', 500)


def clean_hydrograph_signal(root, rank='Rank 1'):
    """Find event, gather data, organize data, clean data according to parameters set in 'gauge_data_clean.py'
    present hydrograph.

    :param root:
    :param rank:
    :return:
    """

    other_gauge_reader = OtherGaugeReader()
    df_generator = DataframeManagement()
    gauge_plotter = PlotGauges()
    csv_parser = CSVParser()
    ppt_reader = PrecipGaugeReader()
    usgs_gauge_reader = USGSGaugeReader()

    ppt, check = ppt_reader.read_in_precip_gauge(ppt_path, 'pottervalleypowerhouse.csv')

    flood_dict = csv_parser.csv_to_dict(flood_path, type='floods')
    print 'Floods: {}'.format(flood_dict)

    gauge_dict = {}

    for (dirpath, dirnames, filenames) in os.walk(root):
        # other gauges
        if os.path.basename(dirpath) in []:
            base = os.path.basename(dirpath)
            data = other_gauge_reader.read_gauge(dirpath, filenames)
            data = df_generator.other_array_to_dataframe(data)
            # usgs gauges
        elif dirpath == 'C:\Users\David\Documents\USACE\FIRO\stream_gauges_test\\15 minute\usgs 11461500':
            print 'found usgs gauge'
            recs, check, base = usgs_gauge_reader.read_gauge(dirpath, filenames=filenames)
            new_df = df_generator.usgs_array_to_dataframe(recs, base)
            gauge_dict.update({base: new_df})
            data = df_generator.clean_dataframe(gauge_dict, single_gauge=True, impose_rolling_condition=True,
                                                rolling_window=5)

    # if rank == 'all':
    #     dates
    # date = flood_dict[rank]['Date']
    def plot_window(gauge_dict, base, date, delta_type='days', time_buffer=20):
        s = data[base]['Q_cfs']
        name = gauge_dict.keys()[0]
        if delta_type == 'days':
            rng = s[date - Timedelta(days=time_buffer): date + Timedelta(days=time_buffer)]
        elif delta_type == 'hours':
            rng = s[date - Timedelta(days=time_buffer): date + Timedelta(days=time_buffer)]
        plt.plot(rng, 'k')
        plt.legend()
        plt.title('RAW DATA -- Discharge at {} Event Peak: {}'.format(name, date))
        plt.xlabel('Date')
        plt.ylabel('[cfs]')

    # this should eventually go into the plotting class
    def select_known_events(gauge_dict, base, precipitation, precip=False, date='2006-01-01', buffer_days=25):
        s = data[base]['Q_cfs']
        name = gauge_dict.keys()[0]
        date_obj = to_datetime(date)
        rng = s[date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
        if precip:
            fig, ax1 = plt.subplots()
            ax1.plot(rng, 'k', label='Discharge [cfs]')
            ax1.legend()
            plt.title('Discharge at {} Event Peak: {}'.format(name, date))
            ppt_s = Series(array(precipitation)[:, 1], index=array(precipitation)[:, 0])
            ppt_s = ppt_s.reindex(index=rng.index, method=None)
            ppt_s = ppt_s.apply(to_numeric)
            ppt_s[ppt_s < 0] = 0.0
            ppt_s = ppt_s[date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
            ax1.set_xlabel('Date')
            ax1.set_ylabel('[cfs]')
            for tl in ax1.get_yticklabels():
                tl.set_color('k')
            ax2 = ax1.twinx()
            ax2.bar(ppt_s.index, ppt_s, width=0.1, label='Precipitation [mm/hr]')
            plt.gca().invert_yaxis()
            ax2.set_ylabel('[mm]')
            for tl in ax2.get_yticklabels():
                tl.set_color('b')
            ax2.legend()
        if not precip:
            plt.plot(rng)
            plt.plot(rng, 'k')
            plt.legend()
            plt.title('Discharge at {} Event Peak: {}'.format(name, date))
            plt.xlabel('Date')
            plt.ylabel('[cfs]')

    select_known_events(data, base, precipitation=ppt)
    plot_window(gauge_dict, base, date)

    rng = data[base]['Q_cfs']
    interp = rng.interpolate(method='linear')
    hamming = rng.rolling(min_periods=5, window=100, win_type='hamming').mean()
    plt.plot(rng, 'g', label='Data')
    # plt.plot(interp, 'b', label='Linear Interpolation')
    # plt.plot(hamming, 'r', label='Hamming')
    plt.legend()
    plt.title('Rolling Median  -- Flow Past Coal Creek')
    plt.xlabel('Date')
    plt.ylabel('[cfs]')
    plt.show()

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    ppt_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'precipitation', 'FIRO_precip_coops',
                            'processed_hourly')
    root = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gauges_test')
    flood_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'Events',
                              'FloodDict_Ralph_etal_2006.csv')
    clean_hydrograph_signal(root, rank='Rank 1')

 #  ==================================EOF=================================
