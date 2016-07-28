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
from Utilities.gauge_reader import PrecipGaugeReader
from Utilities.firo_str_gauge_clean import gauge_clean
from pandas import set_option, Series, Timedelta, to_numeric
from numpy import set_printoptions, array, count_nonzero, isnan
from matplotlib import pyplot as plt
from datetime import datetime

# print options can be set long to see more array data at once
# set_printoptions(threshold=3000, edgeitems=500)
# set_option('display.max_rows', 500)


def clean_hydrograph_signal(root, rank='Rank 1'):
    """Find event, gather data, organize data, clean data according to parameters set in 'gauge_data_clean.py'
    present hydrograph.

    :param root:
    :param rank:
    :return:
    """

    csv_parser = CSVParser()
    ppt_reader = PrecipGaugeReader()

    ppt_dict = {}
    ppt_list = ['pottervalleypowerhouse.csv', 'lakemendocinodam.csv', 'navarro1nw.csv', 'yorkville.csv',
                'w.howardforestrs.csv', 'cloverdale1s.csv']
    for ppt_gauge in ppt_list:
        print 'precip gauge {}'.format(ppt_gauge)
        try:
            ppt, check = ppt_reader.read_in_precip_gauge(ppt_path, ppt_gauge)
        except IOError:
            ppt, check = ppt_reader.read_in_precip_gauge(alt_ppt_path, ppt_gauge)
        print 'length of precip list: {}'.format(len(ppt))
        ppt_dict.update({ppt_gauge.replace('.csv', ''): ppt})

    flood_dict = csv_parser.csv_to_dict(flood_path, type='floods')
    print 'Floods: {}'.format(flood_dict)

    gauge_dict = gauge_clean(root, alt_dirs, gpath)

    # this should eventually go into the plotting class
    def select_known_events(gauges, precip_dict, precip=True, hyeto_key='pottervalleypowerhouse',
                            save_fig=False, save_path=fig_save,
                            date_obj=datetime(2006, 1, 1, 0, 0), buffer_days=10, save_format='.png'):

        def _standard_plotter(use_gauge, b_name, series, sbuffer, dtime_object):
            print 'corresponding stage hydrograph'
            plt.subplots(figsize=(30, 8))
            name = use_gauge.keys()[0]
            s = use_gauge[b_name]['Dataframe'][series]
            time_delta = Timedelta(days=sbuffer)
            rng = s[dtime_object - time_delta: dtime_object + time_delta]
            plt.plot(rng, 'g', label='Data')
            plt.tight_layout()
            plt.legend()
            plt.title('Stage at {} Event Peak: {}'.format(use_gauge[b_name]['Name'], datetime.strftime(dtime_object, '%Y/%m/%d')))
            plt.xlabel('Date')
            plt.ylabel('[ft]')

        for key, val in precip_dict.iteritems():
            if key != hyeto_key:
                print 'key: {} type: {}'.format(key, type(key))
                print 'val: {} type: {}'.format(val[:10], type(val))
                print 'surround precip guage hyetograph'
                s = gauges['COY - Coyote hourly']['Dataframe']['Q_cfs']
                time_delta = Timedelta(days=buffer_days)
                rng = s[date_obj - time_delta: date_obj + time_delta]
                plt.subplots(figsize=(30, 8))
                ppt_s = Series(array(val)[:, 1], index=array(val)[:, 0])
                print 'ppt_s is {} of type {}'.format(ppt_s.shape, type(ppt_s))
                ppt_s = ppt_s.reindex(index=rng.index, method=None)
                ppt_s = ppt_s.apply(to_numeric)
                ppt_s[ppt_s < 0] = 0.0
                ppt_s = ppt_s[date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
                # print 'ppt_s head: {}'.format(ppt_s.head(10))
                # print ''
                print 'ppt_s start: {}  ppt_s end: {}'.format(ppt_s.index[0], ppt_s.index[-1])
                if len(ppt_s) != count_nonzero(isnan(ppt_s)):
                    ppt_s.iloc[0], ppt_s.iloc[-1] = 0.0, 0.0
                    plt.bar(ppt_s.index, ppt_s, width=0.1, label='Precipitation [mm/hr]')
                else:
                    print 'lenghth of ppt array: {} count of nan: {}'.format(len(ppt_s),
                                                                             count_nonzero(isnan(ppt_s)))
                    fig = plt.figure(figsize=(30, 8))
                    ax = fig.add_subplot(111, autoscale_on=False, xlim=(-1, 5), ylim=(-3, 5))
                    ax.annotate('......THIS ARRAY HAS NO DATA.....', xy=(.5, .5), xycoords='axes fraction',
                                horizontalalignment='center', verticalalignment='center', fontsize=20)
                plt.tight_layout()
                plt.legend()
                plt.title('Precipitation at {} Event Peak: {}'.format(key, datetime.strftime(date_obj, '%Y/%m/%d')))
                plt.xlabel('Date')
                plt.ylabel('[ft]')
                if save_fig:
                    plt.savefig('{}\{}{}'.format(save_path, key, save_format), dpi=500)
                    plt.close()

        for key, val in gauges.iteritems():
            for ser in ['Q_cfs', 'Stage_ft']:
                if key == 'COY - Coyote hourly':
                    if ser == 'Q_cfs':
                        s = gauges[key]['Dataframe'][ser]
                        name = gauges.keys()[0]
                        time_delta = Timedelta(days=buffer_days)
                        rng = s[date_obj - time_delta: date_obj + time_delta]
                        if precip:
                            print 'plotting hyetograph and hydrograph response'
                            fig, ax1 = plt.subplots(figsize=(30, 8))
                            ax1.plot(rng, 'k', label='Discharge [cfs]')
                            ax1.legend()
                            plt.title('Discharge at {} Event Peak: {}'.format(name, datetime.strftime(date_obj,
                                                                                                      '%Y/%m/%d')))
                            plt.tight_layout()
                            ppt_s = Series(array(precip_dict[hyeto_key])[:, 1],
                                           index=array(precip_dict[hyeto_key])[:, 0])
                            ppt_s = ppt_s.reindex(index=rng.index, method=None)
                            ppt_s = ppt_s.apply(to_numeric)
                            ppt_s[ppt_s < 0] = 0.0
                            ppt_s = ppt_s[date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
                            print ''
                            print 'ppt_s start: {}  ppt_s end: {}'.format(ppt_s.index[0], ppt_s.index[-1])
                            max_ppt = ppt_s.max()
                            ax1.set_xlabel('Date')
                            ax1.set_ylabel('[cfs]')
                            for tl in ax1.get_yticklabels():
                                tl.set_color('k')
                            ax2 = ax1.twinx()
                            ax2.bar(ppt_s.index, ppt_s, width=0.1, label='Precipitation [mm/hr]')
                            ax = plt.gca()
                            ax.set_ylim([0, max_ppt * 2.0])
                            ax.invert_yaxis()
                            ax2.set_ylabel('[mm]')
                            for tl in ax2.get_yticklabels():
                                tl.set_color('b')
                            ax2.legend()
                            if save_fig:
                                plt.savefig('{}\{}{}'.format(save_path, ser, save_format), dpi=500)
                        if not precip:
                            print 'not precip_hyeto'
                            plt.plot(rng)
                            plt.tight_layout()
                            plt.plot(rng, 'k')
                            plt.legend()
                            plt.title('Discharge at {} Event Peak: {}'.format(name, datetime.strftime(date_obj,
                                                                                                      '%Y/%m/%d')))
                            plt.xlabel('Date')
                            plt.ylabel('[cfs]')
                            if save_fig:
                                plt.savefig('{}\{}{}'.format(save_path, ser, save_format), dpi=500)
                                plt.close()
                    else:
                        _standard_plotter(gauges, key, ser, buffer_days, date_obj)
                        if save_fig:
                            plt.savefig('{}\{}{}'.format(save_path, ser, save_format), dpi=500)
                            plt.close()
                else:
                    if ser == 'Q_cfs':
                        _standard_plotter(gauges, key, ser, buffer_days, date_obj)
                        if save_fig:
                            plt.savefig('{}\{}{}'.format(save_path, gauges[key]['Name'], save_format), dpi=500)
                            plt.close()

    if rank == 'all':
        for key, val in flood_dict.iteritems():
            print 'Date of Peak: {}...... Max Flow at Guernewood: {} cfs'.format(val['Date'],
                                                                                 int(val['Peak Flow [cms]'] * 35.3147))
            date = val['Date']
            print 'date: {}'.format(date)
            select_known_events(gauge_dict, precip=True, precip_dict=ppt_dict, date_obj=date, save_fig=True)
    elif rank:
        print rank
        date = flood_dict[rank]['Date']
        print 'date {}'.format(date)
        select_known_events(gauge_dict, precip=True, precip_dict=ppt_dict, date_obj=date, save_fig=True)
    else:
        select_known_events(gauge_dict, precip=True, precip_dict=ppt_dict, save_fig=True)

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    rating_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'rating_curves')
    ppt_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'precipitation', 'FIRO_precip_coops',
                            'processed_15min')
    alt_ppt_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'precipitation', 'FIRO_precip_coops',
                                'processed_hourly')
    alt_dirs = [r'C:\Users\David\Documents\USACE\FIRO\stream_gauges_test\coverage_check\hourly\COY - Coyote hourly']
    fig_save = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'meeting_26JUL16')
    root = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gauges_test')
    flood_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'Events',
                              'FloodDict_Ralph_etal_2006.csv')
    gpath = os.path.join(root, 'coverage_check', 'FIRO_gaugeDict.csv')
    clean_hydrograph_signal(root, rank='Rank 7')

#  ==================================EOF=================================
