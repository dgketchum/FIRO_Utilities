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
from pandas import set_option, Series, Timedelta, to_numeric
from numpy import set_printoptions, array, count_nonzero, isnan
from matplotlib import pyplot as plt
from datetime import datetime, timedelta

from Utilities.dictUtilities import CSVParser
from Utilities.firo_str_gauge_clean import gauge_clean


# print options can be set long to see more array data at once
# set_printoptions(threshold=3000, edgeitems=500)
# set_option('display.max_rows', 500)

ppt_list = ['pottervalleypowerhouse.csv', 'lakemendocinodam.csv', 'navarro1nw.csv']

ts_obs_gauges = (['11471105 daily', '11471106 daily', '11471099 daily', 'COY - Coyote hourly'],
                 ['COY - Coyote hourly', '11462500 15 minute', '11462080 15 minute', '11461000 15 minute'])

if __name__ != '__main__':
    pass
    # root
    # flood_path
    # alt_dirs
    # gpath
    # fig_save


def find_event(root, window=None, rank=None, all_by_bin=False, time_window=None, time_series_obs_tup=None):
    """Find event, gather data, organize data, clean data according to parameters set in 'gauge_data_clean.py'
    present hydrograph. Requires a csv at level of gauge folders with all guage names.

    :param root:
    :param rank:
    :return:
    """

    csv_parser = CSVParser()
    ppt_dict = csv_parser.make_ppt_dict(ppt_path, alt_ppt_path, ppt_list)

    if rank:
        flood_dict = csv_parser.csv_to_dict(flood_path, type_='floods')
        print 'Floods: {}'.format(flood_dict)

    gauge_dict = gauge_clean(root, alt_dirs, gpath, save_path=fig_save, window=time_window,
                             time_series_ob_tup=ts_obs_gauges)

    # this should eventually go into the plotting class
    def display_event(gauges, precip_dict, precip=True, hyeto_key='pottervalleypowerhouse',
                      save_fig=False, save_path=fig_save, date_range=None,
                      date_obj=None, buffer_days=10, save_format='.png'):

        if date_range:
            i, j = date_range
            buf = j - i
            buffer_days = buf.days
            date_obj = i + timedelta(days=buffer_days/2)

        def _standard_plotter(use_gauge, b_name, series, sbuffer, dtime_object):
            print 'corresponding stage hydrograph'
            plt.subplots(figsize=(30, 8))
            s = use_gauge[b_name]['Dataframe'][series]
            time_delta = Timedelta(days=sbuffer)
            rng = s[dtime_object - time_delta: dtime_object + time_delta]
            plt.plot(rng, 'g', label='Data')
            plt.tight_layout()
            plt.legend()
            if ser == 'Stage_ft':
                plt.title('Stage at {} Event Peak: {}'.format(use_gauge[b_name]['Name'],
                                                              datetime.strftime(dtime_object, '%Y/%m/%d')))
                plt.ylabel('[ft]')
            elif ser == 'Qout_cfs':
                plt.title('Outlet Discharge at {} Event Peak: {}'.format(use_gauge[b_name]['Name'],
                                                              datetime.strftime(dtime_object, '%Y/%m/%d')))
                plt.ylabel('[cfs]')
            plt.xlabel('Date')


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
            for ser in ['Q_cfs', 'Stage_ft', 'Qout_cfs']:
                if key == 'COY - Coyote hourly':
                    if ser == 'Q_cfs':
                        s = gauges[key]['Dataframe'][ser]
                        name = gauges[key]['Name']
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
                            ppt_s = ppt_s[
                                    date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
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
                                plt.savefig('{}\{}_COY_hanging{}'.format(save_path, gauges[key]['Name'], save_format), dpi=500)

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
                                plt.savefig('{}\{}{}'.format(save_path, gauges[key]['Name'], save_format), dpi=500)
                                plt.close()
                    else:
                        _standard_plotter(gauges, key, ser, buffer_days, date_obj)
                        if save_fig:
                            plt.savefig('{}\{}_{}{}'.format(save_path, gauges[key]['Name'], ser, save_format), dpi=500)
                            plt.close()
                else:
                    if ser == 'Q_cfs':
                        _standard_plotter(gauges, key, ser, buffer_days, date_obj)
                        if save_fig:
                            plt.savefig('{}\{}_{}{}'.format(save_path, gauges[key]['Name'], ser, save_format), dpi=500)
                            plt.close()

    if rank == 'all':
        for key, val in flood_dict.iteritems():
            print 'Date of Peak: {}...... Max Flow at Guernewood: {} cfs'.format(val['Date'],
                                                                                 int(val['Peak Flow [cms]'] * 35.3147))
            date = val['Date']
            print 'date: {}'.format(date)
            display_event(gauge_dict, precip=True, precip_dict=ppt_dict, date_obj=date, save_fig=True)
    elif rank:
        print rank
        date = flood_dict[rank]['Date']
        print 'date {}'.format(date)
        display_event(gauge_dict, precip=True, precip_dict=ppt_dict, date_obj=date, save_fig=True)
    if time_window:
        display_event(gauge_dict, precip=True, precip_dict=ppt_dict, date_range=time_window, save_fig=True)

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    firo_dir = os.path.join(home, 'Documents', 'USACE', 'FIRO')
    ppt_path = os.path.join(firo_dir, 'precipitation', 'FIRO_precip_coops', 'processed_15min')
    alt_ppt_path = os.path.join(firo_dir, 'precipitation', 'FIRO_precip_coops', 'processed_hourly')
    rating_path = os.path.join(firo_dir, 'rating_curves')
    alt_dirs = [r'C:\Users\David\Documents\USACE\FIRO\stream_gauges_test\coverage_check\hourly\COY - Coyote hourly']
    fig_save = os.path.join(firo_dir, 'meeting_2AUG16', '2005-12-29_2006-01-01')
    root = os.path.join(firo_dir, 'stream_gauges_test')
    flood_path = os.path.join(firo_dir, 'Events', 'FloodDict_Ralph_etal_2006.csv')
    gpath = os.path.join(root, 'coverage_check', 'FIRO_gaugeDict.csv')
    find_event(root, time_window=(datetime(2005, 12, 29), datetime(2006, 01, 01)),
               time_series_obs_tup=ts_obs_gauges)

# ==================================EOF=================================
