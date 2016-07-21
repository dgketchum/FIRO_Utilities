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


import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from numpy import count_nonzero, isnan, linspace, nan
from pandas import Series, options
from copy import deepcopy
# options.mode.chained_assignment = None

class PlotGauges:

    def __init__(self):
        self._col_list = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']
        self._title_list = ['Discharge at Gauge', 'Stage at Gauge', 'Discharge from COYOTE DAM', 'Storage at COYOTE DAM']
        self._desc_list = ['Discharge [cfs]', 'Stage [ft]', 'Discharge [cfs]', 'Storage [af]']
        self._downstream_gauges = ['CLV - Cloverdale hourly', '11462080 daily', '11462080 15 minute',
                                   '11462500 daily', '11462500 15 minute']
        self._upstream_gauges = ['11460940 daily', '11461000 15 minute', '11461000 daily']
        self._potter_gauges = ['11471000 daily', '11471099 daily', '11471100 daily', '11471105 daily',
                               '11471106 daily']
        self._critical_gauges = ['CLV - Cloverdale hourly', '11462500 15 minute', '11462080 15 minute',
                                 '11461000 15 minute', 'COY - Coyote hourly', '11461500 15 minute',
                                 '11471000 daily']
        self._hopland = '11462500 15 minute'
    def plot_discharge(self, data, save_path=None, save_figure=False, save_format='png'):
        """Plot typical time vs discharge, etc hydrographs

        :param data: 2-tuple or 4-tuple of numpy arrays
        :param save_path: csv save location
        :param save_figure:
        :param save_format
        :return: show and/or save hydrographs of various types (stream flow, stage, etc)

        """
        x = 0
        print data
        for key in data:
            print key
            if key == '11462125 peak':
                print 'skipping peak discharge df'
            elif key == 'CLV - Cloverdale hourly':
                for col in self._col_list[:2]:
                    ser = data[key]['Dataframe'][col]
                    if not ser.empty:
                        self._setup_different_hydrographs(ser, col, key)
                        if save_figure:
                            print 'saving'
                            plt.savefig('{}\{}_{}_hydroraph.{}'.format(save_path, key, col, save_format), dpi=500)
                            plt.close()

            elif key == 'COY - Coyote hourly':
                for col in self._col_list:
                    ser = data[key]['Dataframe'][col]
                    if not ser.empty:
                        print '{} {} not empty'.format(key, col)
                        self._setup_different_hydrographs(ser, col, key)
                        if save_figure:
                            print 'saving'
                            plt.savefig('{}\{}_{}_hydroraph.{}'.format(save_path, key, col, save_format), dpi=500)
                            plt.close()
            else:
                for col in self._col_list:
                    ser = data[key]['Dataframe'][col]
                    print 'nan values: {}'.format(count_nonzero(isnan(ser)))
                    print 'len ser: {}'.format(len(ser))
                    if len(ser) != count_nonzero(isnan(ser)):
                        print '{} {} not empty'.format(key, col)
                        self._setup_different_hydrographs(ser, col, key)
                        if save_figure:
                            print 'saving'
                            plt.savefig('{}\{}_{}_hydroraph.{}'.format(save_path, key, col, save_format), dpi=500)
                            plt.close()


    def plot_hyd_subplots(self, data, save_path=None, save_figure=False, save_format='png'):

        key_list = [key for key in data]
        x = 1
        for key in data:
            print key
            if key in key_list:
                df = data[key]['Dataframe']
                ser = Series(df['Q_cfs'])
                ser = ser[(ser.index.year > 1989)]
                print ser.index[0]
                plt.subplot(len(self._critical_gauges), 1, x)
                plt.plot(ser)
                plt.title('{} {}'.format(data[key]['Name'], key))
                x += 1
                if save_figure:
                    print 'saving'
                    plt.savefig('{}\\allGauges.{}'.format(save_path, save_format), dpi=500)
                    plt.close()

        for key in data:
            print key
            if key in key_list:
                df = data[key]['Dataframe']
                ser = Series(df['Q_cfs'])
                ser = ser[(ser.index.year > 1998) & (ser.index.year < 2002)]
                print ser.index[0]
                if key == '11471000 daily':
                    plt.subplot(2, 1, 1)
                    plt.plot(ser)
                    plt.title('{} {}'.format(data[key]['Name'], key))
                    plt.xlabel('Date')
                    plt.ylabel('[cfs]')
                else:
                    print 'second suplot'
                    if key != '11471099 daily':
                        plt.subplot(2, 1, 2)
                        plt.plot(ser)
                        plt.title('Discharge at Irrigation Canals')
                        plt.xlabel('Date')
                        plt.ylabel('[cfs]')

                if save_figure:
                    print 'saving'
                    plt.savefig('{}\\allGauges.{}'.format(save_path, save_format), dpi=500)
                    plt.close()

    def plot_time_coverage_bar(self, data, zone='all', stage_plot=True,  save_path=None, save_figure=False,
                               save_format='png'):
        """ Plot horizontal bar showing temporal coverage of stream gauges

        :param data: 2-tuple or 4-tuple of numpy arrays
        :param save_path: csv save location
        :param save_figure:
        :param stage_plot:
        :param zone:
        :param save_format:
        :return: show and/or save plot:

        """

        if zone == 'downstream':
            key_list = self._downstream_gauges
        elif zone == 'upstream':
            key_list = self._upstream_gauges
        elif zone == 'potter':
            key_list = self._potter_gauges
        elif zone == 'critical':
            key_list = self._critical_gauges
        else:
            key_list = [key for key in data]
        key_list.sort()
        combo_list = []

        for key in data:
            if key not in ['11462125 peak', '11461000 daily']:
                for id in key_list:
                    if key == id:
                        combo_list.append((id, data[key]['Name'], data[key]['Dataframe']))
                        break
        sort_list = sorted(combo_list, key=lambda xx: xx[0])
        for i, element in enumerate(sort_list):
            if element[2].shape[1] == 5:
                rep = element[2]['Replacement Stage']
                pos = i
        if stage_plot:
            stage_list = [data[x[0]]['Dataframe']['Stage_ft'] for x in sort_list]
            for x in range(len(stage_list)):
                s = stage_list[x]
                s = deepcopy(s)
                s = s
                s[s > -0.1] = x + 1.1
                s[isnan(s)] = nan
                plt.plot(s, 'blue', lw=10, label='Stage [ft]')
                if pos == x:
                    r = rep
                    r[r > -0.1] = x + 1.2
                    plt.plot(r, 'green', lw=10, label='Rebuilt Stage [ft]')

        plt.legend()
        dict_list = [data[x[0]]['Dataframe']['Q_cfs'] for x in sort_list]
        name_list = ['{} {}'.format(x[1], x[0]) for x in sort_list]
        for x in range(len(dict_list)):
            s = dict_list[x]
            s = deepcopy(s)
            s[s > -0.1] = x + 1
            s[isnan(s)] = nan
            plt.plot(s, 'red', lw=10, label='Discharge [cfs]')
        plt.yticks(linspace(1, len(dict_list), len(dict_list)), name_list,
                   rotation='horizontal')
        if stage_plot:
            plt.title('Gauge Discharge and Stage Data Availability Through Time')
        else:
            plt.title('Gauge Discharge Data Availability Through Time')
        plt.grid(color='grey', linestyle='dashed')
        plt.ylim(0, len(dict_list) + 1)
        blue_patch = mpatches.Patch(color='blue', label='Stage [ft]')
        green_patch = mpatches.Patch(color='green', label='Rebuilt Stage [ft]')
        red_patch = mpatches.Patch(color='red', label='Discharge [cfs]')
        plt.legend(handles=[red_patch, blue_patch, green_patch], loc=2)
        plt.show()

        if save_figure:
            plt.savefig('{}\Gauge_time_coverage.{}'.format(save_path, save_format), dpi=500)
            print 'saving barh, gauge time coverage to {}'.format(save_path)
            # plt.close()

    def _setup_different_hydrographs(self, series, column, name):

        if column in self._col_list:
            pos = self._col_list.index(column)
            plt.plot(series)
            if pos in [2, 3]:
                plt.title('{}'.format(self._title_list[pos]))
            else:
                plt.title('{} {}'.format(self._title_list[pos], name))
            plt.ylabel(self._desc_list[pos])
            plt.xlabel('Time')

class PlotDataAsPoint:

    def __init__(self):
        pass

    def plot_to_shapefile(self):
        pass

# ============= EOF =============================================