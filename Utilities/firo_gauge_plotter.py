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


from matplotlib import pyplot as plt
from numpy import count_nonzero, isnan, linspace, nan
from pandas import DataFrame, Series, to_numeric, isnull


class PlotGauges:

    def __init__(self):
        self._col_list = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']
        self._title_list = ['Discharge at Gauge', 'Stage at Gauge', 'Discharge from COYOTE DAM', 'Storage at COYOTE DAM']
        self._desc_list = ['Discharge [cfs]', 'Stage [ft]', 'Discharge [cfs]', 'Storage [af]']

    def plot_discharge(self, data, save_path=None, save_figure=False, save_format='png'):
        """

        :param data: 2-tuple or 4-tuple of numpy arrays
        :param save_path: csv save location
        :param save_figure:
        :param save_format
        :return: show and/or save hydrographs of various types (stream flow, stage, etc)

        """
        x = 0
        for key in data:
            print key
            x += 1
            plt.figure(x)

            for col in data[key]:
                print col
                ser = Series(data[key][col])
                ser[isnull(ser)] = nan
                if count_nonzero(isnan(ser)) != len(ser):
                    self._setup_different_hydrographs(ser, col, key)
                    # plt.show()
                    if save_figure:
                        print 'saving'
                        plt.savefig('{}\{}_{}_hydroraph.{}'.format(save_path, key, col, save_format), dpi=500)
                        plt.close()
                        print 'plot saved'

    def plot_time_coverage_bar(self, data, save_path=None, save_figure=False, save_format='png'):
        """ Plot horizontal bar showing temporal coverage of stream gauges

        :param data: 2-tuple or 4-tuple of numpy arrays
        :param save_path: csv save location
        :param save_figure:
        :param save_format
        :return: show and/or save plot

        """

        plt_dict = {}
        x = 0
        for key in data:
            print key

            x += 1
            df = data[key].apply(to_numeric)
            s = df['Q_cfs']
            s[isnull(s)] = nan
            s[s >= 0] = x
            s[s < x] = nan
            plt_dict.update({key: s})

        cln_df = DataFrame(plt_dict)
        plt.plot(cln_df.index, cln_df, lw=20)
        plt.yticks(linspace(1, 5, 5), cln_df.keys(), rotation='horizontal')
        plt.ylim(0, len(cln_df.keys()) + 1)

        if save_figure:
            plt.savefig('{}\Gauge_time_coverage.{}'.format(save_path, save_format), dpi=500)
            print 'saving barh, gauge time coverage to {}'.format(save_path)
            plt.close()
            # plt.show()

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