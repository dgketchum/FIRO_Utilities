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
from numpy import count_nonzero, isnan, linspace
from pandas import DataFrame, Series, to_numeric


class PlotGauges:
    # hmm: where to put col_list I find myself using again
    # global variable, then add it as an arg to all funtion calls

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
        :return: dict of data

        """
        x = 0
        for key in data:
            print key
            x += 1
            plt.figure(x)

            for col in data[key]:
                ser = Series(data[key][col])
                if count_nonzero(isnan(ser)) != len(ser):
                    self._setup_different_hydrographs(ser, col)
                    # plt.show()
                    if save_figure:
                        print 'saving'
                        plt.savefig('{}\{}_{}_hydroraph.{}'.format(save_path, key, col, save_format), dpi=500)
                        print 'plot saved'

    def plot_time_coverage_bar(self, data, save_path=None, save_figure=False, save_format='png'):

        yvals = self._col_list

        q_dict = {}
        x = 0
        for key in data:
            x += 1
            df = data[key].apply(to_numeric)
            s = df['Q_cfs']
            s[s > 0] = x
            q_dict.update({key: s})
        cln_df = DataFrame(q_dict)
        plt.plot(cln_df.index, cln_df, lw=20)
        plt.yticks(linspace(1, 5, 5), cln_df.keys(), rotation='horizontal')
        plt.ylim(0, 6)

        if save_figure:
            plt.savefig('{}\{}__time_coverage.{}'.format(save_path, save_format, save_format), dpi=500)
            print 'saving barh, gauge time coverage to {}'.format(save_path)
        # plt.show()

    def _setup_different_hydrographs(self, series, column):

        if column in self._col_list:
            pos = self._col_list.index(column)
            plt.plot(series)
            plt.title('{} {}'.format(self._title_list[pos], self._col_list[pos]))
            plt.ylabel(self._desc_list[pos])
            plt.xlabel('Time')

class PlotDataAsPoint:

    def __init__(self):
        pass

    def plot_to_shapefile(self):
        pass

# ============= EOF =============================================