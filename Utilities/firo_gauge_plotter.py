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

from pandas import Series, concat, DataFrame
from numpy import array, column_stack
from matplotlib import pyplot as plt


class PlotGauges:

    def __init__(self):
        pass

    def plot_discharge(self, data, save_path=None, save_figure=False):
        """

        :param data: 2-tuple or 4-tuple of numpy arrays
        :param save_path: csv save location
        :param save_to_csv: choose to save data as csv
        :return: dict of data

        """
        x = 0
        for key in data:
            print key
            x += 1
            plt.figure(x)
            cur_df = data[key]
            plt.plot(cur_df['Q_cfs'])
            plt.xlabel('Days')
            plt.legend()
            plt.show()



# ============= EOF =============================================