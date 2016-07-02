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

from pandas import Series, concat, DataFrame, isnull, to_numeric
from numpy import array, column_stack, nan


class DataframeManagement:

    def __init__(self):
        pass

    def other_array_to_dataframe(self, data, save_path=None, save_to_csv=False):
        """

        :param data: 2-tuple or 4-tuple of numpy arrays
        :param save_path: csv save location
        :param save_to_csv: choose to save data as csv
        :return: dict of data

        """
        q_data = data[0]
        s_data = data[1]

        q_ser = Series(q_data[:, 1], index=q_data[:, 0])
        s_ser = Series(s_data[:, 1], index=s_data[:, 0])

        if len(data) == 4:
            stor_data = data[2]
            out_q_data = data[3]
            stor_ser = Series(stor_data[:, 1], index=stor_data[:, 0])
            out_q_ser = Series(out_q_data[:, 1], index=out_q_data[:, 0])
            base = 'COYOTE'
            cols = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']
            coy_data = concat([q_ser, s_ser, out_q_ser, stor_ser], join='outer', axis=1)
            coy_data.columns = cols
            if save_to_csv:
                coy_data.to_csv(r'{}\{}.csv'.format(save_path, base), sep=',', index_label='DateTime')
            data_dict = coy_data
            data_dict = data_dict.replace({'"n\"': ""}, regex=True)
            return data_dict

        else:
            base = 'CLOVERDALE'
            stor_ser = Series(nan, index=q_data[:, 0])
            out_q_ser = stor_ser
            cols = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']
            clv_data = concat([q_ser, s_ser, out_q_ser, stor_ser], join='outer', axis=1)
            clv_data.columns = cols
            if save_to_csv:
                clv_data.to_csv(r'{}\{}.csv'.format(save_path, base), sep=',', index_label='DateTime')
            data_dict = clv_data
            data_dict = data_dict.replace({'"n\"': ""}, regex=True)
            return data_dict

    def usgs_array_to_dataframe(self, data, base_name, save_path=None, save_to_csv=False):
        """

        :param data: list of [datatime, discharge, stage=None]
        :param save_path: csv save location
        :param save_to_csv: choose to save data as csv
        :param base_name: name of the gauge
        :return: dict of data

        """
        cols = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']
        try:
            q_arr = array([(element[0], element[1], element[2]) for element in data]).squeeze()
            nan_ser = Series(nan, index=q_arr[:, 0])
            q_df1 = DataFrame(column_stack((q_arr[:, 1], q_arr[:, 2], nan_ser, nan_ser)), index=q_arr[:, 0],
                              columns=cols)
            q_df1.columns = cols

        except IndexError:
            print data
            q_arr = array([(element[0], element[1]) for element in data]).squeeze()
            print q_arr
            q_ser = Series(q_arr[:, 1], index=q_arr[:, 0])
            print q_ser
            nan_ser = Series(nan, index=q_arr[:, 0])
            q_df1 = concat([q_ser, nan_ser, nan_ser, nan_ser], join='outer', axis=1)
            q_df1.columns = cols

        grouped = q_df1.groupby(level=0)
        q_df = grouped.last()
        if save_to_csv:
            q_df.to_csv(r'{}\USGS_{}.csv'.format(save_path, base_name), sep=',', index_label='DateTime')
        if not q_df.index.is_unique:
            print 'non-unique indices in your df'
        return q_df

    def clean_dataframe(self, dict_of_dataframes, save_cleaned_df=False, save_path=None):
        print 'clean dfs'
        df_dict = dict_of_dataframes
        cln_dfs = {}
        for key in df_dict:
            df = df_dict[key].apply(to_numeric)
            df[df < 0] = nan
            cln_dfs.update({key: df})
            if save_cleaned_df:
                cln_dfs.to_csv(r'{}\Clean_{}.csv'.format(save_path, key), sep=',', index_label='DateTime')
        return cln_dfs

# ============= EOF =============================================














