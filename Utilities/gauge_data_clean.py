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

from pandas import Series, concat, DataFrame, to_numeric, isnull, notnull, rolling_median
from numpy import array, column_stack, nan, count_nonzero, argmin, abs
from datetime import datetime


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
        # extract CLV COY data from passed tuple
        q_data = data[0]
        s_data = data[1]
        # put data in pandas series, index with pandas Timestamp datetime
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
        """Take lists of USGS data, put into dataframes (pandas)

        :param data: list of [datatime, discharge, stage=None]
        :param save_path: csv save location
        :param save_to_csv: choose to save data as csv
        :param base_name: name of the gauge
        :return: dict of data

        """
        cols = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']

        # this if for the records that have both discharge and stage data
        # fill empty columns with NaN so all dataframes have same shape and keys (column headers)
        # index with pandas Timestamp datetimes
        q_arr = array([(element[0], element[1], element[2]) for element in data]).squeeze()
        nan_ser = Series(nan, index=q_arr[:, 0])
        q_df1 = DataFrame(column_stack((q_arr[:, 1], q_arr[:, 2], nan_ser, nan_ser)), index=q_arr[:, 0],
                          columns=cols)
        q_df1.columns = cols

        # organize concatenated series (dataframe) into consistent shape
        grouped = q_df1.groupby(level=0)
        q_df = grouped.last()
        # optional save to csv
        if save_to_csv:
            q_df.to_csv(r'{}\USGS_{}.csv'.format(save_path, base_name), sep=',', index_label='DateTime')
        if not q_df.index.is_unique:
            print 'non-unique indices in your df'
        return q_df

    def rating_array_to_dataframe(self, data):
        """

        :param data:
        :return:
        """

        arr = array([(element[0], element[1], element[2]) for element in data])
        df = DataFrame(arr, columns=['Ind', 'Shift', 'Dep'])
        print df
        return

    def clean_dataframe(self, dict_of_dataframes, single_gauge=False, fill_stage=False, clean_beyond_three_sigma=False,
                        clean_before_three_sigma=False, impose_rolling_condition=False, rolling_window=5,
                        save_cleaned_df=False, save_path=None, window=None):
        """Take gauge data and clean it, removing supect vales and replacing with NaN

        :param dict_of_dataframes:
        :param clean_beyond_three_sigma:
        :param clean_before_three_sigma:
        :param impose_rolling_condition:
        :param save_cleaned_df:
        :param save_path:
        :return:
        """
        print 'clean dfs'
        df_dict = dict_of_dataframes

        # fill stage data from rating curve data
        if fill_stage:
            self._fill_stage(df_dict)

        # each key is a separate gauge
        for key in df_dict:
            if key == '11462125 peak':
                print 'peak data skipped for cleaning'
            else:
                # put key in numerical dataframe format, remove negative values
                if single_gauge:
                    key = df_dict.keys()[0]
                    df = df_dict.values()[0]
                else:
                    df = df_dict[key]['Dataframe']
                print ''
                print 'key: {}'.format(key)
                df = df.apply(to_numeric)
                df[df < 0] = nan
                # each series is a data type, i.e. discharge, stage, etc.
                for series in df:
                    series = df[series]
                    print series.name, type(series)
                    series_mean = series.mean(skipna=True)
                    series_std = series.std(skipna=True)
                    print 'mean {} with outliers: {}'.format(series.name, series_mean)

                    # count values falling above 3-sigma
                    xx = 0
                    yy = 0
                    for index, value in series.iteritems():
                        if value > (series_mean + 3 * series_std):
                            xx += 1
                        elif value == 0.0:
                            yy += 1
                    print '{} elements over mean 3 * std of n = {}'.format(xx, len(series))
                    print '{} elements are ZERO of n = {}'.format(yy, len(series))

                    # clean values falling above and/or below 3-sigma
                    if clean_beyond_three_sigma:
                        series[series > series_mean + 3 * series_std] = nan
                    if clean_before_three_sigma:
                        series[series > series_mean + 3 * series_std] = nan

                    # clean stage data that is obviously erroneous
                    if key == 'CLV - Cloverdale hourly':
                        if series.name == 'Stage_ft':
                            print ' working on CLV stage'
                            series[series > 15.0] = nan
                            series[series == 0.0] = nan
                            series_mean = series.mean(skipna=True)
                            print 'mean {} without outliers: {}'.format(series.name, series_mean)

                    elif key == 'COY - Coyote hourly':
                        if series.name == 'Stage_ft':
                            print ' working on COY storage'
                            series[series < 705.0] = nan
                            series[series == 0.0] = nan
                            if impose_rolling_condition:
                                self._impose_rolling_condition(series, rolling_window)

                        elif series.name == 'Storage_acft':
                            print ' working on COY stage'
                            if impose_rolling_condition:
                                self._impose_rolling_condition(series, rolling_window)

                        elif series.name == 'Qout_cfs':
                            print 'Coyote Dam Out hydrograph not cleaned'

                        elif series.name == 'Q_cfs':
                            if impose_rolling_condition:
                                self._impose_rolling_condition(series, rolling_window)
                            series[series > 50000.] = nan

                    if series.name == 'Q_cfs':
                        if impose_rolling_condition:
                            self._impose_rolling_condition(series, rolling_window)

                if single_gauge:
                    df_dict.update({key: df})
                else:
                    df_dict[key].update({'Dataframe': df})

        if save_cleaned_df:
            for key in df_dict:
                if window:
                    i, j = window
                    df_w = df_dict[key]['Dataframe']
                    df_w = df_w[(df_w.index > i) & (df_w.index < j)]
                if key != '11462125 peak':
                    if window:
                        df = df_w
                    else:
                        df = df_dict[key]['Dataframe']
                    df.to_csv(r'{}\Clean_{}.csv'.format(save_path, key), sep=',', index_label='DateTime')
        print 'cleaned df'
        return df_dict

    def save_cleaned_stats(self, cleaned_dataframe_dict, gauge_dict, save_path, save_cleaned_states=False):
        pass
        """Find stats on data coverage and save to a csv

        :param cleaned_dataframe_dict:
        :param save_cleaned_states:
        :param save_path:
        :return:
        """
        df_dict = cleaned_dataframe_dict
        for key in df_dict:
            df = df_dict[key]
            for series in df:
                s = df[series]
                start = datetime.strftime(s.index[1].to_datetime(), '%Y/%m/%d %H:%M')
                end = datetime.strftime(s.index[-1].to_datetime(), '%Y/%m/%d %H:%M')
                percnt_cov  = count_nonzero(s)/len(s) * 100
        # not done!

    def _impose_rolling_condition(self, series, rolling_window):

        print 'rolling median'
        threshold = series.std(skipna=True)
        print 'std dev: {}'.format(threshold)
        check = series.rolling(window=rolling_window,
                               center=True).median().fillna(method='bfill').fillna(method='ffill')
        difference = abs(series - check)
        outlier_idx = difference > threshold
        series[outlier_idx] = nan
        series_mean = series.mean(skipna=True)
        print 'mean {} without outliers: {}'.format(series.name, series_mean)
        print 'eliminated {} values from {}'.format(sum(outlier_idx), series.name)
        print ''

    def find_extreme_events(self, dict_of_dataframees, gauge_key):
        pass
        # not done!

    def _fill_stage(self, dict_of_dataframes):

        print 'attempting to fill in stage data'
        for key, dic in dict_of_dataframes.iteritems():
            df = dic['Dataframe']
            try:
                arr = dic['Rating']
                print arr.size
                print df
                recs = []
                for ind, row in df.iterrows():
                    if isnull(row[1]) and notnull(row[0]):
                        idx = (abs(arr[:, 2] - float(row[0]))).argmin()
                        df.set_value(ind, 'Stage_ft', arr[idx, 0])
                        recs.append(arr[idx, 0])
                    else:
                        recs.append(nan)
                rep_stage = array(recs)
                rep_stage = Series(rep_stage, name='Replacement Stage', index=df.index)
                df = concat((df, rep_stage), join='outer', axis=1)
                dict_of_dataframes[key].update({'Dataframe': df})
                print df
                print 'shape of dataframe: {}'.format(df.shape)
                print 'length of replacement stage array: {}'.format(len(recs))
            except TypeError:
                print 'arr {} is {}'.format(key, type(arr))
            except AttributeError:
                print 'arr {} is {}'.format(key, type(arr))



# ============= EOF =============================================













