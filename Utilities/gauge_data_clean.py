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

from pandas import Series, concat, DataFrame, to_numeric, Timedelta, isnull
from numpy import array, column_stack, nan, count_nonzero
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

    def clean_dataframe(self, dict_of_dataframes, clean_beyond_three_sigma=False, clean_before_three_sigma=False,
                        impose_rolling_condition=False, save_cleaned_df=False, save_path=None):
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

        # each key is a separate gauge
        for key in df_dict:
            print ''
            print 'key: {}'.format(key)
            if key == '11462125 peak':
                print 'peak data skipped for cleaning'
            else:
                # put key in numerical dataframe format, remove negative values
                df = df_dict[key]['Dataframe']
                df = df.apply(to_numeric)
                df[df < 0] = nan
                # each series is a data type, i.e. discharge, stage, etc.
                for series in df:
                    print ''
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
                            # print element, 'over 3 * std'
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

                        # what is a resonable outlier that needs removal is arguable
                        # the problem with discharge is that during a runoff event, Q can increase by an order
                        # of magnitude or more within an hour
                        # this rolling window only looks back, perhaps it would be better to center it
                        # or, better, compare with precip and find values not associated with rain or melt

                    elif key == 'COY - Coyote hourly':
                        if series.name == 'Stage_ft':
                            print ' working on COY stage'
                            # stage 670. is about the elevation of the outflow structure
                            series[series < 670.0] = nan
                            series[series == 0.0] = nan
                            series_mean = series.mean(skipna=True)
                            print 'mean {} without outliers: {}'.format(series.name, series_mean)
                            # how fast can the stage change?
                            # there are some suspect, low values that current conditional leaves in the data
                            if impose_rolling_condition:
                                self._impose_rolling_condition(series,
                                                               lambda vi, av: vi < 0.9 * av and vi < 705)
                        elif series.name == 'Qout_cfs':
                            if impose_rolling_condition:
                                x = 0
                                y = 0
                                vals = self._value_list(series)
                                for ind, val in series.iteritems():
                                    if vals:
                                        # consider this condition and adjust it based on hyrdologic knowledge!
                                        if val > 100 * (sum(vals) / len(vals)):
                                            print 'outlier at {} of value:  {}'.format(ind, val)
                                            print 'value among previous values of: {}'.format(vals)
                                            series[ind] = nan
                                            y += 1
                                        elif val < 0.01 * (sum(vals) / len(vals)):
                                            print 'outlier at {} of value:  {}'.format(ind, val)
                                            print 'value among previous values of: {}'.format(vals)
                                            series[ind] = nan
                                            y += 1
                                        vals = vals[1:20]
                                        vals.append(val)
                                        x += 1

                    if series.name == 'Q_cfs':
                        if impose_rolling_condition:
                            self._impose_rolling_condition(series, lambda vi, av: vi > 100 * av)

                df_dict[key].update({'Dataframe': df})

        if save_cleaned_df:
            for key in df_dict:
                if key != '11462125 peak':
                    df = df_dict[key]['Dataframe']
                    df.to_csv(r'{}\Clean_{}.csv'.format(save_path, key), sep=',', index_label='DateTime')

        return df_dict

    def save_cleaned_stats(self, cleaned_dataframe_dict, gauge_dict, save_path, save_cleaned_states=False):
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

    def _impose_rolling_condition(self, series, predicate):

        # x = 0
        remove_cnt = 0
        vals = self._value_list(series)
        if vals:
            for ind, val in series.iteritems():
                # if vals:
                # consider this condition and adjust it based on hyrdologic knowledge!
                # if val > 100 * (sum(vals) / len(vals)):
                vavg = sum(vals) / len(vals)
                if predicate(val, vavg):
                    print 'outlier at {} of value:  {}'.format(ind, val)
                    print 'value among previous values of: {}'.format(vals)
                    series[ind] = nan
                    if val != nan:
                        vals = vals[1:20]
                        vals.append(val)
                    remove_cnt += 1
                    # x += 1

            series_mean = series.mean(skipna=True)
            print 'removed {} values'.format(remove_cnt)
            print 'mean {} without outliers: {}'.format(series.name, series_mean)

    def _value_list(self, series):
        vals = []
        for ind, val in series.iteritems():
            if series[ind] > 0:
                vals.append(series[ind])
            if len(vals) == 20:
                break
        return vals

    # helpful slicer snipets
    # q_df[(q_df.index.year == 2008) & (q_df.index.month == 4) & (q_df.index.day == 7)]
    # s =s[(s.index.year == 1998) & (s.index.month == 11) & (s.index.day > 20)]
    # series = cln_dfs['COY - Coyote']['Q_cfs']

# ============= EOF =============================================













