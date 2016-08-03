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
from pandas import set_option, Series, to_numeric, date_range
from numpy import set_printoptions, array
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.dates as mdates
from matplotlib import pyplot as plt
from datetime import datetime

from Utilities.dictUtilities import CSVParser
from Utilities.firo_str_gauge_clean import gauge_clean


# print options can be set long to see more array data at once
# set_printoptions(threshold=3000, edgeitems=500)
# set_option('display.max_rows', 500)

ppt_list = ['pottervalleypowerhouse.csv']

if __name__ != '__main__':
    fig_save = ''


def get_long_hydrograph(root, str_gauge, ppt_list, save_path, save_format='.pdf'):
    """Show the entire length of a hydrograph by plotting on consecutive panels in a pdf and saving

    :param root:
    :param rank:
    :return:
    """

    csv_parser = CSVParser()
    ppt_dict = csv_parser.make_ppt_dict(ppt_path, alt_ppt_path, ppt_list)
    gauge_dict = gauge_clean(root, alt_dirs, gpath, save_path=fig_save)

    name = gauge_dict[str_gauge]['Name']
    key = ppt_list[0].replace('.csv', '')

    hyd_series = gauge_dict[str_gauge]['Dataframe']['Q_cfs']
    ppt_s = Series(array(ppt_dict[key])[:, 1], index=array(ppt_dict[key])[:, 0])

    series_start = max(hyd_series.index[0], ppt_s.index[0])
    series_start = datetime((series_start.year + 1), 1, 1, 00, 00)
    series_end = min(hyd_series.index[-1], ppt_s.index[-1])
    series_end = datetime(series_end.year, 1, 1, 00, 00)
    ind = date_range(series_start, series_end, freq='15min')

    hyd_series = hyd_series[(hyd_series.index < series_end) & (hyd_series.index > series_start)]
    hyd_series = hyd_series.reindex(index=ind, method='nearest', limit=4)
    hyd_series.fillna(value=0.0, inplace=True)

    ppt_s = ppt_s[(ppt_s.index < series_end) & (ppt_s.index > series_start)]
    ppt_s = ppt_s.reindex(index=ind, method=None)
    ppt_s = ppt_s.apply(to_numeric)
    ppt_s[ppt_s > 1000] = 0.0
    ppt_s[ppt_s == 2.54] = 0.254
    ppt_s.fillna(value=0.0, inplace=True)

    def series_to_hanging_hyeto(hydrograph_series, precipitation_series, start, end, gauge_name):
        s_yr, e_yr = start.year, end.year
        ss, pp = hydrograph_series, precipitation_series
        with PdfPages('{}\\{}.pdf'.format(fig_save, gauge_name)) as pdf:
            for year in range(s_yr, e_yr):
                print 'year {}'.format(year)
                s_yr, ppt_yr = ss[ss.index.year == year], pp[pp.index.year == year]
                s1, p1 = (s_yr[s_yr.index.month < 4], ppt_yr[ppt_yr.index.month < 4])

                s2, p2 = (s_yr[(4 <= s_yr.index.month) & (s_yr.index.month < 7)],
                          ppt_yr[(4 <= ppt_yr.index.month) & (ppt_yr.index.month < 7)])

                s3, p3 = (s_yr[(7 <= s_yr.index.month) & (s_yr.index.month < 10)],
                          ppt_yr[(7 <= ppt_yr.index.month) & (ppt_yr.index.month < 10)])

                s4, p4 = s_yr[s_yr.index.month >= 10], ppt_yr[ppt_yr.index.month >= 10]

                series_list = [(s1, p1), (s2, p2), (s3, p3), (s4, p4)]

                fig, axes = plt.subplots(len(series_list), sharex=False, sharey=True, figsize=(8.5, 11.))
                lst = series_list
                for row, item in zip(axes, lst):
                    s, p = item
                    row.plot(s.index, s)
                    row.set_ylabel('[cfs]')
                    plt.xlabel('Date')
                    max_p = p.max()
                    for tl in row.get_yticklabels():
                        tl.set_color('b')
                    ax2 = row.twinx()
                    ax2.bar(p.index, p, width=0.1, label='Precipitation [mm/hr]')
                    ax = plt.gca()
                    print 'max p : {}'.format(max_p)
                    ax.xaxis.set_major_locator(mdates.MonthLocator())
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%B'))
                    ax.set_ylim([0, max_p * 2.0])
                    ax.invert_yaxis()
                    ax2.set_ylabel('[mm]')
                    for tl in ax2.get_yticklabels():
                        tl.set_color('k')
                plt.tight_layout()
                pdf.savefig()
                plt.close()
            pdf.savefig(fig)
            plt.close()

    series_to_hanging_hyeto(hyd_series, ppt_s, series_start, series_end, name)

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    firo_dir = os.path.join(home, 'Documents', 'USACE', 'FIRO')
    ppt_path = os.path.join(firo_dir, 'precipitation', 'FIRO_precip_coops', 'processed_15min')
    alt_ppt_path = os.path.join(firo_dir, 'precipitation', 'FIRO_precip_coops', 'processed_hourly')
    rating_path = os.path.join(firo_dir, 'rating_curves')
    alt_dirs = [r'C:\Users\David\Documents\USACE\FIRO\stream_gauges_test\coverage_check\hourly\COY - Coyote hourly']
    fig_save = os.path.join(firo_dir, 'meeting_2AUG16', '2001-01-20_2001-03-15')
    root = os.path.join(firo_dir, 'stream_gauges_test')
    flood_path = os.path.join(firo_dir, 'Events', 'FloodDict_Ralph_etal_2006.csv')
    gpath = os.path.join(root, 'coverage_check', 'FIRO_gaugeDict.csv')
    get_long_hydrograph(root, '11461500 15 minute', ppt_list, fig_save)

# ==================================EOF=================================