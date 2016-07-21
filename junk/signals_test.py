###  mess around with a hydrograph  ###

import os
from Utilities.dictUtilities import CSVParser
from Utilities.gauge_reader import OtherGaugeReader, PrecipGaugeReader
from Utilities.gauge_data_clean import DataframeManagement
from Utilities.firo_gauge_plotter import PlotGauges
from pandas import set_option, Series, Timedelta, to_datetime, to_numeric
from numpy import set_printoptions, array
from matplotlib import pyplot as plt

# print options can be set long to see more array data at once
set_printoptions(threshold=3000, edgeitems=500)
set_option('display.height', 500)
set_option('display.max_rows', 500)


def clean_hydrograph_signal(root):

    other_gauge_reader = OtherGaugeReader()
    df_generator = DataframeManagement()
    gauge_plotter = PlotGauges()
    csv_parser = CSVParser()
    ppt_reader = PrecipGaugeReader()

    ppt, check = ppt_reader.read_in_precip_gauge(ppt_path, 'pottervalleypowerhouse.csv')

    flood_dict = csv_parser.csv_to_dict(flood_path, type='floods')
    print 'Floods: {}'.format(flood_dict)

    gauge_dict = {}

    for (dirpath, dirnames, filenames) in os.walk(root):
        if os.path.basename(dirpath) in ['COY']:
            base = os.path.basename(dirpath)
            base += ' hourly'
            data = other_gauge_reader.read_gauge(dirpath, filenames)
            data = df_generator.other_array_to_dataframe(data)
            gauge_dict.update({base: data})
            data = gauge_dict.copy()
            data = df_generator.clean_dataframe(gauge_dict, single_gauge=True, impose_rolling_condition='median',
                                                rolling_window=5)

    date = flood_dict['Rank 1']['Date']

    def plot_window(gauge_dict, base, date, delta_type='days', time_buffer=20):
        s = data[base]['Q_cfs']
        name = gauge_dict.keys()[0]
        if delta_type == 'days':
            rng = s[date - Timedelta(days=time_buffer): date + Timedelta(days=time_buffer)]
        elif delta_type == 'hours':
            rng = s[date - Timedelta(days=time_buffer): date + Timedelta(days=time_buffer)]
        plt.plot(rng, 'k')
        plt.legend()
        plt.title('RAW DATA -- Discharge at {} Event Peak: {}'.format(name, date))
        plt.xlabel('Date')
        plt.ylabel('[cfs]')

    # this should eventually go into the plotting class
    def select_known_events(gauge_dict, base, precipitation, precip=True, date='2006-01-01', buffer_days=25):
        s = data[base]['Q_cfs']
        name = gauge_dict.keys()[0]
        date_obj = to_datetime(date)
        rng = s[date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
        if precip:
            fig, ax1 = plt.subplots()
            ax1.plot(rng, 'k', label='Discharge [cfs]')
            ax1.legend()
            plt.title('Discharge at {} Event Peak: {}'.format(name, date))
            ppt_s = Series(array(precipitation)[:, 1], index=array(precipitation)[:, 0])
            ppt_s = ppt_s.reindex(index=rng.index, method=None)
            ppt_s = ppt_s.apply(to_numeric)
            ppt_s[ppt_s < 0] = 0.0
            ppt_s = ppt_s[date_obj - Timedelta(days=buffer_days): date_obj + Timedelta(days=buffer_days)]
            ax1.set_xlabel('Date')
            ax1.set_ylabel('[cfs]')
            for tl in ax1.get_yticklabels():
                tl.set_color('k')
            ax2 = ax1.twinx()
            ax2.bar(ppt_s.index, ppt_s, width=0.1, label='Precipitation [mm/hr]')
            plt.gca().invert_yaxis()
            ax2.set_ylabel('[mm]')
            for tl in ax2.get_yticklabels():
                tl.set_color('b')
            ax2.legend()
        if not precip:
            plt.plot(rng)
            plt.plot(rng, 'k')
            plt.legend()
            plt.title('Discharge at {} Event Peak: {}'.format(name, date))
            plt.xlabel('Date')
            plt.ylabel('[cfs]')

    select_known_events(data, base, precipitation=ppt)
    plot_window(gauge_dict, base, date)

    rng = data[base]['Storage_acft']
    plt.plot(rng, 'g')
    plt.legend()
    plt.title('Rolling Median -- Storage at Mendocino')
    plt.xlabel('Date')
    plt.ylabel('[acre feet]')

if __name__ == '__main__':
    home = os.path.expanduser('~')
    print 'home: {}'.format(home)
    ppt_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'precipitation', 'FIRO_precip_coops',
                            'processed_hourly')
    root = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'stream_gauges_test')
    flood_path = os.path.join(home, 'Documents', 'USACE', 'FIRO', 'Events',
                              'FloodDict_Ralph_etal_2006.csv')
    clean_hydrograph_signal(root)

 #  ==================================EOF=================================
