from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import usace_gauge_reader
import dictUtilities

np.set_printoptions(threshold=3000, edgeitems=500)

path = r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test'

gauge_csv = r'{}\tables\FIRO_gaugeDict.csv'.format(path)
gauge_headers = ['StationID', 'Name', 'Latitude', 'Longitude']
gauge_dict = dictUtilities.Dict.csv_to_dict(source_file=gauge_csv, headers=gauge_headers)

os.chdir(path)
dfDict = {}
for (dirpath, dirnames, filenames) in os.walk(path):

    if os.path.basename(dirpath) in ['output', 'statistics', 'usgs 11462125', 'tables']:
        print os.path.basename(dirpath), 'left off from data collection'
        pass

    elif not filenames:
        print 'empty filelist in {}'.format(dirpath)
        pass

    elif dirpath in [r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\CLV - Russian River at Cloverdale',
                     r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\COY - Coyote']:


        rg.read_usace_gauge(dirpath, filenames)

        q_recs = []
        s_recs = []
        out_q = []
        stor = []

        for filename in filenames:

            if filename.endswith('.txt') and filename != 'readme.txt':
                fid = open(os.sep.join([dirpath, filename]))
                print fid
                lines = fid.readlines()
                fid.close()
                rows = [line.split(',') for line in lines]
                for line in rows:

                    # '20'  discharge, '76' reservoir inflow
                    if line[0] in ['CLV', 'COY'] and line[1] in ['20', '76']:
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.strptime(line[2], '%Y%m%d')
                                q_recs.append([day + timedelta(hours=xx),  float(line[xx + 3])])

                    # '1' is river stage, '6' reservoir stage
                    if line[0] in ['CLV', 'COY'] and line[1] in ['1', '6']:
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.strptime(line[2], '%Y%m%d')
                                s_recs.append([day + timedelta(hours=xx),  float(line[xx + 3])])

                    if line[0] == 'COY' and line[1] == '15':  # '15' is res. storage
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.strptime(line[2], '%Y%m%d')
                                stor.append([day + timedelta(hours=xx),  float(line[xx + 3])])

                    if line[0] == 'COY' and line[1] == '23':  # '23' is res. outflow
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.strptime(line[2], '%Y%m%d')
                                out_q.append([day + timedelta(hours=xx),  float(line[xx + 3])])

        q_arr = np.array([(element[0], element[1]) for element in q_recs]).squeeze()
        q_ser = pd.Series(q_arr[:, 1], index=q_arr[:, 0])
        s_arr = np.array([(element[0], element[1]) for element in s_recs]).squeeze()
        s_ser = pd.Series(s_arr[:, 1], index=s_arr[:, 0])

        if stor and out_q is not []:
            stor_arr = np.array([(element[0], element[1]) for element in stor]).squeeze()
            stor_ser = pd.Series(stor_arr[:, 1], index=stor_arr[:, 0])
            out_q_arr = np.array([(element[0], element[1]) for element in out_q]).squeeze()
            out_q_ser = pd.Series(out_q_arr[:, 1], index=out_q_arr[:, 0])
            base = 'COYOTE'
            cols = ['Qin_cfs', 'Qout_cfs', 'Storage_acft', 'Elev_ftAbove_CDEC']
            coy_data = pd.concat([q_ser, out_q_ser, stor_ser, s_ser], join='outer', axis=1)

            coy_data.columns = cols
            coy_data.to_csv(r'{}\output\{}.csv'.format(path, base), sep=',', index_label='DateTime')
            dfDict.update({base: coy_data})

        else:
            base = 'CLOVERDALE'
            clv_data = pd.concat([q_ser, s_ser], join='outer', axis=1)
            cols = ['Q_cfs', 'Stage_ftAbove_CDEC']
            clv_data.columns = cols
            clv_data.to_csv(r'{}\output\{}.csv'.format(path, base), sep=',', index_label='DateTime')
            dfDict.update({base: clv_data})

    else:
        print ''
        old_base = []
        recs = []
        abc = []
        for filename in filenames:

            if filename.endswith('.txt') and filename != 'readme.txt':
                fid = open(os.sep.join([dirpath, filename]))
                print fid
                base = os.path.basename(dirpath).replace('usgs ', '')
                print 'base', base
                lines = fid.readlines()
                fid.close()
                rows = [line.split('\t') for line in lines]

                if base != old_base:
                    print 'first file'
                    for line in rows:
                        if line[0] in ['USGS', base]:
                            if line[2] in ['PST', 'PDT']:
                                recs.append([datetime.strptime(line[1], '%Y%m%d%H%M%S'), line[5]])
                                abc.append('a')
                            elif line[3] in ['PST', 'PDT']:
                                try:
                                    recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                                    abc.append('b')
                                except ValueError:
                                    recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4]])
                                    abc.append('x')

                            else:
                                try:
                                    recs.append([datetime.strptime(line[2], '%Y-%m-%d'), line[3]])
                                    abc.append('c')
                                except ValueError:
                                    abc.append('w')

                        else:
                            abc.append('y')

                else:
                    print 'continuing time series'
                    for line in rows:
                        if line[0] in ['USGS', base]:
                            try:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4], line[6]])
                                abc.append('d')
                            except ValueError:
                                recs.append([datetime.strptime(line[2], '%Y-%m-%d %H:%M'), line[4]])
                                abc.append('v')

                        else:
                            abc.append('z')

                old_base = base

            print ' a', abc.count('a'), ' b', abc.count('b'), ' c', abc.count('c'), ' d', abc.count('d'), ' v',\
                abc.count('v'), ' w', abc.count('w'), ' x', abc.count('x'), ' y', abc.count('y'), ' z', abc.count('z')
        try:
            q_arr = np.array([(element[0], element[1], element[2]) for element in recs]).squeeze()
            q_df1 = pd.DataFrame(np.column_stack((q_arr[:, 1], q_arr[:, 2])), index=q_arr[:, 0],
                                 columns=['Q_cfs', 'Stage_ft'])
        except IndexError:
            q_arr = np.array([(element[0], element[1]) for element in recs]).squeeze()
            q_df1 = pd.DataFrame(q_arr[:, 1], index=q_arr[:, 0],
                                 columns=['Q_cfs'])
        grouped = q_df1.groupby(level=0)
        q_df = grouped.last()
        q_df.to_csv(r'{}\output\usgs_{}.csv'.format(path, base), sep=',', index_label='DateTime')
        if not q_df.index.is_unique:
            print 'non-unique indices in your df'
        dfDict.update({base: q_df})

panel = pd.Panel.from_dict(dfDict, orient='items')
