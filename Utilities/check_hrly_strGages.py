import datetime
import os
import pandas as pd
import numpy as np

np.set_printoptions(threshold=3000, edgeitems=500)

path = r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test'
os.chdir(path)
old_base = []
q_recs = []
s_recs = []
out_q = []
stor = []

for (dirpath, dirnames, filenames) in os.walk(path):

    for filename in filenames:

        if filename.endswith('.txt') and filename != 'readme.txt':

            if dirpath in [r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\CLV - Russian River at Cloverdale',
                           r'C:\Users\David\Documents\USACE\FIRO\stream_gages\test\COY - Coyote']:

                fid = open(os.sep.join([dirpath, filename]))
                print fid
                lines = fid.readlines()
                fid.close()
                rows = [line.split(',') for line in lines]
                for line in rows:

                    if line[0] in ['CLV', 'COY'] and line[1] in ['20', '76']:  # '20'  discharge, '76' reservoir inflow
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.datetime.strptime(line[2], '%Y%m%d')
                                q_recs.append([day + datetime.timedelta(hours=xx),  float(line[xx + 3])])

                    if line[0] in ['CLV', 'COY'] and line[1] in ['1', '6']:  # '1' is river stage, '6' reservoir stage
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.datetime.strptime(line[2], '%Y%m%d')
                                s_recs.append([day + datetime.timedelta(hours=xx),  float(line[xx + 3])])

                    if line[0] == 'COY' and line[1] == '15':  # '15' is res. storage
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.datetime.strptime(line[2], '%Y%m%d')
                                stor.append([day + datetime.timedelta(hours=xx),  float(line[xx + 3])])

                    if line[0] == 'COY' and line[1] == '23':  # '23' is res. outflow
                        for xx in range(0, 24):
                            if line[xx + 3] not in ['m', 'm\n']:
                                day = datetime.datetime.strptime(line[2], '%Y%m%d')
                                out_q.append([day + datetime.timedelta(hours=xx),  float(line[xx + 3])])

            else:
                fid = open(os.sep.join([dirpath, filename]))
                print fid
                base = os.path.basename(dirpath).replace('usgs ', '')
                lines = fid.readlines()
                fid.close()
                rows = [line.split('\t') for line in lines]

                if base == old_base:
                    print 'continuing time series'
                    for line in rows:
                        if line[0] in ['USGS', base]:
                            try:
                                recs.append([datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M'),  # date
                                             float(line[4])])
                            except ValueError:
                                print 'Data value on {} is {}'.format(datetime.datetime.strptime(line[2],
                                                                      '%Y-%m-%d %H:%M'), line[4])
                        data = np.array([(element[0].strftime('%Y%m%d%H%M'), element[1]) for element in recs])
                    q_arr = np.array([(element[0], element[1]) for element in recs]).squeeze()

                recs = []
                for line in rows:
                    if line[0] in ['USGS', base]:
                        if line[2] in ['PST', 'PDT']:
                            recs.append([datetime.datetime.strptime(line[1], '%Y%m%d%H%M%S'),  # date
                                         float(line[3])])
                        else:
                            try:
                                recs.append([datetime.datetime.strptime(line[2], '%Y-%m-%d'),  # date
                                             float(line[3])])
                            except ValueError:
                                print 'Data value on {} is {}'.format(datetime.datetime.strptime(line[2], '%Y-%m-%d'),
                                                                      line[3])
                        data = np.array([(element[0].strftime('%Y%m%d%H%M'), element[1]) for element in recs])
                old_base = base
                q_arr = np.array([(element[0], element[1]) for element in recs]).squeeze()

    # Convert arrays to dataframe objects
    if dirpath in ['C:\Users\David\Documents\USACE\FIRO\stream_gages\\test\CLV - Russian River at Cloverdale',
                   'C:\Users\David\Documents\USACE\FIRO\stream_gages\\test\COY - Coyote']:

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
            coy_data.to_csv(r'{}\output\{}.csv'.format(path, base), sep=',')

        else:
            base = 'CLOVERDALE'
            clv_data = pd.concat([q_ser, s_ser], join='outer', axis=1)
            cols = ['Q_cfs', 'Stage_ftAbove_CDEC']
            clv_data.colmns = cols
            clv_data.to_csv(r'{}\output\{}.csv'.format(path, base), sep=',')
            del q_ser, s_ser

    if base not in ['CLOVERDALE', 'COYOTE']:
        try:
            q_df = pd.DataFrame(q_arr[:, 1], index=q_arr[:, 0], columns=['Q_cfs'])
            q_df.to_csv(r'{}\output\usgs_{}.csv'.format(path, base), sep=',')
        except NameError:
            pass