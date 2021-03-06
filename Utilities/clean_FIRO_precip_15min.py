import datetime
import os
import numpy as np

np.set_printoptions(threshold=3000, edgeitems=500)

path = 'C:\Users\David\Documents\USACE\FIRO\precipitation\\to_process\\15min'
os.chdir(path)

for (dirpath, dirnames, filenames) in os.walk(path):
    for filename in filenames:
        if filename.endswith('.csv'):
            fid = open(os.sep.join([dirpath, filename]))
            # print "opening file: " + '{a}'.format(a=fid)
            base = os.path.basename(dirpath)
            station_name = base[14:].replace(', CA US', '').replace(' ', '').lower()
            lines = fid.readlines()[1:]
            fid.close()
            rows = [line.split(',') for line in lines]
            recs = []

            for line in rows:
                recs.append([datetime.datetime.strptime(line[5], '%Y%m%d %H:%M'),  # date
                             float(line[10]), str(line[11])])

            recs_list = []
            for i in recs:
                recs_list.append([str(i[0].year), str(i[0].month).rjust(2, '0'), str(i[0].day).rjust(2, '0'),
                                  str(i[0].hour).rjust(2, '0'), str(i[0].minute).rjust(2, '0'), i[1]/10., str(i[2])])
            data_cleaned = []
            for x in recs_list:

                if x[6] in ['}', '{', '[', ']']:
                    # print 'found no-data value'
                    pass

                elif x[5] < 0.0:
                    # print 'found negative value'
                    pass

                elif x[6] in ['g', 'A', ' ']:
                    x[6] = int(0)
                    data_cleaned.append(x)

                elif x[6] == 'a':
                    x[6] = int(1)
                    data_cleaned.append(x)

                else:
                    # print x[6], type(x[6])
                    pass

            if recs_list:
                data = np.array(data_cleaned)
                print 'saving xlsx and txt'
                path = 'C:\Users\David\Documents\USACE\FIRO\precipitation\\to_process\processed_15min'
                np.savetxt('{f}\\{g}.txt'.format(f=path, g=station_name),
                           data, fmt=['%s', '%s', '%s', '%s', '%s', '%s', '%s'],
                           delimiter='  ')
                np.savetxt('{f}\\{g}.csv'.format(f=path, g=station_name),
                           data, fmt=['%s', '%s', '%s', '%s', '%s', '%s', '%s'],
                           delimiter=',')
            else:
                print '{} has a list with zero values'.format(station_name)