import pandas as pd
import numpy as np
import os

save_path = 'C:\\Users\\David\\Documents\\USACE\\FIRO\\meeting_2AUG16'
cols = list('abcd')
keys = list('lmnop')
rng = pd.date_range('1/1/2011', periods=20, freq='D')
df_dict = {}
yy = 0
for xx in xrange(0, 5):
    arrs = np.column_stack(np.random.randn(4, 20))
    df = pd.DataFrame(arrs, index=rng, columns=cols)
    df[df < 0] = np.nan
    df_dict.update({keys[yy]: df})
    yy += 1
str_1 = 'first line'
str_2 = 'second line'
str_3 = 'third line'
for key in df_dict:
    df = df_dict[key]
    ind = pd.to_datetime(df.index)
    lst = ind.strftime('%Y %m %d    %H:%M:%S')
    lst = np.array(lst, dtype=object)
    df = np.array(df, dtype=float)
    # obs_flows   05 / 22 / 1982    00:00:00    0.0323
    obs_str = np.array(['obs_flows' for x in range(0, len(df))])
    recs = np.column_stack((obs_str, lst, df))

    fmt = '\t'.join(['%s'] + ['%s'] + ['%10.6f'] * (recs.shape[1] - 2))

    with open('{}\\{}.txt'.format(save_path, key), 'wb') as f:
        f.write('{}{}{}{}'.format(str_1, os.linesep, str_2, os.linesep))
        np.savetxt(f, recs, fmt=fmt, delimiter='\t', newline=os.linesep)

