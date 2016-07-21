import pandas as pd
import numpy as np

cols = list('ab')
keys = list('lm')
rng = pd.date_range('1/1/2011', periods=20, freq='D')
df_dict = {}
yy = 0
for xx in xrange(0, 2):
    arrs = np.column_stack(np.random.randn(2, 20))
    df = pd.DataFrame(arrs, index=rng, columns=cols)
    df[df < 0] = np.nan
    df_dict.update({keys[yy]: df})
    yy += 1

df_dict['l']['a'] *= df_dict['l']['a'] * 100
df_dict['m']['a'] *= df_dict['m']['a'] * 100
rating = [(x, x**2.5) for x in xrange(100)]
arr = np.array(rating)
for key in df_dict:
    df = df_dict[key]
    recs = []
    print df
    for ind, row in df.iterrows():
        if pd.isnull(row[1]) and pd.notnull(row[0]):
            # print 'cillable row: {}'.format(row)
            idx = (abs(arr[:, 1] - float(row[0]))).argmin()
            df.set_value(ind, 'b', arr[idx, 0])
            recs.append(arr[idx, 0])
            # print 'updated row: {}'.format(row)
        else:
            recs.append(np.nan)
    rep_stage = np.array(recs)
    rep_stage = pd.Series(rep_stage, name='Replacement Stage', index=df.index)
    df = pd.concat((df, rep_stage), join='outer', axis=1)
    print df