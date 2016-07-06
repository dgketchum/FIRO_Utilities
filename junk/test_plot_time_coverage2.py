import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

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

x = 0
cln_dict = {}
for key in df_dict:
    x += 1
    df = df_dict[key]
    s = df['a']
    # s[pd.isnull(s)] = 0
    s[s > 0] = x
    cln_dict.update({key: s})
cln_df = pd.DataFrame(cln_dict)
fig, ax = plt.subplots()
# ax2 = plt.twiny(ax1)
plt.plot(cln_df.index, cln_df, lw=20)
plt.yticks(np.linspace(1, 5, 5), cln_dict.keys(), rotation='horizontal')
plt.ylim(0, 6)


x = 0
for col in df:
    x += 1
    ser = df[col]
    ser[ser > 0] = x
    ser[ser == np.nan] = 0
plt.plot(df, lw=10)
plt.ylim(0, 5)

