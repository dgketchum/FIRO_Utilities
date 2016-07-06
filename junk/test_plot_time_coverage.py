from matplotlib import dates as mdates
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

yvals = ['Q_cfs', 'Stage_ft', 'Qout_cfs', 'Storage_acft']
start_list = []
end_list = []
key_list = []
for key in df_dict:
    min_date = df_dict[key].index.min()
    start_list.append(min_date.to_datetime())
    max_date = df_dict[key].index.max()
    end_list.append(max_date.to_datetime())
    key_list.append(key)

    start = mdates.date2num(start_list)
    end = mdates.date2num(end_list)
    width = end - start

fig, ax = plt.subplots()
ax.barh(bottom=range(1, 6), width=width, left=start, height=0.3)
xfmt = mdates.DateFormatter('%Y-%m-%d %H:&M')
ax.xaxis.set_major_formatter(xfmt)
# autorotate the dates
fig.autofmt_xdate()
plt.show()

df = df_dict['COY - Coyote']
x = 0

for col in df:
    x += 1
    ser = df[col]
    ser[pd.isnull(ser)] = 0
    ser[pd.notnull(ser)] = x
plt.plot(df)

