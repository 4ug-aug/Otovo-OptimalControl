# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 15:35:11 2022

@author: vidis
"""

import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import datetime as dt
import pytz

df_main = pd.read_csv('sample-csv', delimiter=';', header=0)
df = df_main.drop(['id'], axis=1)


def dtextract(x):
    df[x] = pd.to_datetime(df[x], utc=True)
    df['year'] = df[x].dt.year
    df['month'] = df[x].dt.month
    df['hour'] = df[x].dt.hour
    df['weekday'] = df[x].dt.weekday


dtextract('timeslot')

fig, axs = plt.subplots(figsize=(12, 4))
df2 = df[df['type']=='production']
df3 = df[df['type']=='consumption']
df4 = df[df['type']=='elcert']
plt.plot(df2.groupby(df2['weekday'])["num_kwh"].mean(), color = 'green')
plt.plot(df3.groupby(df3['weekday'])["num_kwh"].mean(), color = 'red')
plt.plot(df4.groupby(df4['weekday'])["num_kwh"].mean(), color = 'blue')


df2.groupby(df2['weekday'])["num_kwh"].mean().plot(kind='bar', rot=0, ax=axs)






