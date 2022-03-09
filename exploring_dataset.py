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


def plot_timeslot():
    df_copy = df_main.copy()
    df_copy = df_main.drop(['id'], axis=1)

    def dtextract(x):
        df_copy[x] = pd.to_datetime(df_copy[x], utc=True)
        df_copy['year'] = df_copy[x].dt.year
        df_copy['month'] = df_copy[x].dt.month
        df_copy['hour'] = df_copy[x].dt.hour
        df_copy['weekday'] = df_copy[x].dt.weekday


    dtextract('timeslot')

    fig, axs = plt.subplots(figsize=(12, 4))
    df2 = df_copy[df_copy['type']=='production']
    df3 = df_copy[df_copy['type']=='consumption']
    df4 = df_copy[df_copy['type']=='elcert']
    plt.plot(df2.groupby(df2['weekday'])["num_kwh"].mean(), color = 'green')
    plt.plot(df3.groupby(df3['weekday'])["num_kwh"].mean(), color = 'red')
    plt.plot(df4.groupby(df4['weekday'])["num_kwh"].mean(), color = 'blue')


    df2.groupby(df2['weekday'])["num_kwh"].mean().plot(kind='bar', rot=0, ax=axs)


if __name__ == "__main__":
    plot_timeslot()





