# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 15:35:11 2022

@author: vidis
"""

import pandas as pd
import matplotlib.pyplot as plt
import dask.dataframe as dd

dtype={'adjusts_id': 'object',
       'ediel_product_code': 'float64',
       'invoice_item_id': 'object',
       'parent_id': 'object'}

df_main = pd.read_csv('/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/meter_ids_data/3ba47f27-33e8-4764-a390-d33ca37d625f.csv', 
delimiter=',', header=0)

def plot_timeslot():
    df_copy = df_main
    print(df_copy.columns)

    def dtextract(x):
        df_copy[x] = pd.to_datetime(df_copy[x], utc=True)
        df_copy["year"] = df_copy[x].dt.year
        df_copy["weekly"] = df_copy[x].dt.isocalendar().week
        df_copy['month'] = df_copy[x].dt.month
        df_copy['hour'] = df_copy[x].dt.hour
        df_copy['weekday'] = df_copy[x].dt.weekday

    dtextract('created_at')

    fig, ((axs1, axs2), (axs3, axs4)) = plt.subplots(2,2)
    df2 = df_copy[df_copy['type']=='production']
    df3 = df_copy[df_copy['type']=='consumption']
    df4 = df_copy[df_copy['type']=='elcert']

    axs1.plot(df2.groupby(df2['month'])["num_kwh"].mean(), color = 'green')
    axs1.plot(df3.groupby(df3['month'])["num_kwh"].mean(), color = 'red')
    axs1.plot(df4.groupby(df4['month'])["num_kwh"].mean(), color = 'blue')
    axs1.set_title("Monthly")
    axs1.legend(["production","consumption","elcert"])

    axs2.plot(df2.groupby(df2['weekday'])["num_kwh"].mean(), color = 'green')
    axs2.plot(df3.groupby(df3['weekday'])["num_kwh"].mean(), color = 'red')
    axs2.plot(df4.groupby(df4['weekday'])["num_kwh"].mean(), color = 'blue')
    axs2.set_title("Weekday")
    axs2.legend(["production","consumption","elcert"])

    axs3.plot(df2.groupby(df2['hour'])["num_kwh"].mean(), color = 'green')
    axs3.plot(df3.groupby(df3['hour'])["num_kwh"].mean(), color = 'red')
    axs3.plot(df4.groupby(df4['hour'])["num_kwh"].mean(), color = 'blue')
    axs3.set_title("Hourly")
    axs3.legend(["production","consumption","elcert"])

    axs4.plot(df2.groupby(df2['weekly'])["num_kwh"].mean(), color = 'green')
    axs4.plot(df3.groupby(df3['weekly'])["num_kwh"].mean(), color = 'red')
    axs4.plot(df4.groupby(df4['weekly'])["num_kwh"].mean(), color = 'blue')
    axs4.set_title("Weekly")
    axs4.legend(["production","consumption","elcert"])
    

    # df2.groupby(df2['weekday'])["num_kwh"].mean().plot(kind='bar', rot=0, ax=axs)
    plt.show();


if __name__ == "__main__":
    plot_timeslot()





