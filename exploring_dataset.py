# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 15:35:11 2022

@author: vidis
"""

import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import dask.dataframe as dd
from load_data import load_dataset

dtype={'adjusts_id': 'object',
       'ediel_product_code': 'float64',
       'invoice_item_id': 'object',
       'parent_id': 'object'}

df_main = pd.read_csv('/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/meter_ids_data/3ba47f27-33e8-4764-a390-d33ca37d625f.csv', 
                        delimiter=',', header=0)

df_whole = load_dataset("/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/gridtx-dump.csv")

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

    fig, axs = plt.subplots(nrows = 3, ncols = 2)
    df2 = df_copy[df_copy['type']=='production']
    df3 = df_copy[df_copy['type']=='consumption']
    df4 = df_copy[df_copy['type']=='elcert']

    axs[0,0].plot(df2.groupby(df2['month'])["num_kwh"].mean(), color = 'green')
    axs[0,0].plot(df3.groupby(df3['month'])["num_kwh"].mean(), color = 'red')
    axs[0,0].plot(df4.groupby(df4['month'])["num_kwh"].mean(), color = 'blue')
    axs[0,0].set_title("Monthly", fontsize=10)
    axs[0,0].legend(["production","consumption","elcert"])

    axs[0,1].plot(df2.groupby(df2['weekday'])["num_kwh"].mean(), color = 'green')
    axs[0,1].plot(df3.groupby(df3['weekday'])["num_kwh"].mean(), color = 'red')
    axs[0,1].plot(df4.groupby(df4['weekday'])["num_kwh"].mean(), color = 'blue')
    axs[0,1].set_title("Weekday", fontsize=10)
    axs[0,1].legend(["production","consumption","elcert"])

    axs[1,0].plot(df2.groupby(df2['hour'])["num_kwh"].mean(), color = 'green')
    axs[1,0].plot(df3.groupby(df3['hour'])["num_kwh"].mean(), color = 'red')
    axs[1,0].plot(df4.groupby(df4['hour'])["num_kwh"].mean(), color = 'blue')
    axs[1,0].set_title("Hourly", fontsize=10)
    axs[1,0].legend(["production","consumption","elcert"])

    axs[1,1].plot(df2.groupby(df2['weekly'])["num_kwh"].mean(), color = 'green')
    axs[1,1].plot(df3.groupby(df3['weekly'])["num_kwh"].mean(), color = 'red')
    axs[1,1].plot(df4.groupby(df4['weekly'])["num_kwh"].mean(), color = 'blue')
    axs[1,1].set_title("Weekly", fontsize=10)
    axs[1,1].legend(["production","consumption","elcert"])
    
    axs[2,0].plot(df3.groupby(df3['weekly'])["num_kwh"].mean(), color = 'red')
    axs[2,0].plot(df3.groupby(df3['weekly'])["amount_no_vat"].mean(), color = 'green')
    axs[2,0].set_title("Amount no VAT vs Consumption", fontsize=10)
    axs[2,0].legend(["Consumption","Spot Price"])

    axs[2,1].remove()

    plt.show();

def describe_whole_dataset():
    df_copy = df_whole
    print("Missing values in the dataset based on columns:")
    nans = df_copy.isnull().sum().compute()
    print(nans)
    print(f"Total nans: {nans.sum()}")

    print("Different sized meter_id occurences")
    value_counts_ = df_copy["meter_id"].value_counts().compute()
    print(value_counts_)

def distribution_of_measurements():
    df_copy = df_whole
    value_counts_ = df_copy["meter_id"].value_counts().compute()
    plot = px.histogram(value_counts_, x="meter_id")
    plot.show();

if __name__ == "__main__":
    distribution_of_measurements()






