import pandas as pd
import numpy as np
from Load_R_Functions import utils, base, TSA, forecast, stats, tsbox
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

loc = "C:/Users/alexa/datadump/files from aws/meter.csv" #the aggregated meter

df_pd = pd.read_csv(loc, sep=",")
df_pd = df_pd[["timeslot","num_kwh"]]
with localconverter(ro.default_converter + pandas2ri.converter):
    df_r2= ro.conversion.py2rpy(df_pd)
    
#This takes so fucking long don't run the code it'll never be fucking over omg
tsdata = tsbox.ts_ts(tsbox.ts_long(df_r2))

forecast.auto_arima(tsdata)

if __name__== "__main__":
    print("PooPooPeePee")
