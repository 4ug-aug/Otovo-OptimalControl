library(forecast)
library(ggplot2)
library(grid)
library(TSstudio)
library(TTR)
library(graphics)

hf_prod <- read.csv('prod.csv', sep=',', header=TRUE)
hf_cons <- read.csv('cons.csv', sep=',', header=TRUE)

# create time series

prod_ts <- ts(hf_prod$num_kwh, start=c(2017,1),frequency=8760)
#prod_ts <- ts(hf_prod$num_kwh, start=c(2016,9),frequency=8760)
plot.ts(prod_ts)

cons_ts <- ts(hf_cons$num_kwh, start=c(2017,1),frequency=8760)
#cons_ts <- ts(hf_cons$num_kwh, start=c(2016,9),frequency=8760)
plot.ts(cons_ts)

#smoothing

plot.ts(SMA(cons_ts, n=90))
plot.ts(SMA(prod_ts, n=90))

# decompose

consComp <- decompose(cons_ts)
plot(consComp)

plot(stl(cons_ts), s.window="periodic")

prodComp <- decompose(prod_ts)
plot(prodComp)

# seasonal shifting

consAdj <- cons_ts - consComp$seasonal
plot.ts(consAdj)

# forecasting w ARIMA
library(forecast)

auto.arima(cons_ts)

