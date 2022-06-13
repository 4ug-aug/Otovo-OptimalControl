library(forecast)
library(ggplot2)
library(grid)
library(TSstudio)
library(TTR)
library(graphics)

data = read.csv('C:/Users/alexa/datadump/Project-Work---Bsc.-AIDS/data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv', sep=',', header=TRUE)
train <- data[953800:962560,]
test <- data[962561:962585,]

firstHour_train <- 24*(as.Date("2019-05-04 3:00:00")-as.Date("2019-1-1 00:00:00")) # As suggested by Mark S
firstHour_test <- 24*(as.Date("2019-05-10 23:00:00")-as.Date("2019-1-1 00:00:00")) # As suggested by Mark S

train.ts <- ts(train$num_kwh_normalized, start=c(2016,firstHour_train), frequency=24*365.25)
test.ts <- ts(test$num_kwh_normalized, start=c(2019,firstHour_test), frequency=24*365.25)

#multiple seasonality
y <- msts(train.ts, seasonal.periods= c(24,7*24, 365.25*24))
fit <- tbats(y)
fc <- forecast(fit)

# auto-arima
fit <- auto.arima(train.ts, seasonal=TRUE, trace=TRUE)
fc <-predict(fit, n.ahead=24, level=c(95))


