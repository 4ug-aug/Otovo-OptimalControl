library(forecast)
library(ggplot2)
library(grid)
library(TSstudio)
library(TTR)
library(graphics)

data = read.csv('C:/Users/alexa/datadump/Project-Work---Bsc.-AIDS/data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv', sep=',', header=TRUE)
data = data[data$meter_id=="28ba7f57-6e83-4341-8078-232c1639e4e3",]
data = data[order(as.POSIXlt(data$timeslot, format="%Y-%m-%d %H:00:00+02")),]
train <- data[1:23567,]
test <- data[23568:23592,]

firstHour_train <- 24*(as.Date("2016-09-04 00:00:00")-as.Date("2016-1-1 00:00:00")) # As suggested by Mark S
firstHour_test <- 24*(as.Date("2019-03-30 00:00:00")-as.Date("2019-1-1 00:00:00")) # As suggested by Mark S

train.ts <- ts(train$num_kwh_normalized, start=c(2016,firstHour_train), frequency=24)
test.ts <- ts(test$num_kwh_normalized, start=c(2019,firstHour_test), frequency=24)

#multiple seasonality
y <- msts(train.ts, seasonal.periods= c(24,7*24, 365.25*24))
fit <- tbats(y)
fc <- forecast(fit)

# auto-arima
fit <- auto.arima(train.ts, trace=T)
fc <-forecast(fit)
x <- data.frame(cbind(test.ts, fc$mean[1:25], fc$lower[1:25,2],fc$upper[1:25,2]))
p = ggplot()+geom_line(data=x, aes(x = seq(1,25), color='blue', y=test.ts))+geom_line(data=x, aes(x = seq(1,25),y=fc.lower.1.25..2., color='green'))+geom_line(data=x, aes(x = seq(1,25),y=fc.mean.1.25., color='red'))+geom_line(data=x, aes(x = seq(1,25),y=fc.upper.1.25..2., color='green'))
print(p)
autoplot(fc)

(1/25)*sum((test.ts-fc$mean[1:25])^2)

