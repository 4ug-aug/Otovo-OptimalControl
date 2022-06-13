library(forecast)
library(ggplot2)
library(grid)
library(TSstudio)
library(TTR)
library(graphics)

data = read.csv('C:/Users/vidis/OneDrive/Desktop/Summer2022/Project Work/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv', sep=',', header=TRUE)
data = data[data$meter_id=="28ba7f57-6e83-4341-8078-232c1639e4e3",]
data = data[order(as.POSIXlt(data$timeslot, format="%Y-%m-%d %H:00:00+02")),]
train <- data[22847:23567,]
test <- data[23568:23592,]

firstHour_train <- 24*(as.Date("2019-02-18 00:00:00")-as.Date("2019-1-1 00:00:00")) # As suggested by Mark S
firstHour_test <- 24*(as.Date("2019-03-30 00:00:00")-as.Date("2019-1-1 00:00:00")) # As suggested by Mark S

train.ts <- ts(train$num_kwh_normalized, start=c(2019,firstHour_train), frequency=24*365.25)
test.ts <- ts(test$num_kwh_normalized, start=c(2019,firstHour_test), frequency=24*365.25)

#multiple seasonality
y <- msts(train$num_kwh_normalized, seasonal.periods= c(24,7*24))
fit <- tbats(y, use.box.cox=FALSE, use.arma.errors = FALSE, use.parallel = TRUE)
fc <- forecast(fit)

## frequency day, initial size = 720 ()

iniTrain<- 1440
step_size <- 24
MSEs <- c()
method <- c()
forecasted <- c()
f_upper_bounds <- c()
f_lower_bounds <- c()

for (i in 0:953){
  
  d <- (iniTrain+step_size*i)
  train <- data[d:((iniTrain-1)+d),]
  test <- data[(iniTrain+d):(iniTrain+23+d),]
  
  train.ts <- ts(train$num_kwh_normalized, frequency=24)
  
  fit <- auto.arima(train.ts, seasonal=TRUE, trace=FALSE, D=1)
  fc <- forecast(fit, h=24, level=c(95))
  
  x <- data.frame(cbind(test$num_kwh_normalized, fc$mean, fc$lower,fc$upper))
  p = ggplot()+geom_line(data=x, aes(x = seq(1,24), y=x[,1], colour='actual'))+geom_line(data=x, aes(x = seq(1,24),y=x[,2], colour='forecasted'))+geom_line(data=x, aes(x = seq(1,24),y=x[,3], colour='lower bound'))+geom_line(data=x, aes(x = seq(1,24),y=x[,4], color='upper bound'))+labs(color = "Value")
  print(p)
  mse <- (1/24)*sum((x[,1]-x[,2])^2)
  MSEs <- append(MSEs, mse)
  method <- append(method, fc$method)
  forecasted <- append(forecasted, fc$mean)
  f_upper_bounds <- append(f_upper_bounds, fc$upper)
  f_lower_bounds <- append(f_lower_bounds, fc$lower)
  
  print(i)
  print(c(mse, mean(MSEs)))
  print(fc$method)
  
}

print(mean(MSEs))

