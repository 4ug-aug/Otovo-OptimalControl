library(forecast)
library(ggplot2)
library(grid)
library(TSstudio)
library(TTR)
library(graphics)
library(xts)
library(reshape2)

data = read.csv('C:/Users/vidis/OneDrive/Desktop/Summer2022/Project Work/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv', sep=',', header=TRUE)
data = data[data$meter_id=="e882f9a7-f1de-4419-9869-7339be303281",]
data = data[order(as.POSIXlt(data$timeslot, format="%Y-%m-%d %H:00:00+02")),]

data = data[(data$timeslot>='2017-09-15 00:00:00+02'),]
data = data[order(as.POSIXlt(data$timeslot, format="%Y-%m-%d %H:00:00+02")),]

## frequency by day, initial size = 1440

iniTrain<- 1440
step_size <- 24
MSEs <- c()
method <- c()
forecasted <- c()
f_upper_bounds <- c()
f_lower_bounds <- c()

for (i in 0:10){
  
  d <- (iniTrain+step_size*i)
  e <- iniTrain-1
  train <- data[(d-e):((iniTrain-1)+d-e),]
  test <- data[(iniTrain+d-e):(iniTrain+step_size-1+d-e),]
  
  train.ts <- ts(train$num_kwh, frequency=24)
  
  fit <- auto.arima(train.ts, seasonal=TRUE, trace=TRUE, D=1)
  fc <- forecast(fit, h=24, level=c(95))
  
  x <- data.frame(cbind(test$num_kwh, fc$mean, fc$lower,fc$upper))
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

a <- data.frame(index=seq(1,length(data[1:length(forecasted),3])), actual= data[1:length(forecasted),12], forecasted=forecasted, UB=f_upper_bounds, LB=f_lower_bounds)
p = ggplot(a, aes(index)) + 
  geom_line(aes(y=actual, colour="actual")) +
  geom_line(aes(y=forecasted, colour="forecasted"))+
  geom_ribbon(aes(ymin=LB, ymax=UB), alpha=0.2)+
  labs(x='index', y='norm_consumption', color='Legend')+
  scale_color_manual(name='Values', values = c('actual'='orange', 'forecasted'='blue') )
print(p)

print(mean(sqrt(MSEs)))

plot(sqrt(MSEs))


