library(forecast)
library(ggplot2)
library(grid)
library(TSstudio)
library(TTR)
library(graphics)
library(xts)
library(reshape2)

data = read.csv('C:/Use`rs/vidis/OneDrive/Desktop/Summer2022/Project Work/sorted_cons.csv', sep=',', header=TRUE)

data = data[(data$timeslot>='2017-09-15 00:00:00+02'),]

## frequency by day, initial size = 1440

iniTrain<- 1440
step_size <- 24
MSEs <- c()
method <- c()
forecasted <- c()
f_upper_bounds <- c()
f_lower_bounds <- c()

for (i in 0:544){
  
  d <- (iniTrain+step_size*i)
  e <- iniTrain-1
  train <- data[(d-e):((iniTrain-1)+d-e),]
  test <- data[(iniTrain+d-e):(iniTrain+step_size-1+d-e),]
  
  train.ts <- ts(train$num_kwh, frequency=24)
  
  fit <- auto.arima(train.ts, seasonal=TRUE, trace=FALSE, D=1)
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

# for last fold
d <- (1440+23*545)
e <- 1440-1
train <- data[(d-e):((1440-1)+d-e),]
test <- data[(1440+d-e):(1440+23-1+d-e),]

train.ts <- ts(train$num_kwh, frequency=24)

fit <- auto.arima(train.ts, seasonal=TRUE, trace=FALSE, D=1)
fc <- forecast(fit, h=23, level=c(95))

x <- data.frame(cbind(test$num_kwh, fc$mean, fc$lower,fc$upper))
p = ggplot()+geom_line(data=x, aes(x = seq(1,23), y=x[,1], colour='actual'))+geom_line(data=x, aes(x = seq(1,23),y=x[,2], colour='forecasted'))+geom_line(data=x, aes(x = seq(1,23),y=x[,3], colour='lower bound'))+geom_line(data=x, aes(x = seq(1,23),y=x[,4], color='upper bound'))+labs(color = "Value")
print(p)
mse <- (1/23)*sum((x[,1]-x[,2])^2)
MSEs <- append(MSEs, mse)
method <- append(method, fc$method)
forecasted <- append(forecasted, fc$mean)
f_upper_bounds <- append(f_upper_bounds, fc$upper)
f_lower_bounds <- append(f_lower_bounds, fc$lower)

print(545)
print(c(mse, mean(MSEs)))
print(fc$method)




library(reshape)

a <- data.frame(index=seq(1,length(data[1:length(forecasted),3])), actual= data[1441:(length(forecasted)+1440),5], forecasted=forecasted, UB=f_upper_bounds, LB=f_lower_bounds)

write.csv(a,"C:/Users/vidis/OneDrive/Desktop/Summer2022/Project Work/final_SARIMA_cons.csv", row.names = FALSE)
write.csv(MSEs,"C:/Users/vidis/OneDrive/Desktop/Summer2022/Project Work/final_SARIMA_MSEs_cons.csv", row.names = FALSE)
write.csv(method,"C:/Users/vidis/OneDrive/Desktop/Summer2022/Project Work/final_SARIMA_method_cons.csv", row.names = FALSE)

b <- melt(a, id.vars='index', variable_name='variable')
p = ggplot(b, aes(x=index, y=ma(value, order=24, centre=TRUE), color=variable))+geom_line(alpha=0.5)+ guides(colour = guide_legend(override.aes = list(alpha = 1)))
print(p)

print(mean(sqrt(MSEs)))

plot(sqrt(MSEs))





