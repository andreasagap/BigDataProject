setwd("/home/andreas/Desktop/")
library(tidyverse)
data<-read_csv("results_ohe_task2.csv")
# data<-data %>% select(-DateTime)
data %>%
  summarize_all(list(mean=mean, median=median, min = min, max = max)
  ) %>%
  pivot_longer(everything()) %>%
  separate(name, into = c("var","stat")) %>%
  pivot_wider(names_from = stat, values_from=value)

drops <- c("correlated")
data = data[ , !(names(data) %in% drops)]

data = data %>% mutate(n_samples = log10(n_samples))

# data = data %>% mutate(anti_n = log10(n_samples)*anticorrelated)
# data = data %>% mutate(norm_n = log10(n_samples)*normal)
# data = data %>% mutate(uni_n = log10(n_samples)*uniform)

# anti = data %>% filter(anticorrelated == 1) %>% select(dims, n_samples, time)
# corr = data %>% filter(correlated == 1) %>% select(dims, n_samples, time)
# norm = data %>% filter(normal == 1) %>% select(dims, n_samples, time)
# uni = data %>% filter(uniform == 1) %>% select(dims, n_samples, time)


# data %>% cor()

model<-lm(time~., data=data)

summ = model %>%
  summary() # model summary

summary_list = summ["coefficients"]
print(typeof(summ))
lapply(summary_list, function(x) write.table( data.frame(x), 'lm_summary.csv'  , append= T, sep=',' ))



