library(RMySQL)
require(lubridate)
require(xts)

# Query
mydb = dbConnect(MySQL(), user='', password='', dbname='', host='')
#dbListTables(mydb)

#rs = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank IN ('A','B') ")
v_rs_a = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank = 'A' ")
v_data_a = fetch(v_rs_a, n=-1)
v_rs_b = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank = 'B' ")
v_data_b = fetch(v_rs_b, n=-1)
v_rs_c = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank = 'C' ")
v_data_c = fetch(v_rs_c, n=-1)
v_rs_d = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank = 'D' ")
v_data_d = fetch(v_rs_d, n=-1)
v_rs_e = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank = 'E' ")
v_data_e = fetch(v_rs_e, n=-1)
v_rs_f = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count WHERE rank = 'F' ")
v_data_f = fetch(v_rs_f, n=-1)

rs = dbSendQuery(mydb, "SELECT * FROM influs_propagations_count")
data = fetch(rs, n=-1)


boxplot(data$c)
boxplot.stats(data$c)$stats
hist(data$c)

# Calc DeltaDays
v_earliest = as.Date(data$earliest ,"%Y-%m-%d")
v_latest = as.Date(data$latest ,"%Y-%m-%d")
v_delta = v_latest - v_earliest
v_delta_int = as.integer(v_delta)
hist( v_delta_int )

v_mean_per_day = data$c / (v_delta_int + 1)
boxplot( v_mean_per_day )
boxplot.stats(v_mean_per_day)$stats

# Boxplot v_data_*
v_data_stats = as.matrix( cbind( boxplot.stats(v_data_a$c)$stats, boxplot.stats(v_data_b$c)$stats, boxplot.stats(v_data_c$c)$stats, boxplot.stats(v_data_d$c)$stats, boxplot.stats(v_data_e$c)$stats, boxplot.stats(v_data_f$c)$stats ))
#v_data_stats[5,] / v_data_stats[3,]
# == PHI ?

boxplot(v_data_a$c, v_data_b$c, v_data_c$c, v_data_d$c, v_data_e$c, v_data_f$c)
#boxplot(v_data_d$c, v_data_e$c, v_data_f$c)
title( main = "influs_propagations_count", xlab = "rank", ylab = "Propagations" )
lines(v_data_stats[3,], type="o", col="red")
lines(v_data_stats[5,], type="o", col="red")


# Query v_summary
#rs = dbSendQuery(mydb, "SELECT * FROM v_summary_webproperties_contracts_count")
rs = dbSendQuery(mydb, "SELECT * FROM v_summary_influs_propagations_count")
data_summary = fetch(rs, n=-1)

data_summary$cumsum = data_summary$sum
data_summary$cumsum[5] = data_summary$sum[5] + data_summary$cumsum[6]
data_summary$cumsum[4] = data_summary$sum[4] + data_summary$cumsum[5]
data_summary$cumsum[3] = data_summary$sum[3] + data_summary$cumsum[4]
data_summary$cumsum[2] = data_summary$sum[2] + data_summary$cumsum[3]
data_summary$cumsum[1] = data_summary$sum[1] + data_summary$cumsum[2]

#plot(data_summary$cumsum)

data_summary$cumsumpercent = ( ( data_summary$cumsum / sum(data_summary$sum) ) ) * 100
plot(data_summary$cumsumpercent, xlab = "rank", ylab = "Cumulative Percentage", main = "v_summary_influs_propagations_count")
lines(1:length(data_summary$cumsumpercent), data_summary$cumsumpercent, type="o", col="red")

plot(data_summary$c)

plot(data_summary$cumsumpercent, data_summary$c/sum(data_summary$c)*100 )

data_summary$mean = data_summary$sum / data_summary$c



# Plot v_summary brackets
v_x = sort( c(data_summary$min, data_summary$max) )
v_y_c = rep(data_summary$c, each = 2)
v_y_mean = rep(data_summary$mean, each = 2)
v_df = as.data.frame( cbind(v_x, v_y_c, v_y_mean) )
colnames(v_df) = c("bracket", "c", "mean")

plot(v_df$bracket, v_df$c, xlab = "bracket", ylab = "c")
lines(v_df$bracket, v_df$c, type="o", col="red")

plot(v_df$bracket, v_df$mean, xlab = "bracket", ylab = "mean")
lines(v_df$bracket, v_df$mean, type="o", col="red")


###

#rs = dbSendQuery(mydb, "SELECT * FROM propagations LIMIT 10")
#rs = dbSendQuery(mydb, "SELECT * FROM webproperties_contracts_exposure WHERE rank = 'A' ")
#rs = dbSendQuery(mydb, "SELECT * FROM v_webproperties_api WHERE influencers_used_rank='C' AND exposure_gained_rank='C'")
rs = dbSendQuery(mydb, "SELECT * FROM propagations WHERE webproperty_id='74684cb136559aaca1638b50dfea7919' AND igid='290225410' ")
data = fetch(rs, n=-1)

v_xts_by_date = xts( rep(1, each = length(v_ts_by_date)), sort(as.Date(data$taken_at,"%Y-%m-%d"), decreasing = FALSE) )
v_xts_daily = apply.daily(v_xts_by_date, sum)
plot.xts(v_xts_daily, lty = 0, type="o")
mean(diff(index(v_xts_daily)))
min(index(v_xts_daily))
max(index(v_xts_daily))

####

rs = dbSendQuery(mydb, "SELECT * FROM v_webproperties_api WHERE influencers_used_rank='C' AND exposure_gained_rank='C'")
v_webproperties_api = fetch(rs, n=-1)

rs = dbSendQuery(mydb, "SELECT contracts.* FROM contracts INNER JOIN v_webproperties_api ON contracts.webproperty_id=v_webproperties_api.webproperty_id WHERE influencers_used_rank='C' AND exposure_gained_rank='C'")
v_contracts = fetch(rs, n=-1)

rs = dbSendQuery(mydb, "SELECT propagations.* FROM propagations INNER JOIN contracts ON contracts.webproperty_id=propagations.webproperty_id AND contracts.igid=propagations.igid INNER JOIN v_webproperties_api ON contracts.webproperty_id=v_webproperties_api.webproperty_id WHERE v_webproperties_api.influencers_used_rank='C' AND v_webproperties_api.exposure_gained_rank='C' ")
v_propagations = fetch(rs, n=-1)


v_cumul_influs_stats = c()

for( v_webproperty_id in v_webproperties_api$webproperty_id ) {
  v_influs = v_contracts$igid[grep(v_webproperty_id, v_contracts$webproperty_id)]
  
  for(v_influ in v_influs) {
    v_campaign_sprint = v_propagations$pk[grep(v_influ, v_propagations$igid)]
    v_campaign_dates = v_propagations$taken_at[grep(v_influ, v_propagations$igid)]
    
    v_xts_by_date = xts( rep(1, each = length(v_campaign_dates)), sort(as.Date(v_campaign_dates,"%Y-%m-%d"), decreasing = FALSE) )
    v_xts_daily = apply.daily(v_xts_by_date, sum)
    
    v_xts_daily_i = index(v_xts_daily)
    v_min = min(v_xts_daily_i)
    v_max = max(v_xts_daily_i)
    v_c = length(v_xts_daily_i)
    
    v_mean = 0
    if(v_min != v_max) {
      v_mean = mean(diff(v_xts_daily_i))
    }
    
    v_line_influs_stats = cbind(v_webproperty_id, v_influ, v_c, v_mean, v_min, v_max)
    v_cumul_influs_stats = rbind(v_cumul_influs_stats, v_line_influs_stats)
    
  }
}
v_influs_stats = as.data.frame(v_cumul_influs_stats)
v_influs_stats$v_min_corr = as.Date.numeric(as.numeric(as.character(v_influs_stats$v_min)))
v_influs_stats$v_max_corr = as.Date.numeric(as.numeric(as.character(v_influs_stats$v_max)))
v_influs_stats$v_mean_corr = as.numeric(as.character(v_influs_stats$v_mean))
v_influs_stats$v_c_corr = as.numeric(as.character(v_influs_stats$v_c))


boxplot(v_influs_stats$v_mean_corr)
title( main = "Average Delta Days per Campaign", ylab = "Delta Days" )
boxplot.stats(v_influs_stats$v_mean_corr)$stats

v_influs_stats_non_zero = subset(v_influs_stats, v_mean_corr > 0)
boxplot(v_influs_stats_non_zero$v_mean_corr)
title( main = "Average Delta Days per Campaign", ylab = "Delta Days" )
boxplot.stats(v_influs_stats_non_zero$v_mean_corr)$stats

v_influs_stats_outliers = subset(v_influs_stats, v_mean_corr > 62)
boxplot(v_influs_stats_outliers$v_mean_corr)
title( main = "Average Delta Days per Campaign", ylab = "Delta Days" )
boxplot.stats(v_influs_stats_outliers$v_mean_corr)$stats

v_influs_stats_restrict = subset(v_influs_stats, v_mean_corr > 0)
v_influs_stats_restrict = subset(v_influs_stats_restrict, v_mean_corr < 62)
boxplot(v_influs_stats_restrict$v_mean_corr)
title( main = "Average Delta Days per Campaign", ylab = "Delta Days" )
boxplot.stats(v_influs_stats_restrict$v_mean_corr)$stats

v_influs_stats_restrict = subset(v_influs_stats_restrict, v_c <= 49)
boxplot(v_influs_stats_restrict$v_c)
boxplot.stats(v_influs_stats_restrict$v_c)$stats

####