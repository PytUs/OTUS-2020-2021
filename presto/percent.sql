select
round(sum(case when trip_miles < 5 then 1 else 0 end)*100/cast(count(*) as DOUBLE),2) as less_5_miles,
round(sum(case when trip_miles >= 5 and trip_miles <= 15 then 1 else 0 end)*100/cast(count(*) as DOUBLE),2) as miles_5_15,
round(sum(case when trip_miles >= 16 and trip_miles <= 25 then 1 else 0 end)*100/cast(count(*) as DOUBLE),2) as miles_5_1516_25,
round(sum(case when trip_miles >= 26 and trip_miles <= 100 then 1 else 0 end)*100/cast(count(*) as DOUBLE),2) as miles_26_100
from chicago_taxi_trips_parquet;
