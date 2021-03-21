SELECT company,round(sum(trip_total),0) as totals FROM chicago_taxi_trips_parquet
group by company
order by 2 desc limit 10;
