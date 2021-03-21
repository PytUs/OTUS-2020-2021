select month(cast(split_part(trip_start_timestamp,' ',1) as date)) as months,
             year(cast(split_part(trip_start_timestamp,' ',1) as date)) as years,
             count (*) as counts
             FROM chicago_taxi_trips_parquet
             group by months,
             years
order by 3 desc;
