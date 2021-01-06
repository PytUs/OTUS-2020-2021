package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


//Creating case classes
case class Total (district:String,crimes_total:Double,lat:Double,lng:Double)//for total count crimes and coordinate averages
case class Median (districts:String,crimes_monthly:Double)//for median of number of crimes per month/year
case class Top (dist:String,crime_type:String,count:Long) //case class for top 3 for each district
case class Ftop (dist:String,crime_type:String) //case class for top 3 crime-types in the string per district
//creating object MyCode
object MyCode extends  App {
  //Path files in my computer for dataframes with crime_total,median of number crimes per month and finally parquet file

  //values for jar file
  val crime: String =  args(0)
  val final_result: String =args(2)
  //Sparksession for working
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  //Definition value for dataframe Total
  val crime_read = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",","header" -> "true")).csv(crime)
  val crime_total= crime_read
    .select($"district", $"Lat", $"Long")
    .groupBy($"district")
    .agg(
      count($"*").as("crimes_total"),
      avg($"Lat").as("lat"),
      avg($"Long").as("lng"))
    .as[Total]
  //.show(false)
  //Dataframe Median
  val crime_month = crime_read
    .select($"district" as "districts", $"YEAR", $"MONTH")
    .groupBy($"districts", $"YEAR", $"MONTH")
    .count()
    .createOrReplaceTempView("crime_month")
  val crime_median= spark.sql("SELECT districts as districts,percentile_approx(count,0.5) as crimes_monthly FROM crime_month group by districts")
    .as[Median]
  //.show(false)
  //Preparation data Offence_codes for top 3.


  //value for jar file
  val ofcodes: String = args(1)
  val ofc = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(ofcodes)
  //Join data ofcs and form top 3 per district
  val joined_data = crime_read
    .join(ofc, crime_read("OFFENSE_CODE") === ofc("CODE"))
    .withColumn("NAME", split($"NAME", "-")(0))
    .select($"district" as "dist",$"NAME" as "crime_type")
    .groupBy($"dist",$"crime_type")
    .count()
    .orderBy('count.desc)
    .as[Top]
    .groupByKey(_.dist)
    .flatMapGroups{case(keys,iter)=>iter.toList.sortBy(x => - x.count).take(3)
    }
    .select($"dist",$"crime_type")
    .as[Ftop] //for transpose crime types to string
    .groupByKey(_.dist)
    .mapGroups{case (key,crime)=>(key,crime.map(x=> x.crime_type).mkString(", "))}
    .select($"_1" as "dist",$"_2" as "crime_type")
    .as[Ftop]
  //.show(false)
  //Joining dataframes to final unit and writing parquet file
  val finalFrame = crime_total
    .join(broadcast(crime_median), crime_total("district") <=> crime_median("districts"))
    .join(broadcast(joined_data),crime_total("district")<=>joined_data("dist"))
    .select("district","crimes_total","crimes_monthly","crime_type","lat","lng")
    .repartition(1)
    .toDF()
    //.coalesce(1)
    .write.parquet(final_result)
  //.show()

}
