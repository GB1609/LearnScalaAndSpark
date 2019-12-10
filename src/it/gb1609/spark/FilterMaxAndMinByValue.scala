package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object FilterMaxAndMinByValue extends App {
  def split_string(line: String) = {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toInt)
  }

  def print_Mod(line:(String,Float,Float))={
    println(f"LOCATION: ${line._1}, MAX: ${line._3}, MIN: ${line._2}")

  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkContext = new SparkContext("local[*]", "Filtering")
  val lines = sparkContext.textFile("data_testing/1800.csv")
  val infos = lines.map(line => {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toFloat)
  })
  val min_type=infos.filter(x=>x._2=="TMIN")
  val max_type=infos.filter(x=>x._2=="TMAX")
  val station_temp_min= min_type.map(x=> (x._1,x._3.toFloat))
  val station_temp_max=max_type.map(x=> (x._1,x._3.toFloat))


  val min_for_stations=station_temp_min.reduceByKey((x,y)=>Math.min(x,y))
  val max_for_stations=station_temp_max.reduceByKey((x,y)=>Math.max(x,y))
  val result=min_for_stations.join(max_for_stations).collect().map(x=> (x._1,x._2._1,x._2._2))
  result.foreach(print_Mod)
}
