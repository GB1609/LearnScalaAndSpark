package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object FilteringByLoc extends App {
  def split_string(line: String) = {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toInt)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkContext = new SparkContext("local[*]", "Filtering")
  val lines = sparkContext.textFile("data_testing/1800.csv")
  val infos = lines.map(line => {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toFloat)
  })
  val min_type=infos.filter(x=>x._2=="TMIN")
  val station_temp= min_type.map(x=> (x._1,x._3.toFloat))

  //to find min for every station
  val min_for_stations=station_temp.reduceByKey((x,y)=>Math.min(x,y))
  val result=min_for_stations.collect()
  result.sorted.foreach(println)
}
