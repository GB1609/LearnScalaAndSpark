package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularHero extends App {

  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split("\\s\"")
    if (fields.length > 1) {
      Some(fields(0).toInt, fields(1).replaceAll("\"", ""))
    }
    else
      None
  }

  def countFriends(line: String) = {
    val fields = line.split("\\s")
    (fields(0).toInt, fields.length - 1)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MostPopularSuperhero")
  val name = sc.textFile("data_testing/Marvel-names.txt").flatMap(parseNames)
  val lines = sc.textFile("data_testing/Marvel-graph.txt")
  val occurrence = lines.map(countFriends)
  val totFriends = occurrence.reduceByKey((x, y) => x + y).map(x => (x._2, x._1))
  val winner = totFriends.max()
  val result = name.lookup(winner._2)(0)
  println(s"$result is the most popular superhero with ${winner._1} co-appearances.")

}
