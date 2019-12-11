package it.gb1609.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object FindMostPopularFilm extends App {
  def split_lines(line: String) = {
    val fields = line.split("\t")
    (fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3))
  }

  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkContext = new SparkContext("local[*]", "MostPopular")
  val lines = sparkContext.textFile("data_testing/ml-100k/u.data")
  val tuples = lines.map(split_lines)
  val tuplesForCountMovies = tuples.map(x => (x._2, 1))
  val counts = tuplesForCountMovies.reduceByKey((x, y) => x + y)
  val names=sparkContext.broadcast(loadMovieNames)
  val countForNames=counts.map(x=>(names.value(x._1),x._2)).collect()
  countForNames.sortBy(x=>x._2).foreach(println)

}
