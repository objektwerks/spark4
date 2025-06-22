package objektwerks

import org.apache.spark.sql.SparkSession

@main def runWordCountApp: Unit =
  val sparkSession = SparkSession.builder().master("local").getOrCreate

  import sparkSession.implicits.*
  import scala3encoders.given

  val lines = sparkSession.read.textFile("./data/ipa.txt")
  val counts = lines
    .flatMap(line => line.split("\\W+"))
    .filter(_.nonEmpty)
    .groupByKey(_.toLowerCase)
    .count
    .collect
    .map { case (line, count) => Count(line, count) }
  println(s"*** Word count: $counts")