package objektwerks

import buildinfo.BuildInfo.inputDirectory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*

import spark.implicits.*
import scala3encoders.given

@main def runWordCountApp: Unit =
  val spark = SparkSession.builder().master("local").getOrCreate

  val sc = spark.sparkContext

  val textFile = sc.textFile(inputDirectory.getPath + "/data/ipa.txt")
  val words: Dataset[String] = textFile.flatMap(line => line.split(" ")).toDS

  val counts: Dataset[(String, Double)] =
    words
      .map(word => (word, 1d))
      .groupByKey((word, _) => word)
      .reduceGroups((a, b) => (a._1, a._2 + b._2))
      .map(_._2)