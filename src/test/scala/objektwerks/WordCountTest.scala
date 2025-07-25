package objektwerks

import munit.FunSuite

import scala3encoders.given

import SparkInstance.*

class WordCountTest extends FunSuite:
  test("dataset"):
    val lines = sparkSession.read.textFile("./data/ipa.txt")
    val counts = lines
      .flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .collect
      .map { case (line, count) => Count(line, count) }
    assert( counts.length == 122 )
    assert( counts.map(_.count).sum == 196 )

  test("dataframe"):
    val lines = sparkSession.read.textFile("./data/ipa.txt").toDF("line")
    val counts = lines
      .flatMap(row => row.getString(0).split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .collect
    assert( counts.length == 122 )
    assert( counts.map(_._2).sum == 196 )

  test("structured streaming"):
    sparkSession
      .readStream
      .text("./data/words")
      .flatMap(row => row.getString(0).split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .writeStream
      .format("memory")
      .queryName("words")
      .outputMode("complete")
      .start()
      .awaitTermination(6000L)
    val words = sparkSession.sql("select * from words")
    assert( words.count == 122 )
    assert( words.map(_.getLong(1)).collect.sum == 196 )