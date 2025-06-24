package objektwerks

import SparkInstance.*

@main def runWordCountApp: Unit =
  import scala3encoders.given

  val lines = sparkSession
    .read
    .textFile("./data/ipa.txt")
  val counts = lines
    .flatMap(line => line.split("\\W+"))
    .filter(_.nonEmpty)
    .groupByKey(_.toLowerCase)
    .count
    .collect
    .map { case (line, count) => Count(line, count) }
  println(s"*** Distinct word count: ${counts.length}")
  println(s"*** Total word count: ${counts.map(_.count).sum}")