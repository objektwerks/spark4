package objektwerks

import org.apache.spark.sql.SparkSession

final case class Count(value: String, count: Long)

@main def runWordCountApp: Unit =
  val sparkSession = SparkSession
    .builder()
    .master("local")
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    .getOrCreate

  import scala3encoders.given

  val lines = sparkSession.read.textFile("./data/ipa.txt")
  val counts = lines
    .flatMap(line => line.split("\\W+"))
    .filter(_.nonEmpty)
    .groupByKey(_.toLowerCase)
    .count
    .collect
    .map { case (line, count) => Count(line, count) }
  println(s"*** Word count: ${counts.length}")
  for (count <- counts) println(s"$count")