package objektwerks

import org.apache.spark.sql.SparkSession

object SparkInstance:
  def create(): SparkSession =
    SparkSession
      .builder()
      .master("local")
      .getOrCreate