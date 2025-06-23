package objektwerks

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SparkInstance:
  def session(): SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-app")
      .getOrCreate

  def sessionAndContext(): (SparkSession, SparkContext) =
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-app")
      .getOrCreate
    val context = session.sparkContext
    (session, context)