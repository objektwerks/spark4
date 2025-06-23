package objektwerks

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SparkInstance:
  def session(): SparkSession =
    SparkSession
      .builder()
      .master("local")
      .getOrCreate

  def sessionAndContext(): (SparkSession, SparkContext) =
    val session = SparkSession
      .builder()
      .master("local")
      .getOrCreate
    val context = session.sparkContext
    (session, context)