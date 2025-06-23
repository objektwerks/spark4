package objektwerks

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

object SparkInstance:
  private val logger = LogManager.getLogger(SparkInstance.getClass)

  val sparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-app")
      .getOrCreate
  val sparkContext = sparkSession.sparkContext
  logger.info("*** Initialized Spark Session instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    logger.info("*** Terminated Spark Session instance.")
  }