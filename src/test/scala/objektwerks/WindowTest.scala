package objektwerks

import munit.FunSuite

import org.apache.spark.sql.functions.*

import scala3encoders.given

import SparkInstance.*
import sparkSession.implicits.*

/**
  * This test is inspired by this article:
  * https://towardsdatascience.com/spark-3-2-session-windowing-feature-for-streaming-data-e404d92e267
  */
class WindowTest extends FunSuite:
  val data = List(
    Event(1, "2023-01-02 15:30:00"),
    Event(1, "2023-01-02 15:30:30"),
    Event(1, "2023-01-02 15:31:00"),
    Event(1, "2023-01-02 15:31:50"),
    Event(1, "2023-01-02 15:31:55"),
    Event(2, "2023-01-02 15:33:00"),
    Event(2, "2023-01-02 15:35:20"),
    Event(2, "2023-01-02 15:37:00"),
    Event(3, "2023-01-02 15:30:30"),
    Event(3, "2023-01-02 15:31:00"),
    Event(3, "2023-01-02 15:31:50"),
    Event(3, "2023-01-02 15:31:55"),
    Event(3, "2023-01-02 15:33:00"),
    Event(3, "2023-01-02 15:35:20"),
    Event(3, "2023-01-02 15:37:00"),
    Event(3, "2023-01-02 15:40:00"),
    Event(3, "2023-01-02 15:45:00"),
    Event(3, "2023-01-02 15:46:00"),
    Event(3, "2023-01-02 15:47:30"),
    Event(3, "2023-01-02 15:48:00"),
    Event(3, "2023-01-02 15:48:10"),
    Event(3, "2023-01-02 15:48:20"),
    Event(3, "2023-01-02 15:48:30"),
    Event(3, "2023-01-02 15:50:00"),
    Event(3, "2023-01-02 15:53:00"),
    Event(3, "2023-01-02 15:54:30"),
    Event(3, "2023-01-02 15:55:00"),
    Event(4, "2023-01-02 15:50:30"),
    Event(4, "2023-01-02 15:52:00"),
    Event(4, "2023-01-02 15:50:30"),
    Event(4, "2023-01-02 15:52:00"),
    Event(4, "2023-01-02 15:50:30"),
    Event(4, "2023-01-02 15:52:00")
  )
  val dataset = data.toDS().cache()

  test("tumbling"):
    val dataframe = dataset
      .withWatermark(eventTime = "datetime", delayThreshold = "10 minutes")
      .groupBy(
        col("id"),
        window(timeColumn = col("datetime"), windowDuration = "10 minutes")
      )
      .count()
    assert( dataframe.collect().nonEmpty )

  test("sliding") {
    val dataframe = dataset
      .withWatermark(eventTime = "datetime", delayThreshold = "10 minutes")
      .groupBy(
        col("id"),
        window(timeColumn = col("datetime"), windowDuration = "10 minutes", slideDuration = "5 minutes")
      )
      .count()
    assert( dataframe.collect().nonEmpty )
  }

  test("session") {
    val dataframe = dataset
      .withWatermark(eventTime = "datetime", delayThreshold = "10 minutes")
      .groupBy(
        col("id"),
        session_window(timeColumn = col("datetime"), gapDuration = "5 minutes")
      )
      .count()
    assert( dataframe.collect().nonEmpty )
  }

  test("dynamic session") {
    val dataframe = dataset
      .withWatermark(eventTime = "datetime", delayThreshold = "10 minutes")
      .groupBy(
        col("id"),
        session_window(
          timeColumn = col("datetime"),
          gapDuration = when(col("id") === lit(3), "10 seconds")
                        .when(col("id") === lit(1),"30 seconds")
                        .otherwise("10 minutes")
        )
      )
      .count()
    assert( dataframe.collect().nonEmpty )
  }