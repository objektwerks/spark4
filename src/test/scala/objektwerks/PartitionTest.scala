package objektwerks

import java.util.UUID

import munit.FunSuite

import org.apache.spark.storage.StorageLevel

import SparkInstance.*
import sparkSession.implicits.*

class PartitionTest extends FunSuite {
  import scala3encoders.given

  val dataframe = (1 to 10).toDF("number").persist(StorageLevel.MEMORY_AND_DISK)

  override def afterAll(): Unit = dataframe.unpersist()

  test("partition") { // wide dependency-transformation, full ( evenly distributed ) shuffle
    assert( dataframe.rdd.partitions.length == 8 )
    dataframe.write.csv(s"./target/partitioned-numbers-${UUID.randomUUID.toString}")
  }

  test("coalesce") { // narrow dependency-transformation, not a full shuffle, designed to reduce partitions
    val coalesced = dataframe.coalesce(2)
    assert( coalesced.rdd.partitions.length == 2 )
    coalesced.write.csv(s"./target/coalesced-numbers-${UUID.randomUUID.toString}")
  }

  test("repartition") {
    dataframe.repartition(4).rdd.partitions.length shouldEqual 4
    dataframe.repartition(2).rdd.partitions.length shouldEqual 2
  }

  test("partitionBy") {
    val persons = sparkSession.read.json("./data/person/person.json").cache
    val file = s"./target/partitionby-roles-${UUID.randomUUID.toString}"
    persons
      .repartition(2)
      .write
      .partitionBy("role")
      .parquet(file)
  }
}