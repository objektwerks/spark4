package objektwerks

import munit.FunSuite

import org.apache.spark.sql.{Dataset, SaveMode}

import scala3encoders.given

import SparkInstance.*

class DataSourceTest extends FunSuite:
  test("csv"):
    val dataframe = sparkSession
      .read
      .format("csv")
      .option("delimiter",",")
      .option("inferSchema","true")
      .load("./data/friends.txt")
      .cache
    dataframe.count shouldBe 500

    val friends = dataframe.map(row => Friend(row.getInt(0), row.getString(1), row.getInt(2), row.getInt(3)))
    friends.count shouldBe 500

  test("text"):
    val dataframe = sparkSession.read.text("./data/license.txt")
    dataframe.count shouldBe 19

    val dataset = sparkSession.read.textFile("./data/license.txt")
    dataset.count shouldBe 19

  test("json"):
    val dataframe = sparkSession.read.json("./data/person.json")
    dataframe.count shouldBe 4

    val dataset = sparkSession.read.json("./data/person.json").as[Person]
    dataset.count shouldBe 4

  test("parquet"):
    val dataset = sparkSession.read.json("./data/person.json").as[Person]

    dataset.write.parquet("./target/person.parquet")

    val parquet = sparkSession.read.parquet("./target/parquet/person.parquet").as[Person]

    parquet.createOrReplaceTempView("persons")
    val persons = sparkSession.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    persons.count shouldBe 2
    persons.head.name shouldBe "betty"
    persons.head.age shouldBe 21