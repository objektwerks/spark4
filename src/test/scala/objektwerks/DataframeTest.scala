package objektwerks

import munit.FunSuite

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.*
import org.apache.spark.sql.functions.*
import org.apache.spark.storage.StorageLevel

import scala3encoders.given

import SparkInstance.*

class DataframeTest extends FunSuite {
  val dataframe = sparkSession.read.json("./data/person.json").persist(StorageLevel.MEMORY_ONLY)
  dataframe.write.json("./target/dataframe/person.json")

  //override def afterAll(): Unit = dataframe.unpersist()

  test("dataframe") {
    assert( dataframe.count == 4 )
    assert( dataframe.isInstanceOf[Dataset[Row]] )
    assert( dataframe.as[Person].isInstanceOf[Dataset[Person]] )
  }

  test("column") {
    val idColumn = dataframe.col("id")
    val nameColumn = col("name")
    val ageColumn = column("age")
    val roleColumn = expr("role")
    assert( dataframe.select(idColumn, nameColumn, ageColumn, roleColumn).count == 4 )
  }

  test("selectExpr") {
    assert( dataframe.selectExpr("id", "name", "age", "role").count == 4 )
  }

  test("add column") {
    import sparkSession.implicits.*

    assert(
      dataframe
      .withColumn("dog_age", $"age" * 7)
      .head.getLong(4) == 168
    )
  }

  test("update") {
    import sparkSession.implicits.*

    val incrementAgeNameToUpper = dataframe
      .withColumn("age", $"age" + 1)
      .withColumn("name", upper($"name"))
      .cache
    assert( incrementAgeNameToUpper.count == 4 )
    assert( incrementAgeNameToUpper.head.getLong(0) == 25 )
    assert( incrementAgeNameToUpper.head.getString(2) == "FRED" )
  }

  test("transform") {
    import sparkSession.implicits.*

    def incrementAge(df: DataFrame): DataFrame = df.withColumn("age", $"age" + 1)
    def nameToUpper(df: DataFrame): DataFrame = df.withColumn("name", upper($"name"))
    val incrementAgeNameToUpper = dataframe
      .transform(incrementAge)
      .transform(nameToUpper)
      .cache
    assert( incrementAgeNameToUpper.count == 4 )
    assert( incrementAgeNameToUpper.head.getLong(0) == 25 )
    assert( incrementAgeNameToUpper.head.getString(2) == "FRED" )
  }

  test("filter") {
    val filterByName = dataframe.filter("name == 'barney'").cache
    assert( filterByName.count == 1 )
    assert( filterByName.head.getAs[Long]("age") == 22 )
    assert( filterByName.head.getAs[String]("name") == "barney" )
    assert( filterByName.head.getAs[String]("role") == "husband" )
  }

  test("select > where") {
    val selectByName = dataframe
      .select("name")
      .where("name == 'barney'")
      .cache
    assert( selectByName.count == 1 )
    assert( selectByName.head.getString(0) == "barney" )

    val selectByAge = dataframe
      .select("age")
      .where("age > 23")
      .cache
    assert( selectByAge.count == 1 )
    assert( selectByAge.head.getLong(0) == 24 )
  }

  test("select > orderBy") {
    val orderByName = dataframe
      .select("name")
      .orderBy("name")
      .cache
    assert( orderByName.count == 4 )
    assert( orderByName.head.getString(0) == "barney" )
  }

  test("sort") {
    val sortByName = dataframe
      .select(col("id"), col("age"), col("name"), col("role"))
      .sort("name")
      .cache
    assert( sortByName.count == 4 )
    assert( sortByName.head.getLong(1) == 22 )
    assert( sortByName.head.getString(2) == "barney" )
    assert( sortByName.head.getString(3) == "husband" )
  }

  test("agg") {
    assert( dataframe.agg("age" -> "min").head.getLong(0) == 21 )
    assert( dataframe.agg("age" -> "avg").head.getDouble(0) == 22.5 )
    assert( dataframe.agg("age" -> "max").head.getLong(0) == 24 )
    assert( dataframe.agg("age" -> "sum").head.getLong(0) == 90 )
  }

  test("select > agg") {
    assert( dataframe.select(min(col("age"))).head.getLong(0) == 21 )
    assert( dataframe.select(max(col("age"))).head.getLong(0) == 24 )
    assert( dataframe.select(avg(col("age"))).head.getDouble(0) == 22.5 )
    assert( dataframe.select(sum(col("age"))).head.getLong(0) == 90 )
  }

  test("select > agg > case class") {
    assert(
      dataframe
        .select(min(col("age")))
        .map(row => Age(row.getLong(0)))
        .head == Age(21)
    )

    assert(
      dataframe
        .select(max(col("age")))
        .map(row => Age(row.getLong(0)))
        .head == Age(24)
    )
  }

  test("groupBy > avg") {
    val groupByRole = dataframe
      .groupBy("role")
      .avg("age")
      .cache
    assert( groupByRole.count == 2 )
    groupByRole.collect.foreach {
      case Row("husband", avgAge) => assert( avgAge == 23.0 )
      case Row("wife", avgAge) => assert( avgAge == 22.0 )
    }
  }

  test("groupBy > agg(min, avg, max)") {
    val groupByRole = dataframe
      .groupBy("role")
      .agg(
        min("age"),
        avg("age"),
        max("age")
      )
      .cache
    assert( groupByRole.count == 2 )
    groupByRole.collect.foreach {
      case Row("husband", minAge, avgAge, maxAge) =>
        assert( minAge == 22 )
        assert( avgAge == 23.0 )
        assert( maxAge == 24 )
      case Row("wife", minAge, avgAge, maxAge) => 
        assert( minAge == 21 )
        assert( avgAge == 22.0 )
        assert( maxAge == 23 )
    }
  }

  test("when > otherwise") {
    import sparkSession.implicits.*

    val personsWithGender = dataframe
      .withColumn("gender", when($"role" === "husband", "male")
      .otherwise("female"))
    personsWithGender.collect.foreach {
      case Row(_, _, _, "husband", gender ) => assert( gender == "male" )
      case Row(_, _, _, "wife", gender) => assert( gender == "female" )
    }
  }

  test("window") {
    import sparkSession.implicits.*

    val window = Window.partitionBy("role").orderBy($"age".desc)
    val ranking = rank.over(window).as("rank")
    val result = dataframe
      .select(col("role"), col("name"), col("age"), ranking)
      .as[(String, String, Long, Int)]
      .cache
    assert( ("wife", "wilma", 23, 1) == result.head )
  }

  test("join") {
    val persons = sparkSession.read.json("./data/person/person.json").cache
    val tasks = sparkSession.read.json("./data/task/task.json").cache
    assert( persons.count == 4 )
    assert( tasks.count == 4 )

    val joinCondition = persons.col("id") === tasks.col("pid")
    val personsTasks = persons.join(tasks, joinCondition)
    assert( personsTasks.count == 4 )
  }
}