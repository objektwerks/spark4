package objektwerks

import munit.FunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.*
import org.apache.spark.sql.functions.*
import org.apache.spark.storage.StorageLevel

import scala3encoders.given

import SparkInstance.*

class DatasetTest extends FunSuite {
  val dataset = sparkSession.read.json("./data/person.json").as[Person].persist(StorageLevel.MEMORY_ONLY)
  dataset.write.json("./target/dataset/person.json")

  test("dataset") {
    assert( dataset.count == 4 )
    assert( dataset.toDF.isInstanceOf[Dataset[Row]] )
    assert( dataset.rdd.isInstanceOf[RDD[Person]] )
  }

  test("column") {
    val idColumn = dataset.col("id")
    val nameColumn = col("name")
    val ageColumn = column("age")
    val roleColumn = expr("role")
    assert(
      dataset
        .select(idColumn, nameColumn, ageColumn, roleColumn)
        .as[Person]
        .count == 4
    )
  }

  test("selectExpr") {
    assert(
      dataset
        .selectExpr("id", "name", "age", "role")
        .as[Person]
        .count == 4
    )
  }

  test("add column") {
    import sparkSession.implicits.*

    assert(
      dataset
        .withColumn("dogAge", $"age" * 7)
        .as[PersonAsDog]
        .head
        .dogAge == 168
    )
  }

  test("update") {
    import sparkSession.implicits.*

    val incrementAgeNameToUpper = dataset
      .withColumn("age", $"age" + 1)
      .withColumn("name", upper($"name"))
      .as[Person]
      .cache
    assert( incrementAgeNameToUpper.count == 4 )
    assert( incrementAgeNameToUpper.head.age == 25 )
    assert( incrementAgeNameToUpper.head.name == "FRED" )
  }

  test("transform") {
    import sparkSession.implicits.*

    def incrementAge(ds: Dataset[Person]): Dataset[Person] = ds.withColumn("age", $"age" + 1).as[Person]
    def nameToUpper(ds: Dataset[Person]): Dataset[Person] = ds.withColumn("name", upper($"name")).as[Person]
    val incrementAgeNameToUpper = dataset
      .transform(incrementAge)
      .transform(nameToUpper)
      .cache
    assert( incrementAgeNameToUpper.count == 4 )
    assert( incrementAgeNameToUpper.head.age == 25 )
    assert( incrementAgeNameToUpper.head.name == "FRED" )
  }

  test("map") {
    val mapNameToUpperCase = dataset.map(_.name.toUpperCase).cache
    assert( mapNameToUpperCase.count == 4 )
    assert( mapNameToUpperCase.head == "FRED" )
  }

  test("filter") {
    val filterByName = dataset.filter(_.name == "barney").cache
    assert( filterByName.count == 1 )
    assert( filterByName.head.name == "barney" )

    val filterByAge = dataset.filter(_.age > 23).cache
    assert( filterByAge.count == 1 )
    assert( filterByAge.head.age == 24 )
  }

  test("filter > map") {
    val betty = dataset
      .filter(_.name == "betty")
      .map(_.name.toUpperCase)
      .cache
    assert( betty.count == 1 )
    assert( betty.head == "BETTY" )
  }

  test("sort") {
    import sparkSession.implicits.*

    val sortByName = dataset.sort($"name").cache
    assert( sortByName.count == 4 )
    assert( sortByName.head.name == "barney" )
  }

  test("select > orderBy") {
    import sparkSession.implicits.*

    val orderByName = dataset
      .select($"name")
      .orderBy($"name")
      .as[String]
      .cache
    assert( orderByName.count == 4 )
    assert( orderByName.head == "barney" )
  }

  test("select > agg > case class") {
    assert(
      dataset
        .select(min(col("age")))
        .map(row => Age(row.getLong(0)))
        .head == Age(21)
    )
    assert(
      dataset
        .select(max(col("age")))
        .map(row => Age(row.getLong(0)))
        .head == Age(24)
    )
  }

  test("groupBy > avg") {
    import sparkSession.implicits.*

    val groupByRole = dataset
      .groupBy($"role")
      .avg("age")
      .as[(String, Double)]
      .cache
    assert( groupByRole.count == 2 )
    groupByRole.collect.foreach {
      case ("husband", avgAge) => assert( avgAge == 23.0 )
      case ("wife", avgAge) => assert( avgAge == 22.0 )
      case _ => fail("groupBy > avg test failed!")
    }
  }

  test("groupBy > agg(min, avg, max)") {
    val groupByRole = dataset
      .groupBy("role")
      .agg(
        min("age"),
        avg("age"),
        max("age")
      )
      .cache
    groupByRole.count shouldBe 2
    groupByRole.collect.foreach {
      case Row("husband", minAge, avgAge, maxAge) =>
        minAge shouldBe 22
        avgAge shouldBe 23.0
        maxAge shouldBe 24
      case Row("wife", minAge, avgAge, maxAge) =>
        minAge shouldBe 21
        avgAge shouldBe 22.0
        maxAge shouldBe 23
      case _ => fail("groupBy > agg( min, avg, max) test failed!")
    }
  }

  test("when > otherwise") {
    val personsWithGender = dataset
      .withColumn("gender", when($"role" === "husband", "male")
      .otherwise("female"))
      .as[PersonWithGender]
    personsWithGender.collect.foreach {
      case PersonWithGender(_, _, _, "husband", gender) => gender shouldBe "male"
      case PersonWithGender(_, _, _, "wife", gender) => gender shouldBe "female"
      case _ => fail("when > otherwise test failed!")
    }
  }

  test("window") {
    val window = Window.partitionBy('role).orderBy($"age".desc)
    val ranking = rank.over(window).as("rank")
    val result = dataset
      .select(col("role"), col("name"), col("age"), ranking)
      .as[(String, String, Long, Int)]
      .cache
    ("wife", "wilma", 23, 1) shouldEqual result.head
  }

  test("join") {
    val persons = sparkSession.read.json("./data/person.json").as[Person].cache
    val tasks = sparkSession.read.json("./data/task.json").as[Task].cache
    persons.count shouldBe 4
    tasks.count shouldBe 4

    val joinCondition = persons.col("id") === tasks.col("pid")
    val personsTasks = persons.joinWith(tasks, joinCondition)
    personsTasks.count shouldBe 4
  }
}