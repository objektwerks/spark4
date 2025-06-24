package objektwerks

import org.apache.spark.sql.{Dataset, Row, SaveMode}

import munit.FunSuite

import org.apache.spark.sql.{Dataset, SaveMode}

import scala3encoders.given

import SparkInstance.*

final class SqlTest extends FunSuite:
  test("dataframe sql"):
    val dataframe = sparkSession.read.json("./data/person.json").cache
    assert( dataframe.isInstanceOf[Dataset[Row]] )
    assert( dataframe.count == 4 )
    dataframe.createOrReplaceTempView("persons")

    val persons = sparkSession
      .sql("select * from persons where age >= 21 and age <= 22 order by age")
      .cache
    assert( persons.count == 2 )
    assert( persons.head.getLong(0) == 21 )
    assert( persons.head.getString(2) == "betty" )

    assert( sparkSession.sql("select min(age) from persons").head.getLong(0) == 21 )
    assert( sparkSession.sql("select avg(age) from persons").head.getDouble(0) == 22.5 )
    assert( sparkSession.sql("select max(age) from persons").head.getLong(0) == 24 )
    assert( sparkSession.sql("select sum(age) from persons").head.getLong(0) == 90 )

    val agesLimitByTwoDesc = sparkSession.sql("select name, age from persons where role = 'wife' order by name desc limit 2")
    assert( agesLimitByTwoDesc.head.getString(0) == "wilma" )
    assert( agesLimitByTwoDesc.head.getLong(1) == 23 )
    assert( agesLimitByTwoDesc.take(2).tail(0).getString(0) == "betty" )
    assert( agesLimitByTwoDesc.take(2).tail(0).getLong(1) == 21 )

  test("dataset sql"):
    val dataset = sparkSession
      .read
      .json("./data/person/person.json")
      .as[Person]
      .cache
    assert( dataset.count == 4 )
    dataset.createOrReplaceTempView("persons")

    val persons = sparkSession
      .sql("select * from persons where age >= 21 and age <= 22 order by age")
      .as[Person]
      .cache
    assert( persons.count == 2 )
    assert( persons.head.age == 21 )
    assert( persons.head.name == "betty" )

    assert( sparkSession.sql("select min(age) from persons").as[Long].head == 21 )
    assert( sparkSession.sql("select avg(age) from persons").as[Double].head == 22.5 )
    assert( sparkSession.sql("select max(age) from persons").as[Long].head == 24 )
    assert( sparkSession.sql("select sum(age) from persons").as[Long].head == 90 )

    val agesLimitByTwoDesc = sparkSession
      .sql("select name, age from persons where role = 'husband' order by name desc limit 2")
      .as[(String, Long)]
    assert( ("fred", 24) == agesLimitByTwoDesc.head )
    assert( ("barney", 22) == agesLimitByTwoDesc.take(2).tail(0) )

  test("dataframe join"):
    val persons = sparkSession.read.json("./data/person.json").cache
    val tasks = sparkSession.read.json("./data/task.json").cache
    assert( persons.count == 4 )
    assert( tasks.count == 4 )
    persons.createOrReplaceTempView("persons")
    tasks.createOrReplaceTempView("tasks")

    val personsTasks: Dataset[Row] = sparkSession.sql("SELECT * FROM persons, tasks WHERE persons.id = tasks.pid").cache
    assert( personsTasks.count == 4 )

    personsTasks.createOrReplaceTempView("persons_tasks")
    val personTask: Dataset[Row] = sparkSession.sql("select name, task from persons_tasks").cache
    assert( personTask.count == 4 )

  test("dataset join"):
    val persons = sparkSession.read.json("./data/person/person.json").as[Person].cache
    val tasks = sparkSession.read.json("./data/task/task.json").as[Task].cache
    persons.count shouldBe 4
    tasks.count shouldBe 4
    persons.createOrReplaceTempView("persons")
    tasks.createOrReplaceTempView("tasks")

    val personsTasks: Dataset[PersonsTasks] = sparkSession.sql("select * from persons, tasks where persons.id = tasks.pid").as[PersonsTasks].cache
    personsTasks.count shouldBe 4

    personsTasks.createOrReplaceTempView("persons_tasks")
    val personTask: Dataset[(String, String)] = sparkSession.sql("select name, task from persons_tasks").as[(String, String)].cache
    personTask.count shouldBe 4

  test("udf"):
    val cityTemps = sparkSession
      .read
      .json("./data/weather/city_temps.json")
      .cache
    cityTemps.createOrReplaceTempView("city_temps")

    sparkSession
      .udf
      .register("celciusToFahrenheit", (degreesCelcius: Double) => (degreesCelcius * 9.0 / 5.0) + 32.0)

    val temps = sparkSession
      .sql("select city, celciusToFahrenheit(avgLow) as avgLowFahrenheit, celciusToFahrenheit(avgHigh) as avgHighFahrenheit from city_temps")
    temps.count shouldBe 6

  test("jdbc") {
    val tableName = "key_values"
    writeKeyValues(tableName, List[KeyValue](KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3)).toDS)
    val keyvalues = readKeyValues(tableName).toDF
    keyvalues.createOrReplaceTempView("key_values")
    sparkSession.sql("select count(*) as total_rows from key_values").head.getLong(0) shouldBe 3
    sparkSession.sql("select min(key) as min_key from key_values").head.getInt(0) shouldBe 1
    sparkSession.sql("select max(value) as max_value from key_values").head.getInt(0) shouldBe 3
    sparkSession.sql("select sum(value) as sum_value from key_values").head.getLong(0) shouldBe 6
  }

  private def writeKeyValues(table: String, keyValues: Dataset[KeyValue]): Unit =
    keyValues
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", table)
      .save

  private def readKeyValues(table: String): Dataset[KeyValue] =
    sparkSession
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", table)
      .load
      .as[KeyValue]