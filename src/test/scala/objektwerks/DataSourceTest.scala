package objektwerks

import java.sql.DriverManager

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
    assert( dataframe.count == 500 )

    val friends = dataframe.map(row => Friend(row.getInt(0), row.getString(1), row.getInt(2), row.getInt(3)))
    assert( friends.count == 500 )

  test("text"):
    val dataframe = sparkSession
      .read
      .text("./data/license.txt")
    assert( dataframe.count == 19 )

    val dataset = sparkSession
      .read
      .textFile("./data/license.txt")
    assert( dataset.count == 19 )

  test("json"):
    val dataframe = sparkSession
      .read
      .json("./data/person.json")
    assert( dataframe.count == 4 )

    val dataset = sparkSession
      .read
      .json("./data/person.json")
      .as[Person]
    assert( dataset.count == 4 )

  test("parquet"):
    val dataset = sparkSession
      .read
      .json("./data/person.json")
      .as[Person]

    dataset
      .write
      .parquet("./target/parquet/person.parquet")

    val parquet = sparkSession
      .read
      .parquet("./target/parquet/person.parquet")
      .as[Person]

    parquet.createOrReplaceTempView("persons")
    val persons = sparkSession
      .sql("select * from persons where age >= 21 and age <= 22 order by age")
      .as[Person]
      .cache
    assert( persons.count == 2 )
    assert( persons.head.name == "betty" )
    assert( persons.head.age == 21 )

  test("jdbc"):
    DriverManager.getConnection(
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;AUTO_RECONNECT=TRUE;INIT=runscript from 'classpath:/ddl.sql'",
      "sa",
      "sa"
    )

    val persons = readPersonsDatasource  // Source
    val avgAgeByRole = personsToAvgAgeByRole(persons)  // Flow
    writeAvgAgeByRoleDatasource(avgAgeByRole)  // Sink

    val avgAgeByRoles = readAvgAgeByRoleDatasource  // Verify
    assert( avgAgeByRoles.count == 2 )

  private def readPersonsDatasource: Dataset[Person] =
    sparkSession
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "persons")
      .load
      .as[Person]

  private def personsToAvgAgeByRole(persons: Dataset[Person]): Dataset[AvgAgeByRole] =
    persons
      .groupBy("role")
      .avg("age")
      .map(row => AvgAgeByRole(row.getString(0), row.getDouble(1)))

  private def writeAvgAgeByRoleDatasource(avgAgeByRole: Dataset[AvgAgeByRole]): Unit =
    avgAgeByRole
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "avg_age_by_role")
      .save

  private def readAvgAgeByRoleDatasource: Dataset[AvgAgeByRole] =
    sparkSession
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "avg_age_by_role")
      .load
      .as[AvgAgeByRole]
      .cache