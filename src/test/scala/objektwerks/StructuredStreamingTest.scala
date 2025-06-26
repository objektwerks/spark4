package objektwerks

import munit.FunSuite

import scala3encoders.given

import Person.{personForeachWriter, personStructType}
import SparkInstance.*
//import sparkSession.implicits.*

class StructuredStreamingTest extends FunSuite {
  test("structured") {
    sparkSession
      .readStream
      .schema(personStructType)
      .json("./data")
      .as[Person]
      .writeStream
      .foreach(personForeachWriter)
      .start
      .awaitTermination(3000L)
  }
}