package objektwerks

import munit.FunSuite

import scala3encoders.given

import SparkInstance.*
  
class DescribeTest extends FunSuite:
  test("describe"):
    val persons = sparkSession.read.json("./data/person.json").as[Person].cache
    assert( persons.count == 4 )
    persons.describe("id", "age", "name", "role").show()