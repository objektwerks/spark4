package objektwerks

import munit.FunSuite

import scala3encoders.given

import SparkInstance.*

final class ExplainTest extends FunSuite:
  test("explain"):
    val persons = sparkSession.read.json("./data/person.json").as[Person].cache
    val fred = persons.map(_.name.toUpperCase).filter(_ == "FRED").cache
    assert( fred.count == 1 )
    assert( fred.head == "FRED" )
    fred.explain(extended = true)