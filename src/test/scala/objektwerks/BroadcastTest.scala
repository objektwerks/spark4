package objektwerks

import munit.FunSuite

import scala3encoders.given

import SparkInstance.*

final class BroadcastTest extends FunSuite:
  test("broadcast variable"):
    val keyValues = List( KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3) )
    val broadcastKeyValues = sparkContext.broadcast( keyValues )
    assert( broadcastKeyValues.value.length == 3 )

  test("broadcast join"):
    val broadcastPersons = sparkContext
      .broadcast(
        sparkSession
          .read
          .json("./data/person.json")
          .as[Person]
      )

    val persons = broadcastPersons.value
    val tasks = sparkSession
      .read
      .json("./data/task.json")
      .as[Task]

    val joinCondition = persons.col("id") === tasks.col("pid")
    val personsTasks = persons.join(tasks, joinCondition)

    assert( personsTasks.count == 4 )