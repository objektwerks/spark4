package objektwerks

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

final case class Age(value: Long)

final case class AvgAgeByRole(role: String, avgAge: Double)
object AvgAgeByRole:
  given Ordering[AvgAgeByRole] = Ordering.by(role => role.avgAge > role.avgAge)

final case class Count(value: String, count: Long)

final case class Event(id: Int, datetime: String)
object Event:
  given Ordering[Event] = Ordering.by(_.id)

final case class Friend(id: Int, name: String, age: Int, score: Int)

final case class KeyValue(key: Int, value: Int)
object KeyValue:
  given Ordering[KeyValue] = Ordering.by(_.key)

final case class Person(id: Long, age: Long, name: String, role: String)
object Person:
  val personStructType = new StructType()
    .add("id", IntegerType)
    .add("age", IntegerType)
    .add("name", StringType)
    .add("role", StringType)
  val personForeachWriter = new ForeachWriter[Person]:
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = println(s"$person")
    override def close(errorOrNull: Throwable): Unit = ()
  given Ordering[Person] = Ordering.by(_.name)

final case class PersonsTasks(id: Long, age: Long, name: String, role: String, tid: Long, pid: Long, task: String)

final case class Task(tid: Long, pid: Long, task: String)
object Task:
  given Ordering[Task] = Ordering.by(_.task)

final case class PersonAsDog(id: Long, age: Long, name: String, role: String, dogAge: Long)

final case class PersonWithGender(id: Long, age: Long, name: String, role: String, gender: String)