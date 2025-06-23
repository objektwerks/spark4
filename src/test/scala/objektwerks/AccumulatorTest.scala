package objektwerks

import munit.FunSuite

import scala.collection.JavaConverters.*

final class AccumulatorTest extends FunSuite:
  val (sparkSession, sparkContext) = SparkInstance.sessionAndContext()

  test("long accumulator"):
    val longAcc = sparkContext.longAccumulator("longAcc")
    longAcc.add(1)
    assert( longAcc.name.get == "longAcc" )
    assert( longAcc.value == 1 )

  test("double accumulator"):
    val doubleAcc = sparkContext.doubleAccumulator("doubleAcc")
    doubleAcc.add(1.0)
    assert( doubleAcc.name.get == "doubleAcc" )
    assert( doubleAcc.value == 1.0 )

  test("collection accumulator"):
    val intsAcc = sparkContext.collectionAccumulator[Int]("intsAcc")
    intsAcc.add(1)
    intsAcc.add(2)
    intsAcc.add(3)
    assert( intsAcc.name.get == "intsAcc" )
    assert( intsAcc.value.asScala.sum == 6 )