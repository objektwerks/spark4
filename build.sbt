name := "spark4"
organization := "objektwerks"
version := "1.0.0"
scalaVersion := "3.7.1"
libraryDependencies ++= {
  val sparkVersion = "4.0.0"
  val spark4Scala3Version = "0.3.0"
  Seq(
    ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-streaming" % sparkVersion).cross(CrossVersion.for3Use2_13),
    "io.github.vincenzobaz" %% "spark4-scala3-encoders" % spark4Scala3Version,
    "io.github.vincenzobaz" %% "spark4-scala3-udf" % spark4Scala3Version,
    ("org.scalatest" %% "scalatest" % "3.2.19").cross(CrossVersion.for3Use2_13) % Test
  )
}