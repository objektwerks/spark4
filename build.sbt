name := "spark4"
organization := "objektwerks"
version := "1.0.0"
scalaVersion := "3.7.1"
libraryDependencies ++= {
  val sparkVersion = "4.0.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.2.19" % Test
  )
}