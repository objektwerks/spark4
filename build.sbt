name := "spark4"
organization := "objektwerks"
version := "1.0.0"
scalaVersion := "3.7.1"
libraryDependencies ++= {
  val sparkVersion = "4.0.0"
  Seq(
    ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-streaming" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.scalatest" %% "scalatest" % "3.2.19").cross(CrossVersion.for3Use2_13) % Test
  )
}