name := "spark4"
organization := "objektwerks"
version := "4.0.0"
scalaVersion := "3.7.2-RC2"
libraryDependencies ++= {
  val sparkVersion = "4.0.0"
  val spark4Scala3Version = "0.3.2"
  Seq(
    "io.github.vincenzobaz" %% "spark4-scala3-encoders" % spark4Scala3Version,
    "io.github.vincenzobaz" %% "spark4-scala3-udf" % spark4Scala3Version,
    ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-streaming" % sparkVersion).cross(CrossVersion.for3Use2_13),
    "com.h2database" % "h2" % "2.3.232",
    "org.scalameta" %% "munit" % "1.1.1" % Test
  )
}
scalacOptions ++= Seq(
  "-Wunused:all"
)
fork := true
