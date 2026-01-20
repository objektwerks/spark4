name := "spark4"
organization := "objektwerks"
version := "4.0.0"
scalaVersion := "3.8.0-RC6"
libraryDependencies ++= {
  val sparkVersion = "4.2.0-preview1"
  val spark4Scala3Version = "0.3.2"
  Seq(
    "com.github.RoaringBitmap" % "RoaringBitmap" % "1.6.0", 
    "io.github.vincenzobaz" %% "spark4-scala3-encoders" % spark4Scala3Version,
    "io.github.vincenzobaz" %% "spark4-scala3-udf" % spark4Scala3Version,
    ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ("org.apache.spark" %% "spark-streaming" % sparkVersion).cross(CrossVersion.for3Use2_13),
    "com.h2database" % "h2" % "2.4.240",
    "org.scalameta" %% "munit" % "1.2.1" % Test
  )
}
scalacOptions ++= Seq(
  "-Wunused:all"
)
fork := true