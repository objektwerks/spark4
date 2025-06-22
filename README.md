Spark 4
-------
>Spark 4 feature apps and tests using Scala 3.

Note
----
>The **WordCountApp** yields this error: ```java.lang.UnsupportedOperationException: getSubject is not supported```
>Spark requires JDK 17. I'm on JDK 24, which has permanently disabled the **Security Manager**. Classic. :)

Build
-----
1. sbt clean compile

Test
----
1. sbt clean test

Run
---
1. sbt clean run

Logs
----
1. target/app.log
2. target/test.log

Resources
---------
* [Spark 4 Intro](https://www.databricks.com/blog/introducing-apache-spark-40)
* [Spark Docs](https://spark.apache.org/docs/latest/)
* [Spark4-Scala3](https://vincenzobaz.github.io/spark-scala3/)