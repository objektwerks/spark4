Spark 4
-------
>Spark 4 feature apps and tests using Scala 3.

Note
----
>**WordCountApp**, executing on JDK 24, yields this error:

```java.lang.UnsupportedOperationException: getSubject is not supported```

>The **Security Manager** has been permanently disabled in JDK 24.

>Spark requires JDK 17/21.

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