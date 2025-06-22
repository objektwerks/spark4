Spark 4
-------
>Spark 4 feature apps and tests using Scala 3.

Note
----
>**WordCountApp**, executing on JDK 24, yields this error:

```java.lang.UnsupportedOperationException: getSubject is not supported```

>The **Security Manager**, the likely culprit, has been permanently disabled in JDK 24.

>Spark 4 currently requires JDK 17/21.

>Setting ```JAVA_HOME``` to JDK 21 is the only way to enforce SBT's use of JDK 21. Using ```.jvmopts``` failed.

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