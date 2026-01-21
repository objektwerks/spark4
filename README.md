Spark 4
-------
>Spark 4 feature apps and tests using H2, MUnit and Scala 3.

Warning
-------
>Spark 4 currently requires JDK 17/21!

Issues
------
1. **JDK**
>**WordCountApp**, executing on JDK 25, yields this error:

```java.lang.UnsupportedOperationException: getSubject is not supported```

>The **Security Manager**, the likely culprit, has been permanently disabled in JDK 25.

2. **Fork**
>And while **WordCountApp** works on JDK 21, the following exception is thrown:

```java.nio.file.NoSuchFileException: ./hadoop-client-api-3.4.1.jar```

>Said jar is a Spark 4 dependency. Adding ```fork := true``` to build.sbt resolves this error.

3. **Tests**
>Due to unresovable dependency and type issues, MUnit was used instead of ScalaTest.

JDK
---
>Setting ```JAVA_HOME``` to JDK 21 is the principal way to enforce SBT's use of JDK 21. Using ```.jvmopts``` fails.

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
