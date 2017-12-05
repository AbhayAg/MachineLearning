name := "Spark-unit-testing"

version := "1.0"

scalaVersion := "2.11.0"
lazy val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.1",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.4.4",
  "com.typesafe.akka" % "akka-http_2.11" % "10.0.9",
  "com.typesafe.akka" % "akka-http-core_2.11" % "3.0.0-RC1",
  "com.typesafe.akka" % "akka-http-spray-json_2.11" % "3.0.0-RC1",
  "org.apache.httpcomponents" % "httpclient" % "4.3.4",
  "org.apache.httpcomponents" % "httpcore" % "4.3.2",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.5",

  // testing Libraries
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test"
)

/*parallelExecution in Test := false
fork in Test := true*/
        