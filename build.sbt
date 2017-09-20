name := "machineLearning"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" ,//% "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.1",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.4.4",
  "com.typesafe.akka" % "akka-http_2.11" % "10.0.9",
  "com.typesafe.akka" % "akka-http-core_2.11" % "3.0.0-RC1",
  "com.typesafe.akka" % "akka-http-spray-json_2.11" % "3.0.0-RC1",
  "org.apache.httpcomponents" % "httpclient" % "4.3.4",
  "org.apache.httpcomponents" % "httpcore" % "4.3.2",
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
  "com.cloudera.sparkts" % "sparkts" % "0.4.0"
)

        