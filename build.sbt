ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaIcebergClient"
  )

libraryDependencies ++= Seq (

  "org.apache.iceberg" % "iceberg-core" % "1.1.0",
  "org.apache.iceberg" % "iceberg-aws" % "1.1.0",
  "org.apache.iceberg" % "iceberg-data" % "1.1.0",
  "org.apache.iceberg" % "iceberg-parquet" % "1.1.0",


  "software.amazon.awssdk" % "bundle" % "2.18.25",
  "software.amazon.awssdk" % "url-connection-client" % "2.18.35",

  "org.apache.hadoop" % "hadoop-client" % "3.3.2",

  // logging
  "ch.qos.logback" % "logback-classic" % "1.4.5",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  //duckdb
  "org.duckdb" % "duckdb_jdbc" % "0.6.1"

)
