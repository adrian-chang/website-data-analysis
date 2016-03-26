import sbt._
import Process._
import Keys._

lazy val root = (project in file(".")).
  settings(
    name := "website-examine",
    version := "1.0",
    scalaVersion := "2.10.6"
  )

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"  % "1.6.1" % "provided",
  "org.apache.spark"  %% "spark-sql" % "1.6.1",
  "org.apache.spark"  %% "spark-mllib"  % "1.6.1",
  "org.apache.spark"  %% "spark-graphx" % "1.6.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
)