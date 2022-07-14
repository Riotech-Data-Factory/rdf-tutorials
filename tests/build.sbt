version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.2"
name := "tests"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided"
)