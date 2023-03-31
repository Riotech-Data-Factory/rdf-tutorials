version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.2"
name := "tests"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided",
  "org.elasticsearch" % "elasticsearch-spark-30_2.12" % "8.0.0"
)

mainClass in assembly := some("ElasticApp")
assemblyJarName := "rdf-spark-es.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}