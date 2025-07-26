name := "scala-spark-broadcast-join"
version := "0.1.0"
scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0", 
  "org.apache.spark" %% "spark-sql"  % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalatest" %% "scalatest-flatspec" % "3.2.17" % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case x                           => MergeStrategy.first
}