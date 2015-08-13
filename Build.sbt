import sbt._
import Keys._

name := "devsumi_autumn"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1"

libraryDependencies += "org.apache.hbase" % "hbase" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-hadoop2-compat" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.0.0"

libraryDependencies += "org.apache.htrace" % "htrace-core" % "3.0.4"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
