name := "SBTTest"

version := "0.1"

scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-streaming" % "3.3.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.1"
)