

name := "template-scala-parallel-vanilla"

version :="1.2.1"

organization := "org.apache.predictionio"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.13.0" % "provided",
  "org.apache.spark" %% "spark-core"    % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided"
)
