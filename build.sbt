
name := "Knowledge Graph Generator"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "info.debatty" % "java-string-similarity" % "1.2.1"
libraryDependencies += "io.sgr" % "s2-geometry-library-java" % "1.0.0"
libraryDependencies += "com.esri.geometry" % "esri-geometry-api" % "2.2.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4"