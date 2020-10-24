name := "spark_auto_ml"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.0.0-SNAP13"
libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric" % "0.27.4"
libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric-cli" % "0.27.4"

libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.9.4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.4"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.4"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.4"

//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4"
//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.4"
//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.4"
//libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.9.4"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}





