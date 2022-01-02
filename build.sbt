name := "sparkwithscala"

version := "0.1"

scalaVersion := "2.11.0"

idePackagePrefix := Some("com.suanki")

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0"

libraryDependencies += "com.oracle.database.jdbc" % "ojdbc6" % "11.2.0.4"


