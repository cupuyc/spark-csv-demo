name := "spark-csv-demo"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  
  "org.json4s" %% "json4s-native" % "3.6.0-M2",

  "org.specs2" %% "specs2-core" % "4.0.0" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")
    
