


name := "backend"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "org.apache.commons" % "commons-text" % "1.4",
  "com.google.guava" % "guava" % "23.0"
)
