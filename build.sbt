name := "amazon-book-reviews"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("com.bigdata.amazon")

val sparkVersion = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)