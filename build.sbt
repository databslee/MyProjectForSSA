name := "MyProject"

version := "0.1"

scalaVersion := "2.11.0"


val sparkVersion = "2.3.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.2.2",
  "org.apache.spark" %% "spark-core" %  sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "net.liftweb" %% "lift-json" % "2.6-M4"
)
