ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"
val sparkVersion = "3.2.1"
lazy val root = (project in file("."))
  .settings(
    name := "Spark_project",
    idePackagePrefix := Some("com.sr"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion withSources(),
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion withSources(),
    libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion withSources()

  )
