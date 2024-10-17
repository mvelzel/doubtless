ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.doubtless"

javah / target := file("project/native")

javaOptions ++= Seq(
  "-Djava.library.path=" + baseDirectory.value.getAbsolutePath + "/project/native"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3"
)

fork := true
