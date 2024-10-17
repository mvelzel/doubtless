ThisBuild / scalaVersion := "3.5.1"
ThisBuild / organization := "com.doubtless"

javah / target := file("project/native")

sbtJniCoreScope := Compile

javaOptions ++= Seq(
  "-Djava.library.path=" + baseDirectory.value.getAbsolutePath + "/project/native"
)

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.5.3").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % "3.5.3").cross(CrossVersion.for3Use2_13)
)

fork := true
