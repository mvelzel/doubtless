ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.doubtless"

javah / target := file("project/native")

fork := true
javaOptions ++= Seq(
  "-Djava.library.path=" + baseDirectory.value.getAbsolutePath + "/project/native"
)
