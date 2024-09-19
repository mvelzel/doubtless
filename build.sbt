ThisBuild / scalaVersion := "2.13.14"
javah / target := file("project/native")

fork := true
javaOptions ++= Seq(
  "-Djava.library.path=" + baseDirectory.value.getAbsolutePath + "/project/native"
)
