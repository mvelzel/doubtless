scalaVersion := "3.5.1"
organization := "com.doubtless"

javah / target := file("project/native")

sbtJniCoreScope := Compile

javaOptions ++= Seq(
  "-Djava.library.path=" + baseDirectory.value.getAbsolutePath + "/project/native"
)

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.5.3")
    .exclude("org.scala-lang.modules", "scala-xml_2.13")
    .cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % "3.5.3")
    .exclude("org.scala-lang.modules", "scala-xml_2.13")
    .cross(CrossVersion.for3Use2_13),
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest-funspec" % "3.2.19" % "test"
)

coverageEnabled := true

fork := true
