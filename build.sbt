scalaVersion := "2.13.15"
organization := "com.doubtless"

javah / target := file("project/native")

sbtJniCoreScope := Compile

javaOptions ++= Seq(
  "-Djava.library.path=" + baseDirectory.value.getAbsolutePath + "/project/native",
  "-Dlog4j.configuration=" + baseDirectory.value.getAbsolutePath + "/log4j.properties",
  "--add-exports",
  "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens",
  "java.base/sun.security.action=ALL-UNNAMED"
)

coverageEnabled := true

fork := true

lazy val PerfTest = config("perf") extend(Test)

lazy val root = (project in file ("."))
  .configs(PerfTest)
  .settings(
    inConfig(PerfTest)(Defaults.testSettings),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test",
      "org.scalactic" %% "scalactic" % "3.2.19",
      "org.scalatest" %% "scalatest-funspec" % "3.2.19" % "test",
      "org.scalatest" %% "scalatest-funspec" % "3.2.19" % PerfTest
    )
  )
