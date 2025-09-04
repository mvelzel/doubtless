import com.github.sbt.jni.build.CMake

scalaVersion := "2.13.15"
organization := "com.doubtless"

coverageEnabled := true

fork := true

lazy val PerfTest = config("perf") extend (Test)

lazy val root = (project in file("."))
  .configs(PerfTest)
  .settings(
    nativeCompile / sourceDirectory := baseDirectory.value / "native" / "src",
    nativeMultipleOutputs := true,
    nativeBuildTool := CMake.make(
      Seq(
        "-DCMAKE_BUILD_TYPE=Release",
        "-DSBT:BOOLEAN=true",
        //"-DCMAKE_TOOLCHAIN_FILE=linux-toolchain.cmake"
      )
    ),
    javah / target := baseDirectory.value / "native" / "src",
    inConfig(PerfTest)(Defaults.testSettings),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.4",
      "org.apache.spark" %% "spark-hive" % "3.5.4",
      "org.apache.spark" %% "spark-hive-thriftserver" % "3.5.4",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test",
      "org.scalactic" %% "scalactic" % "3.2.19",
      "org.scalatest" %% "scalatest-funspec" % "3.2.19" % "test",
      "org.scalatest" %% "scalatest-funspec" % "3.2.19" % PerfTest,
      "com.typesafe" % "config" % "1.4.3"
    ),
    javaOptions ++= Seq(
      "-Dlog4j.configuration=" + baseDirectory.value.getAbsolutePath + "/log4j.properties",
      "--add-exports",
      "java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens",
      "java.base/sun.security.action=ALL-UNNAMED"
    )
  )
  .enablePlugins(JniNative, JniPackage)
  .disablePlugins(ScoverageSbtPlugin)
