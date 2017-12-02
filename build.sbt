lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.yannmoisan",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Hello",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.0",
      "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test",
      "org.apache.spark" %% "spark-hive"       % "2.0.0" % "test"
    )
  )
