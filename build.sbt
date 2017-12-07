useGpg := true

licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/yannmoisan/spark-sql-helper"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/yannmoisan/spark-sql-helper"),
    "scm:git@github.com:yannmoisan/spark-sql-helper.git"
  )
)

developers := List(
  Developer(
    id    = "yannmoisan",
    name  = "Yann Moisan",
    email = "",
    url   = url("https://github.com/yannmoisan")
  )
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

import sbtrelease.ReleaseStateTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _), enableCrossBuild = true),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
  pushChanges
)

lazy val root = (project in file("."))
  .enablePlugins(GitVersioning)
  .settings(
    inThisBuild(List(
      organization := "com.yannmoisan",
      scalaVersion := "2.11.11"
//      version      := "0.1-SNAPSHOT"
    )),
    name := "spark-sql-helper",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.0",
      "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test",
      "org.apache.spark" %% "spark-hive"       % "2.0.0" % "test"
    )
  )