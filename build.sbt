import sbt._
import Keys._

val avroVersion = "1.8.2"
val parquetVersion = "1.10.1"
val scalatestVersion = "3.0.8"

val commonSettings = Sonatype.sonatypeSettings ++ Seq(
  organization       := "me.lyh",

  scalaVersion       := "2.13.1",
  crossScalaVersions := Seq("2.11.12", "2.12.10", "2.13.1"),
  scalacOptions      ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  javacOptions       ++= Seq("-source", "1.8", "-target", "1.8"),

  version in AvroConfig := avroVersion,

  coverageExcludedPackages := Seq(
    "me\\.lyh\\.parquet\\.avro\\.examples\\..*"
  ).mkString(";"),

  // Release settings
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "me.lyh",

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/nevillelyh/parquet-avro-extra")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/nevillelyh/parquet-avro-extra.git"),
    "scm:git:git@github.com:nevillelyh/parquet-avro-extra.git")),
  developers := List(
    Developer(id="sinisa_lyh", name="Neville Li", email="neville.lyh@gmail.com", url=url("https://twitter.com/sinisa_lyh")))
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project(
  "parquet-avro-extra-parent",
  file(".")
).settings(
  commonSettings ++ noPublishSettings,
  run := run in Compile in parquetAvroExamples
).aggregate(
  parquetAvroExtra,
  parquetAvroExamples
)

lazy val parquetAvroExtra: Project = Project(
  "parquet-avro-extra",
  file("parquet-avro-extra")
).settings(
  commonSettings,
  libraryDependencies += ("org.scala-lang" % "scala-reflect" % scalaVersion.value),
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.apache.avro" % "avro-compiler" % avroVersion,
    "org.apache.parquet" % "parquet-column" % parquetVersion
  )
)

lazy val parquetAvroSchema: Project = Project(
  "parquet-avro-schema",
  file("parquet-avro-schema")
).settings(
  commonSettings ++ noPublishSettings
)

lazy val parquetAvroExamples: Project = Project(
  "parquet-avro-examples",
  file("parquet-avro-examples")
).settings(
  commonSettings ++ noPublishSettings,
  libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % Test
).dependsOn(
  parquetAvroExtra,
  parquetAvroSchema
)
