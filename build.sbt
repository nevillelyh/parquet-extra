import sbt._
import Keys._

val avroVersion = "1.8.2"
val hadoopVersion = "3.2.1"
val magnoliaVersion = "0.12.0"
val parquetVersion = "1.10.1"
val scalatestVersion = "3.0.8"

val commonSettings = Sonatype.sonatypeSettings ++ Seq(
  organization       := "me.lyh",

  scalaVersion       := "2.13.1",
  crossScalaVersions := Seq("2.11.12", "2.12.10", "2.13.1"),
  scalacOptions      ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  javacOptions       ++= Seq("-source", "1.8", "-target", "1.8"),


  // Release settings
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "me.lyh",

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/nevillelyh/parquet-extra")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/nevillelyh/parquet-extra.git"),
    "scm:git:git@github.com:nevillelyh/parquet-extra.git")),
  developers := List(
    Developer(id="sinisa_lyh", name="Neville Li", email="neville.lyh@gmail.com", url=url("https://twitter.com/sinisa_lyh")))
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project(
  "parquet-extra",
  file(".")
).settings(
  commonSettings ++ noPublishSettings,
  run := run in Compile in parquetExamples
).aggregate(
  parquetAvro,
  parquetTypes,
  parquetExamples
)

lazy val parquetAvro: Project = Project(
  "parquet-avro",
  file("parquet-avro")
).settings(
  commonSettings,
  libraryDependencies += ("org.scala-lang" % "scala-reflect" % scalaVersion.value),
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.apache.avro" % "avro-compiler" % avroVersion,
    "org.apache.parquet" % "parquet-column" % parquetVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  )
).dependsOn(
  parquetSchema % Test
)

lazy val parquetTypes: Project = Project(
  "parquet-types",
  file("parquet-types")
).settings(
  commonSettings,
  libraryDependencies += {
    if (scalaBinaryVersion.value == "2.11") {
      "me.lyh" %% "magnolia" % "0.10.1-jto"
    } else {
      "com.propensive" %% "magnolia" % magnoliaVersion
    }
  },
  libraryDependencies ++= Seq(
    "org.apache.parquet" % "parquet-column" % parquetVersion,
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  )
)

lazy val parquetSchema: Project = Project(
  "parquet-schema",
  file("parquet-schema")
).settings(
  commonSettings ++ noPublishSettings,
  version in AvroConfig := avroVersion
)

lazy val parquetExamples: Project = Project(
  "parquet-examples",
  file("parquet-examples")
).settings(
  commonSettings ++ noPublishSettings,
  coverageExcludedPackages := Seq(
    "me\\.lyh\\.parquet\\.examples\\..*"
  ).mkString(";")
).dependsOn(
  parquetAvro,
  parquetTypes,
  parquetSchema
)
