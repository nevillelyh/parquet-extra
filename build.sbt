import sbt._
import Keys._

val avroVersion = "1.9.2"
val hadoopVersion = "3.3.0"
val magnoliaVersion = "0.16.0"
val parquetVersion = "1.11.1"
val scalatestVersion = "3.2.2"
val tensorFlowVersion = "1.15.0"

val commonSettings = Sonatype.sonatypeSettings ++ Seq(
  organization := "me.lyh",
  scalaVersion := "2.13.3",
  crossScalaVersions := Seq("2.12.12", "2.13.3"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions ++= (scalaBinaryVersion.value match {
    case "2.12" => Seq("-language:higherKinds")
    case "2.13" => Nil
  }),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javacOptions in (Compile, doc) := Seq("-source", "1.8"),
  // Release settings
  publishTo := Some(
    if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging
  ),
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  sonatypeProfileName := "me.lyh",
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/nevillelyh/parquet-extra")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/nevillelyh/parquet-extra.git"),
      "scm:git:git@github.com:nevillelyh/parquet-extra.git"
    )
  ),
  developers := List(
    Developer(
      id = "sinisa_lyh",
      name = "Neville Li",
      email = "neville.lyh@gmail.com",
      url = url("https://twitter.com/sinisa_lyh")
    )
  )
)

val scalatestDependencies = Seq("flatspec", "shouldmatchers")
  .map(m => "org.scalatest" %% s"scalatest-$m" % scalatestVersion % Test)

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
  parquetTensorFlow,
  parquetTypes,
  parquetExamples
)

lazy val parquetAvro: Project = Project(
  "parquet-avro",
  file("parquet-avro")
).settings(
  commonSettings,
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.apache.avro" % "avro-compiler" % avroVersion,
    "org.apache.parquet" % "parquet-column" % parquetVersion
  ),
  libraryDependencies ++= scalatestDependencies
).dependsOn(
  parquetSchema % Test
)

lazy val parquetTensorFlow: Project = Project(
  "parquet-tensorflow",
  file("parquet-tensorflow")
).settings(
  commonSettings,
  crossPaths := false,
  autoScalaLibrary := false,
  publishArtifact := scalaBinaryVersion.value == "2.12",
  libraryDependencies ++= Seq(
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
    "org.tensorflow" % "proto" % tensorFlowVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided
  ),
  libraryDependencies ++= scalatestDependencies
)

lazy val parquetTypes: Project = Project(
  "parquet-types",
  file("parquet-types")
).settings(
  commonSettings,
  libraryDependencies ++= Seq(
    "com.propensive" %% "magnolia" % magnoliaVersion,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided
  ),
  libraryDependencies ++= scalatestDependencies
)

lazy val parquetSchema: Project = Project(
  "parquet-schema",
  file("parquet-schema")
).settings(
  commonSettings ++ noPublishSettings,
  libraryDependencies += "org.apache.avro" % "avro" % avroVersion
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
  parquetTensorFlow,
  parquetTypes,
  parquetSchema
)
