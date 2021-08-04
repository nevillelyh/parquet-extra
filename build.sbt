import sbt._
import Keys._

val avroVersion = "1.10.1"
val hadoopVersion = "3.3.1"
val parquetVersion = "1.12.0"
val scalatestVersion = "3.2.9"
val tensorFlowVersion = "0.3.2"

val commonSettings = Sonatype.sonatypeSettings ++ Seq(
  organization := "me.lyh",
  scalaVersion := "2.13.6",
  crossScalaVersions := Seq("2.12.14", "2.13.6"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions ++= (scalaBinaryVersion.value match {
    case "2.12" => Seq("-language:higherKinds")
    case "2.13" => Nil
  }),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  Compile / doc / javacOptions := Seq("-source", "1.8"),
  // Release settings
  publishTo := Some(
    if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging
  ),
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  Test / publishArtifact := false,
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
  run := parquetExamples / Compile / run
).aggregate(
  parquetAvro,
  parquetTensorFlow,
  parquetExamples
)

lazy val parquetAvro: Project = Project(
  "parquet-avro",
  file("parquet-avro")
).settings(
  commonSettings,
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion % Provided,
    "org.apache.avro" % "avro-compiler" % avroVersion % Provided,
    "org.apache.parquet" % "parquet-column" % parquetVersion % Provided
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
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion % Provided,
    "org.tensorflow" % "tensorflow-core-api" % tensorFlowVersion % Provided,
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
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.apache.avro" % "avro-compiler" % avroVersion,
    "org.apache.parquet" % "parquet-column" % parquetVersion
  ),
  coverageExcludedPackages := Seq(
    "me\\.lyh\\.parquet\\.examples\\..*"
  ).mkString(";")
).dependsOn(
  parquetAvro,
  parquetTensorFlow,
  parquetSchema
)
