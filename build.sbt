import sbt._
import Keys._

def jdkVersion(scalaBinaryVersion: String) = if (scalaBinaryVersion == "2.12") "1.8" else "1.7"
val avroVersion = "1.7.4"

val commonSettings = Sonatype.sonatypeSettings ++ Seq(
  organization       := "me.lyh",

  scalaVersion       := "2.12.3",
  crossScalaVersions := Seq("2.10.6", "2.11.11", "2.12.3"),
  scalacOptions      ++= Seq("-target:jvm-" + jdkVersion(scalaBinaryVersion.value), "-deprecation", "-feature", "-unchecked"),
  javacOptions       ++= Seq("-source", "1.7", "-target", "1.7"),

  version in avroConfig := avroVersion,

  coverageExcludedPackages := Seq(
    "me\\.lyh\\.parquet\\.avro\\.examples\\..*"
  ).mkString(";"),
  coverageHighlighting := (if (scalaBinaryVersion.value == "2.10") false else true),

  // Release settings
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "me.lyh",
  pomExtra           := {
    <url>https://github.com/nevillelyh/parquet-avro-extra</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:git@github.com:nevillelyh/parquet-avro-extra.git</connection>
      <developerConnection>scm:git:git@github.com:nevillelyh/parquet-avro-extra.git</developerConnection>
      <url>github.com/nevillelyh/parquet-avro-extra</url>
    </scm>
    <developers>
      <developer>
        <id>sinisa_lyh</id>
        <name>Neville Li</name>
        <url>https://twitter.com/sinisa_lyh</url>
      </developer>
    </developers>
  }
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project(
  "root",
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
    "org.apache.parquet" % "parquet-column" % "1.9.0"
  ),
  libraryDependencies := {
    CrossVersion.partialVersion(scalaVersion.value) match {
      // if Scala 2.11+ is used, quasiquotes are available in the standard distribution
      case Some((2, scalaMajor)) if scalaMajor >= 11 =>
        libraryDependencies.value
      // in Scala 2.10, quasiquotes are provided by macro paradise
      case Some((2, 10)) =>
        libraryDependencies.value ++ Seq(
          compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full),
          "org.scalamacros" %% "quasiquotes" % "2.1.0" cross CrossVersion.binary)
    }
  }
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
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"
).dependsOn(
  parquetAvroExtra,
  parquetAvroSchema
)
