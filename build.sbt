import sbt._
import Keys._

val avroVersion = "1.8.2"
val parquetVersion = "1.9.0"
val scalatestVersion = "3.0.3"

val commonSettings = Sonatype.sonatypeSettings ++ Seq(
  organization       := "me.lyh",

  scalaVersion       := "2.12.3",
  crossScalaVersions := Seq("2.11.11", "2.12.3"),
  scalacOptions      ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  javacOptions       ++= Seq("-source", "1.8", "-target", "1.8"),

  version in AvroConfig := avroVersion,

  coverageExcludedPackages := Seq(
    "me\\.lyh\\.parquet\\.avro\\.examples\\..*"
  ).mkString(";"),

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
  libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test"
).dependsOn(
  parquetAvroExtra,
  parquetAvroSchema
)
