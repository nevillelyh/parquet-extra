import sbt._
import Keys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization  := "parquet",
    version       := "0.1.0-SNAPSHOT",
    scalaVersion  := "2.10.4",
    crossScalaVersions := Seq("2.10.4", "2.11.2"),
    scalacOptions ++= Seq()
  )
}

object ScalaMacroDebugBuild extends Build {
  import BuildSettings._

  lazy val root: Project = Project(
    "root",
    file("."),
    settings = buildSettings ++ Seq(
      run <<= run in Compile in parquetAvroExamples)
  ) aggregate(parquetAvroExtra, parquetAvroExamples)

  lazy val parquetAvroExtra: Project = Project(
    "parquet-avro-extra",
    file("parquet-avro-extra"),
    settings = buildSettings ++ Seq(
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
      libraryDependencies ++= Seq(
        "org.apache.avro" % "avro" % "1.7.4",
        "org.apache.avro" % "avro-compiler" % "1.7.4"
      ),
      libraryDependencies := {
        CrossVersion.partialVersion(scalaVersion.value) match {
          // if Scala 2.11+ is used, quasiquotes are available in the standard distribution
          case Some((2, scalaMajor)) if scalaMajor >= 11 =>
            libraryDependencies.value
          // in Scala 2.10, quasiquotes are provided by macro paradise
          case Some((2, 10)) =>
            libraryDependencies.value ++ Seq(
              compilerPlugin(
                "org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
                "org.scalamacros" %% "quasiquotes" % "2.0.0" cross CrossVersion.binary)
        }
      }
    )
  )

  lazy val parquetAvroExamples: Project = Project(
    "parquet-avro-examples",
    file("parquet-avro-examples"),
    settings = buildSettings ++ sbtavro.SbtAvro.avroSettings ++ Seq(
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
    )
  ) dependsOn parquetAvroExtra
}
