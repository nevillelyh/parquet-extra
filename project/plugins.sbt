// sbt-avro 4.0.2 is published against sbt 2.0.0-M3.
libraryDependencies += Defaults.sbtPluginExtra(
  "com.github.sbt" % "sbt-avro" % "4.0.2",
  "2.0.0-M3",
  "3"
)
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.5.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")
libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.12.2"
