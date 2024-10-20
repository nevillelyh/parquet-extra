addSbtPlugin("com.github.sbt" % "sbt-avro" % "3.5.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.4.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.2.2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")

libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.12.0"
