addSbtPlugin("com.github.sbt" % "sbt-avro" % "4.0.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.4.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")

libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.12.0"
