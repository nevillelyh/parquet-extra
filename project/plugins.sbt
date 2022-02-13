addSbtPlugin("com.github.sbt" % "sbt-avro" % "3.4.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.11")

libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.11.0"
