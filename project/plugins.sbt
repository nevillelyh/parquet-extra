addSbtPlugin("com.cavorite" % "sbt-avro" % "3.2.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.8.2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.7")

libraryDependencies += "org.apache.avro" % "avro-compiler" % "1.10.2"
