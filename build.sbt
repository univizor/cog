name := "cog"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.1"
val pdfboxVersion = "2.0.3"

libraryDependencies ++= Seq(
  "org.apache.pdfbox" % "pdfbox" % pdfboxVersion,
  "org.apache.pdfbox" % "fontbox" % pdfboxVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion,
  "org.bouncycastle" % "bcprov-jdk15on" % "1.55",
  "org.bouncycastle" % "bcmail-jdk15on" % "1.55"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-language:_",
  "-deprecation"
)


resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += "Plasma Conduit Repository" at "http://dl.bintray.com/plasmaconduit/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += Resolver.bintrayRepo("yetu", "maven")