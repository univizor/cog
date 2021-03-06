name := "cog"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += "Plasma Conduit Repository" at "http://dl.bintray.com/plasmaconduit/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += Resolver.bintrayRepo("yetu", "maven")

val sparkVersion = "2.0.2"
val pdfboxVersion = "2.0.3"
val scalaTestVersion = "3.0.0"
val opennlpVersion = "1.6.0"

libraryDependencies ++= Seq(
  "org.apache.pdfbox" % "pdfbox" % pdfboxVersion,
  "org.apache.pdfbox" % "fontbox" % pdfboxVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.apache.opennlp" % "opennlp-tools" % opennlpVersion,

  "org.bouncycastle" % "bcprov-jdk15on" % "1.55",
  "org.bouncycastle" % "bcmail-jdk15on" % "1.55",

  "org.zouzias" %% "spark-lucenerdd" % "0.2.4",

  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4",

  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",

  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-Xfuture",
  "-language:_",
  "-deprecation",
  "-Yno-adapted-args",
  "-Xlint",
  "-Yinline-warnings",
  "-Ywarn-adapted-args",
  "-Ywarn-inaccessible",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Xfatal-warnings",
  "-deprecation:false",
  "-optimise",
  "-Yclosure-elim",
  "-Yinline"
)

mainClass in(Compile, run) := Some("cog.TextSplitter")

fork := true

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.rename
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    if (oldStrategy(x) == MergeStrategy.deduplicate) MergeStrategy.first else oldStrategy(x)
  }
}