package launchers

import cog.TextSplitter
import org.apache.log4j.{Level, LogManager}

object TextSplitterLauncher extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  System.setProperty("spark.master", "local[*]")

  val testArgs = Array[String]("./data/10-files")
  TextSplitter.main(testArgs)
}
