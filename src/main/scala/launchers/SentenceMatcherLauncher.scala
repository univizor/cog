package launchers

import cog.SentenceMatcher
import org.apache.log4j.{Level, LogManager}

object SentenceMatcherLauncher extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  System.setProperty("spark.master", "local[*]")

  SentenceMatcher.main(args)
}
