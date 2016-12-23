package launchers

import cog.PointsToPG

import org.apache.log4j.{Level, LogManager}

object PointsToPGLauncher extends App {
  /*
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)
    */
  System.setProperty("spark.master", "local[7]")

  override val args = Array.empty[String]

  PointsToPG.main(args)
}
