package cog

import org.apache.log4j.{LogManager, Level}
import org.apache.spark.sql.{SparkSession, Row, SQLContext, SaveMode}
import java.util.Properties

trait PointsToPGApp extends App {
  final val logLevel = Level.WARN
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder.appName(this.getClass.getName).getOrCreate()
}

object PointsToPG extends PointsToPGApp {

  import spark.implicits._

  val opts = Map("url" -> "jdbc:postgresql:cog?user=postgres", "dbtable" -> "points")

  try {
    val points = spark.read.load("data/points")

    points.createOrReplaceTempView("points")

    val newPoints = spark.sql("SELECT " +
      "monotonically_increasing_id() AS id," +
      "fileNameA AS source, " +
      "fileNameB AS target " +
      "FROM points")

    val props = new Properties()
    newPoints.write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:postgresql:cog/?user=postgres", "new_points", props)

  } finally {
    spark.stop()
  }
}