package cog

import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.sql.SparkSession

trait CogSparkMatcherApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder.appName("TextSplitter").getOrCreate()
}

object SentenceMatcher extends CogSparkMatcherApp {

  import spark.implicits._

  try {

    val documents = spark.read.load("data/documents")
    documents.createOrReplaceTempView("documents")

    spark.sql("SELECT COUNT(*) from documents").show

  } finally {
    spark.stop()
  }
}
