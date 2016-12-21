package cog

import org.apache.log4j.{LogManager, Level}
import org.apache.spark.sql.{SparkSession, Row, SQLContext, SaveMode}
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}

trait SentenceMatcherXSparkApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder
    .appName(this.getClass.getName)
    .getOrCreate()
}

case class Sentence(fileName: String, pageNumber: Int, sentenceIndex: Int, sentenceLength: Int, sentence: String)

object SentenceMatcherX extends SentenceMatcherXSparkApp {
  final val MIN_SENTENCE_LENGTH = spark.conf.getOption("min_sentence_length").getOrElse("20").toInt
  final val MAX_SENTENCE_LENGTH = spark.conf.getOption("max_sentence_length").getOrElse("200").toInt

  val logger = LogManager.getLogger("SentenceMatcherX")
  val start = System.currentTimeMillis()

  import spark.implicits._

  try {
    val documents = spark.read.load("data/documents")
    documents.createOrReplaceTempView("documents")

    logger.info(s"Documents count: ${documents.count()}")

    val sentences = spark.sql(
      s"""| SELECT fileName, inline(sentences) FROM documents
          | HAVING sentenceLength BETWEEN $MIN_SENTENCE_LENGTH AND $MAX_SENTENCE_LENGTH
          | """.stripMargin)

    logger.info(s"Sentences count is ${sentences.count()}.")

    val sentencesRDD = LuceneRDD(
      sentences.select('fileName, 'pageNumber, 'sentenceIndex, 'sentenceLength, 'sentence))
    sentencesRDD.cache()

    logger.info(s"LuceneRDD count: ${sentencesRDD.count()}")

    val results = sentencesRDD.termQuery("sentence", "diploma", 10)
    results.foreach(println)

  } finally {
    val end = System.currentTimeMillis()
    logger.info(s"Elapsed time: ${(end - start) / 1000.0} seconds.")
    spark.stop()
  }
}
