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
  final val MAX_SENTENCE_LENGTH = spark.conf.getOption("max_sentence_length").getOrElse("400").toInt
  final val SENTENCE_FUZZINESS = spark.conf.getOption("sentence_fuzziness").getOrElse("0.99").toFloat

  val logger = LogManager.getLogger("SentenceMatcherX")
  val start = System.currentTimeMillis()

  import spark.implicits._

  try {
    val documents = spark.read.load("data/documents")
    documents.createOrReplaceTempView("documents")

    logger.info(s"Documents count: ${documents.count()}")

    val sentences = spark.sql(
      s"""| SELECT fileName, inline(sentences) FROM documents
          |
          | HAVING sentenceLength BETWEEN $MIN_SENTENCE_LENGTH AND $MAX_SENTENCE_LENGTH
          | """.stripMargin) // WHERE fileName LIKE '%novica%'

    logger.info(s"Sentences count is ${sentences.count()}.")

    val rawSentencesRDD = sentences.select('fileName, 'pageNumber, 'sentenceIndex, 'sentenceLength, 'sentence)

    val sentencesRDD = LuceneRDD(rawSentencesRDD)
    sentencesRDD.cache()

    val luceneRDDCount: Long = sentencesRDD.count()
    logger.info(s"LuceneRDD count: ${luceneRDDCount}")

    println("~*" * 40)

    val results = sentencesRDD.termQuery("sentence", "pogodba", 10)
    results.foreach(println)

    println("~*" * 40)

    val rowLinker: Row => String = {
      case row => {
        val fileName = row.getString(0)
        val sentence = row.getString(4)
        s"""(sentence:"$sentence"~$SENTENCE_FUZZINESS) AND !(fileName:"$fileName")"""
      }
    }

    val linked = sentencesRDD.linkDataFrame(rawSentencesRDD, rowLinker, luceneRDDCount.toInt)

    /*
    val linkageResults = spark.createDataFrame(linkedResults.filter(_._2.nonEmpty).map{ case (acm, topDocs) => (topDocs.head.doc.textField("id").head, acm.getInt(acm.fieldIndex("id")).toString)})
      .toDF("idDBLP", "idACM")
      */


    val linkedResults = linked.filter(_._2.nonEmpty)

    // linkedResults.foreach(println)


    linkedResults.take(10).foreach {
      case (row, docs) => {
        println("\n" * 1)
        println("#" * 40)

        val fileName = row.getString(0)
        val sentence = row.getString(4)
        println(s"[$fileName] $sentence")
        println("-" * 20)

        docs.foreach(sparkScoreDoc => {
          val score = sparkScoreDoc.score
          val sentence = sparkScoreDoc.doc.textField("sentence").getOrElse("[nothing]")
          val fileName = sparkScoreDoc.doc.textField("fileName").getOrElse("[nothing]")
          println(s"--> $fileName [$score] $sentence")
        })
      }
    }


  } finally {
    val end = System.currentTimeMillis()
    logger.info(s"Elapsed time: ${(end - start) / 1000.0} seconds.")
    spark.stop()
  }
}
