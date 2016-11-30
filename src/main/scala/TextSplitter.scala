package cog

import org.apache.spark.sql.{SaveMode, Row, Encoders, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.input.{PortableDataStream}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import utils.{PDFReader, SentenceSplitter}

import scala.collection.mutable.WrappedArray

trait CogSparkApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder.appName("TextSplitter").getOrCreate()
}

object TextSplitter extends CogSparkApp {
  final val SENTENCE_MIN_LENGTH = 40

  val Array(sourceFiles) = Array[String](args(0))
  val sc = spark.sparkContext

  import spark.implicits._

  def documentToSentences: String => Array[(String, Int)] = (text) => SentenceSplitter.sentences(text)
    .zipWithIndex.filter(p => p._1.length > SENTENCE_MIN_LENGTH)

  def documentToSentencesWithPagesX: Array[(Int, String)] => Array[(String, Int, Int)] = (pages) => pages.flatMap {
    case (pageNumber: Int, page: String) => SentenceSplitter.sentences(page).zipWithIndex
      .filter(p => p._1.length > SENTENCE_MIN_LENGTH)
      .map(sPair => (sPair._1, sPair._2, pageNumber))
  }

  def documentToSentencesWithPagesXX: Array[(Int, String)] => Array[(String, Int, Int)] = (pages) => pages.flatMap {
    case (pageNumber: Int, page: String) => SentenceSplitter.sentences(page).zipWithIndex
      .filter(p => p._1.length > SENTENCE_MIN_LENGTH)
      .map(sPair => (sPair._1, sPair._2, pageNumber))
  }

  def documentToSentencesWithPagesY: WrappedArray[Row[(Int, String)]] => Array[Row[(String, Int, Int)]] = (pages) =>
    // pages.map { case (pageNumber: Int, page: String) => ("ok", 1, 1)
    /*
    pages.flatMap {
      case (pageNumber: Int, page: String) => Array.empty[(String, Int, Int)]
    }
    */
    pages.array.flatMap {
      case (pageNumber: Int, page: String) => SentenceSplitter.sentences(page).zipWithIndex
        .filter(p => p._1.length > SENTENCE_MIN_LENGTH)
        .map(sPair => Row(sPair._1, sPair._2, pageNumber))

  def toSentences = udf(documentToSentences)

  def toSentencesWithPages = udf {
    row: WrappedArray[(Int, String)] =>
      documentToSentencesWithPagesY(row)
    // Array[(String, Int, Int)].e

    // Array.empty[(String, Int, Int)]
  }

  try {
    val files = sc.binaryFiles(sourceFiles).map(line => PDFReader.readAsPagesTextDocument(line._1, line._2.open()))

    val documents = files.toDF().cache()
    val documentsWithPages = documents
      .withColumn("sentences", toSentencesWithPages('pages))
    // .write
    // .mode(SaveMode.Overwrite)
    // .save("data/documents")

    documentsWithPages.printSchema()

    documentsWithPages.show(1, true)


    // .write
    // .mode(SaveMode.Overwrite)
    // .save("data/documents")

  } finally {
    sc.stop()
  }
}





