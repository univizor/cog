package cog

import java.io.DataInputStream

import org.apache.spark.sql.{SaveMode, Row, Encoders, SparkSession}
import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.input.{PortableDataStream}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import utils.{PDFReader, SentenceSplitter}

trait CogSparkApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder.appName("TextSplitter").getOrCreate()
}

object TextSplitter extends CogSparkApp {
  final val SENTENCE_MIN_LENGTH = 40
  final val PAGES_SEPARATOR = "\n\n"

  val Array(sourceFiles) = Array[String](args(0))
  val sc = spark.sparkContext

  import spark.implicits._

  case class Sentence(pageNumber: Int, sentenceIndex: Int, sentence: String)

  case class Document(fileName: String, document: String, sentences: Seq[Sentence])

  def documentWithSentences(fileName: String, dataInputStream: DataInputStream) = {
    val (documentName, pagesDocument) = PDFReader.read(fileName, dataInputStream)
    Document(
      documentName,
      document = pagesDocument.pages.map(_._2).mkString(PAGES_SEPARATOR),
      sentences = pagesDocument.pages.flatMap {
        case (pageNumber: Int, page: String) =>
          SentenceSplitter.sentences(page).zipWithIndex.map {
            case (sentence: String, sentenceIndex: Int) => Sentence(pageNumber, sentenceIndex, sentence)
          }
      })
  }

  try {
    val files = sc.binaryFiles(sourceFiles).map(line => documentWithSentences(line._1, line._2.open()))

    val documents = files.toDF().cache()

    documents
      .write
      .mode(SaveMode.Overwrite)
      .save("data/documents")
  } finally {
    sc.stop()
  }
}




