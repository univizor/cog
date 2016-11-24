package cog

import org.apache.spark.sql.{SaveMode, Row, Encoders, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.input.{PortableDataStream}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import utils.{PDFReader, SentenceSplitter}

trait CogSparkApp extends App {
  val spark = SparkSession.builder.appName("TextSplitter").getOrCreate()
}

object TextSplitter extends CogSparkApp {
  final val SENTENCE_MIN_LENGTH = 40

  val Array(sourceFiles) = Array[String](args(0))
  val sc = spark.sparkContext

  import spark.implicits._

  def documentToSentences: String => Array[(String, Int)] = (text) => SentenceSplitter.sentences(text)
    .zipWithIndex.filter(p => p._1.length > SENTENCE_MIN_LENGTH)

  def toSentences = udf(documentToSentences)

  try {
    val files = sc.binaryFiles(sourceFiles).map(line => PDFReader.readAsPagesTextDocument(line._1, line._2.open()))

    files.toDF()
      .withColumn("sentences", toSentences('document))
      .write
      .mode(SaveMode.Overwrite)
      .save("data/documents")

  } finally {
    sc.stop()
  }
}





