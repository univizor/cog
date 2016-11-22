package cog

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.input.{PortableDataStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.{Try, Success, Failure}
import models._
import utils._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe.TypeTag

object SentenceProcessor {
  final val APP_NAME = "SentenceProcessor"
  final val DEFAULT_MIN_PARTITIONS = 10
  final val MIN_SENTENCE_LENGTH = 40
  final val MATCH_THRESHOLD = 0.90

  def main(args: Array[String]): Unit = {
    val master: String = "local[*]"
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(master)
    val sc = SparkSession.builder().config(conf).getOrCreate()

    val sentences = sc.read.parquet("data/sentences")
    sentences.createOrReplaceTempView("sentences")

    // sentences.show(10)
    sentences.printSchema()

    val results = sentences.sqlContext.sql(s"" +
      s"SELECT * FROM sentences s " +
      s"WHERE s.pageNumber > 5 AND LENGTH(s.sentence) > ${MIN_SENTENCE_LENGTH} " + // Ignore "Index" page
      s"ORDER BY s.fileName ASC, s.pageNumber ASC, s.sentenceNumber ASC")

    val matching = results.rdd.cartesian(results.rdd)
      .filter { case (a: Row, b: Row) => a.getString(0) < b.getString(0) }
      .map { case (a: Row, b: Row) => (
        a.getString(0), // Document A
        b.getString(0), // Document B
        a.getInt(1), // Document A pageNumber
        b.getInt(1), // Document B pageNumber
        a.getInt(2), // Document A sentenceNumber
        b.getInt(2), // Document B sentenceNumber
        StringMetric.distance(a.getString(3), b.getString(3)), // Number
        a.getString(3), // Document A sentence
        b.getString(3) // Document B sentence
        )
      }
      .filter {
        case (row) => row._7 >= MATCH_THRESHOLD
      }

    matching.take(5000).foreach { row => println(row) }

    sc.stop()
  }
}