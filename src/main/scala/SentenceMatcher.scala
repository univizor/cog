package cog

import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{SaveMode, Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{explode}
import utils.{TextMatcher, StringMetric}
import scala.collection.mutable.{WrappedArray}
import org.apache.spark.sql.functions.udf

trait CogSparkMatcherApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder.appName("TextSplitter")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()
}

object SentenceMatcher extends CogSparkMatcherApp {
  final val NUM_FEATURES = 100
  final val MIN_SENTENCE_LENGTH = 50
  final val K = 10
  final val SEED = 1L

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val upper: String => String = _.toUpperCase
  val similarity: (String, String) => Double = (a: String, b: String) => StringMetric.distance(a, b)
  spark.sqlContext.udf.register("stringSimilarity", (a: String, b: String) => StringMetric.distance(a, b))

  try {
    val documents = spark.read.load("data/documents")

    val tokenizer = new Tokenizer().setInputCol("document").setOutputCol("words")

    val wordsData = tokenizer.transform(documents)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures")
      .setNumFeatures(NUM_FEATURES)

    val ftdData = hashingTF.transform(wordsData).cache()

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(ftdData)

    val rescaledData = idfModel.transform(ftdData).withColumnRenamed("fileName", "label")

    val kmeans = new KMeans().setK(K).setSeed(SEED)
    val kmeansModel = kmeans.fit(rescaledData)

    // val WSSSE = kmeansModel.computeCost(rescaledData)
    // println(s"Within Set Sum of Squared Errors = $WSSSE")

    val predictions = kmeansModel.summary.predictions.cache()
    predictions.createOrReplaceTempView("predictions")

    spark.sql(
      s"""
         | SELECT label as fileName, prediction, inline(sentences)
         | FROM predictions
         | HAVING sentenceLength > ${MIN_SENTENCE_LENGTH}
         | """.stripMargin)
      .createOrReplaceTempView("sentences")

    val matches = spark.sql(
      s"""
         | SELECT
         |  a.fileName AS fileName_a,
         |  b.fileName AS fileName_b,
         |  a.sentence AS sentence_a,
         |  b.sentence AS sentence_b,
         |  a.pageNumber AS pageNumber_a,
         |  b.pageNumber AS pageNumber_b,
         |  a.sentenceIndex AS sentenceIndex_a,
         |  b.sentenceIndex AS sentenceIndex_b,
         |  stringSimilarity(a.sentence, b.sentence) as similarity
         | FROM sentences a
         | INNER JOIN sentences b
         | WHERE
         |  a.fileName < b.fileName AND
         |  a.prediction = b.prediction AND
         |  a.sentenceLength = b.sentenceLength
         | ORDER BY a.fileName, b.fileName
       """.stripMargin)

    matches.write
      .mode(SaveMode.Overwrite)
      .save("data/matches")
  } finally {
    spark.stop()
  }
}
