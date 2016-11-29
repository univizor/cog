package cog

import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{explode}
import scala.collection.mutable.{WrappedArray}

trait CogSparkMatcherApp extends App {
  final val logLevel = Level.ERROR
  LogManager.getRootLogger.setLevel(logLevel)
  LogManager.getLogger("org").setLevel(logLevel)

  val spark = SparkSession.builder.appName("TextSplitter").getOrCreate()
}

object SentenceMatcher extends CogSparkMatcherApp {
  final val NUM_FEATURES = 100
  final val K = 10
  final val SEED = 1L

  import spark.implicits._

  try {
    val documents = spark.read.load("data/documents")
    documents.createOrReplaceTempView("documents")

    val tokenizer = new Tokenizer().setInputCol("document").setOutputCol("words")

    val wordsData = tokenizer.transform(documents)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(NUM_FEATURES)

    val ftdData = hashingTF.transform(wordsData).cache()

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(ftdData)

    val rescaledData = idfModel.transform(ftdData).withColumnRenamed("fileName", "label")

    val kmeans = new KMeans().setK(K).setSeed(SEED).setFeaturesCol("rawFeatures")
    val kmeansModel = kmeans.fit(rescaledData)

    // val WSSSE = kmeansModel.computeCost(rescaledData)
    // println(s"Within Set Sum of Squared Errors = $WSSSE")

    // kmeansModel.summary.predictions.printSchema()

    val predictions = kmeansModel.summary.predictions
      .withColumn("sentence", explode('sentences))

    predictions.createTempView("sentences")

    val sentences = spark.sql("SELECT * FROM sentences")

    println(s"Sentences COUNT = ${sentences.count()}")

    sentences.printSchema()


    // predictions.show(3, false)

    /*
    val sentencesRDD = predictions.rdd.flatMap { row =>
      // row.geAs[WrappedArray[(String, Int)]]("sentences")
      // val sentences = row.getAs[WrappedArray[(String, Int)]]("sentences")
      // sentences.map(pair => (pair._1, pair._2))
      // row.getAs[Array[(String, Int)]]("sentences")

      // (row.getString(1), 10)

      (row.getString(0), 10)
    }
    */

    // sentencesRDD.take(3).foreach(println)

    // sentencesRDD.printSchema()

    // println(s"Count = ${sentencesRDD.count()}")

    // sentencesRDD.foreach(println)


    /* .flatMap { row =>
    row.getAs[Seq[Seq[Double]]]("rawFeatures")
  }    */


    /*
    predictions.rdd.map {
      case Row(label: String, prediction: Int, word: mutable.WrappedArray[String], rawFeatures: Vector, features: Vector) =>
        var di
    }
    */


  } finally {
    spark.stop()
  }
}
