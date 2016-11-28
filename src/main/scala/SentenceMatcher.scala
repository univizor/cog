package cog

import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

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

    val tokenizer = new Tokenizer().setInputCol("document").setOutputCol("words")

    val wordsData = tokenizer.transform(documents)
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)

    val ftdData = hashingTF.transform(wordsData)
      .cache()

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(ftdData)

    val rescaledData = idfModel.transform(ftdData)
      .withColumnRenamed("fileName", "label")

    val kmeans = new KMeans()
      // .setK(3)
      .setSeed(1L)
      .setFeaturesCol("rawFeatures")

    val kmeansModel = kmeans.fit(rescaledData)

    val WSSSE = kmeansModel.computeCost(rescaledData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    kmeansModel.clusterCenters.foreach(println)

    println(s"K = ${kmeansModel.getK}")

    kmeansModel.summary
      .predictions
      .select("label", "prediction")
      .show()
    // .printSchema()

  } finally {
    spark.stop()
  }
}
