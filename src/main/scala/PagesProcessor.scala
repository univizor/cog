package cog

import nlp.Lemmatizer
import org.apache.log4j.{Level, Logger}
import utils._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

object PagesProcessor {
  final val APP_NAME = "PagesProcessor"

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(APP_NAME)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val pages = spark.read.parquet("data/pages")
    pages.createOrReplaceTempView("pgs")

    spark.sqlContext.udf.register("matchOf", (text: String, expression: String) => TextMatcher.matchOf(text, expression))
    spark.sqlContext.udf.register("yearOf", (text: String) => TextMatcher.firstOf(text, "(20\\d{2,2})"))
    spark.sqlContext.udf.register("lemmatize", (text: String) => Lemmatizer.lemmatize(text))

    // UDFs
    val q_2 = pages.sqlContext.sql("SELECT name, number, page, lemmatize(page) FROM pgs WHERE number = 2 AND name = 'f26008a185b6ce8d99a6fd779e869551d63d4ccc.pdf'")
    q_2.show(3, false)

    // https://github.com/tensorflow/models/blob/master/syntaxnet/universal.md
    // http://stackoverflow.com/questions/17450652/create-conll-file-as-output-of-stanford-parser

    spark.stop()
  }
}
