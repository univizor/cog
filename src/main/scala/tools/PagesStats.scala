package tools

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import utils._

object PagesStats {
  final val APP_NAME = "PagesProcessor"

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(APP_NAME)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val pages = spark.read.parquet("data/pages")
    pages.createOrReplaceTempView("pgs")

    spark.sqlContext.udf.register("matchOf", (text: String, expression: String) => TextMatcher.matchOf(text, expression))
    spark.sqlContext.udf.register("yearOf", (text: String) => TextMatcher.firstOf(text, "(20\\d{2,2})"))

    // Counting number of all rows
    val q = pages.sqlContext.sql("SELECT COUNT(*) as number_of_pages FROM pgs WHERE pgs.number = 1")
    val row = q.collect()
    println(s"Rows ~> ${row(0)}")

    // UDFs
    val q_2 = pages.sqlContext.sql(
      """
        |
        | SELECT
        |   at_year,
        |   kind,
        |   count(*) as number_of
        | FROM (
        |   SELECT
        |     name,
        |     (
        |       CASE
        |         WHEN (matchOf(page, '(?i)(diplo(ma)?)') != -1) THEN 'dip'
        |         WHEN (matchOf(page, '(?i)(magistr(sk(o|a))?)') != -1) THEN 'mag'
        |         WHEN (matchOf(page, '(?i)(doktorsk(o|a)?)') != -1) then 'dok'
        |       ELSE 'misc' END
        |     ) AS kind,
        |     yearOf(page) AS at_year
        |   FROM pgs
        |   WHERE number = 1
        | ) AS sub_pgs
        | GROUP BY at_year, kind ORDER BY at_year DESC, kind DESC
        | """.stripMargin)

    // q_2.explain()
    q_2.show(false)

    q_2.repartition(1).write.mode(SaveMode.Overwrite).csv("data/reports")

    spark.stop()
  }
}
