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

object TextSplitter {

  final val APP_NAME = "TextSplitter"
  final val DEFAULT_MIN_PARTITIONS = 10
  // val log: Logger = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]): Unit = {
    var master: String = ""
    var withSample: Option[Int] = None
    var minPartitions = 10
    var path = "./data/files"
    var toPath = "./data/pages"
    var fromPage: Option[Int] = Some(1)
    var toPage: Option[Int] = None

    args.sliding(2, 1).toList.collect {
      case Array("master", argsMaster: String) => master = argsMaster
      case Array("withSample", argsSample: String) => withSample = Try(argsSample.toInt).toOption
      case Array("minPartitions", p: String) => minPartitions = p.toInt
      case Array("path", p: String) => path = p
      case Array("toPath", p: String) => toPath = p
      case Array("fromPage", p: String) => fromPage = Try(p.toInt).toOption
      case Array("toPage", p: String) => toPage = Try(p.toInt).toOption
    }

    val conf = new SparkConf().setAppName(APP_NAME)
      .setMaster(master = master)

    val session = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val files = readFiles(session.sparkContext, path, withSample, Some(minPartitions))
    val pages = files.map(pair => PDFReader.readAsPages(pair._1, pair._2.open)).flatMap(raw => raw)

    val df = session.createDataFrame(pages)
    df.write.mode(SaveMode.Append).parquet(toPath)

    session.stop()
  }

  def readFiles(sc: SparkContext, path: String, withSample: Option[Int] = None, minPartitions: Option[Int] = None): RDD[(String, PortableDataStream)] = {
    if (withSample.isDefined) {
      sc.parallelize(sc.binaryFiles(path, minPartitions.getOrElse(DEFAULT_MIN_PARTITIONS)).takeSample(false, withSample.get))
    } else {
      sc.binaryFiles(path)
    }
  }
}





