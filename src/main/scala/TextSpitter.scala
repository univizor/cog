package cog

import java.io.{DataInput, DataInputStream}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.input.{PortableDataStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.pdfbox.pdfparser.PDFParser
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.pdfbox.pdmodel.PDDocument
import utils.PDFReader
import scala.util.{Try, Success, Failure}
import models._
import util._

object TextSpitter {
  final val APP_NAME = "TextSplitter"
  // val log: Logger = LoggerFactory.getLogger(getClass())

  /* Silencing loggers */
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDCIDFontType2").setLevel(Level.OFF)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDFont").setLevel(Level.OFF)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDSimpleFont").setLevel(Level.OFF)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.FontManager").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    var master: String = ""
    var withSample: Option[Int] = None
    var minPartitions = 10
    var path = ""
    var fromPage: Option[Int] = Some(1)
    var toPage: Option[Int] = None

    args.sliding(2, 1).toList.collect {
      case Array("--master", argsMaster: String) => master = argsMaster
      case Array("--withSample", argsSample: String) => withSample = Try(argsSample.toInt).toOption
      case Array("--minPartitions", p: String) => minPartitions = p.toInt
      case Array("--path", p: String) => path = p
      case Array("--fromPage", p: String) => fromPage = Try(p.toInt).toOption
      case Array("--toPage", p: String) => toPage = Try(p.toInt).toOption
    }

    val conf = new SparkConf().setAppName(APP_NAME).setMaster(master = master)
    val sc = new SparkContext(conf)

    val files = readFiles(sc, path, withSample, Some(minPartitions))

    val pages = files.map(pair => PDFReader.readAsPages(pair._1, pair._2.open)).flatMap(raw => raw)

    pages.foreach((page) => {
      println(page)
    })

    if (!sc.isStopped) sc.stop()
  }

  def readFiles(sc: SparkContext, path: String, withSample: Option[Int] = None, minPartitions: Option[Int] = None): RDD[(String, PortableDataStream)] = {
    if (withSample.isDefined) {
      sc.parallelize(sc.binaryFiles(path, minPartitions.getOrElse(10)).takeSample(false, withSample.get))
    } else {
      sc.binaryFiles(path)
    }
  }
}





