package cog;
import java.io.DataInputStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.pdfbox.pdfparser.PDFParser
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.pdfbox.pdmodel.PDDocument
import scala.util.{Try, Success, Failure}

object GeneralStats {
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDCIDFontType2").setLevel(Level.OFF)

  final val MIN_PARTITIONS = 10
  final val MASTER = "local[8]"
  final val SAMPLE_SIZE = 500

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(MASTER)
    val sc = new SparkContext(conf)

    val files = sc.binaryFiles("./data/files/", minPartitions = MIN_PARTITIONS).takeSample(false, SAMPLE_SIZE)
    val pages = readPDFs(sc.parallelize(files))

    val metas = extractMeta(pages)

    val perYear = metas.map { case (name: String, seq: Seq[String]) => (seq(0), name) }.countByKey()
    val perKind = metas.map { case (name: String, seq: Seq[String]) => (seq(1), name) }.countByKey()

    println(perYear)
    println(perKind)

    sc.stop()
  }

  def readPDFs(rdd: RDD[(String, PortableDataStream)]) = {
    rdd.flatMapValues(x => readPDF(x.open()))
      .flatMapValues(x => firstPage(x))
      .mapValues { case (doc: PDDocument, result: String) => {
        doc.close()
        result
      }
      }
  }

  def readPDF(dataInputStream: DataInputStream) = Try(PDDocument.load(dataInputStream)).toOption

  def pagesBetween(pDDocument: PDDocument, start: Option[Int] = Some(1), end: Option[Int] = None) = Try[(PDDocument, String)] {
    val stripper = new PDFTextStripper()
    stripper.setStartPage(start.get)
    if (end.isDefined) stripper.setEndPage(end.get)
    (pDDocument, stripper.getText(pDDocument))
  }.toOption

  def firstPage(pDDocument: PDDocument) = pagesBetween(pDDocument, end = Some(1))

  def extractMeta(rdd: RDD[(String, String)]) = {
    rdd.mapValues(text => MetaExtractor.extract(text))
  }
}

object MetaExtractor {
  def extract(text: String) = {
    val extractors: Seq[(String) => String] = Seq(year, kind)
    extractors.map(f => f(text))
  }

  def year(text: String) = "(20\\d{2,2})".r.findFirstIn(text).orNull

  def kind(text: String): String = {
    val d = "(?i)(diplo(ma)?)".r.findFirstIn(text).orNull
    if (d != null) return "diplomsko-delo"
    val m = "(?i)(magistr(sk(o|a))?)".r.findFirstIn(text).orNull
    if (m != null) return "magistrsko-delo"
    val doc = "(?i)(doktorsk(o|a)?)".r.findFirstIn(text).orNull
    if (doc != null) return "doktorska-disertacija"
    null
  }
}
