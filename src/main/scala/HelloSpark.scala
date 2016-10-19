import java.io.DataInputStream
import java.util.logging.LogManager

import org.apache.hadoop.io.LongWritable
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.pdfbox.pdfparser.PDFParser
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.pdfbox.pdmodel.{PDDocument}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

object HelloSpark extends App {

  def pages(dataInputStream: DataInputStream, start: Option[Int] = Some(1), end: Option[Int] = None): String = {
    try {
      val document = PDDocument.load(dataInputStream)
      val stripper = new PDFTextStripper()
      if (start.isDefined) stripper.setStartPage(start.get)
      if (end.isDefined) stripper.setEndPage(end.get)
      val text = stripper.getText(document)
      document.close
      return text
    } catch {
      case e: Exception => println(e)
    }

    null
  }

  def tags(text: String): Array[String] = {
    if (text == null) return Array.empty[String]
    var list = List.empty[String]
    list = list :+ s"year:${year(text)}"
    list = list :+ s"kind:${kind(text)}"
    list.toArray
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

  val sparkConf = new SparkConf().setMaster("local[8]")
    .setAppName(HelloSpark.getClass.getName)

  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")

  val path = "./"
  val files = sc.binaryFiles(path + "data/files/*", 10)

  val pdfDocuments = files.map {
    case (fileName, stream) => (fileName.split("/").last, pages(stream.open(), Some(1), Some(2)))
  }
    .filter { case (key, text) => text != null }
    .map { case (key, text) => (key, tags(text).mkString(";")) }

  pdfDocuments.foreach(x => println(x._1, x._2))

  pdfDocuments.saveAsTextFile("dump")

  sc.stop()
}

