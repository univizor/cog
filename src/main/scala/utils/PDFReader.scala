package utils

import java.io.DataInputStream
import models.{RawDocument, Page}
import org.apache.log4j.{Level, Logger}
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import scala.util.{Try}

object PDFReader {
  final val level = Level.OFF
  Logger.getLogger("org.apache.pdfbox").setLevel(Level.INFO)
  Logger.getLogger("org.apache.fontbox.util.autodetect.FontFileFinder").setLevel(level)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDFont").setLevel(level)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDSimpleFont").setLevel(level)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.FontManager").setLevel(level)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font").setLevel(level)
  Logger.getLogger("org.apache.pdfbox.pdmodel").setLevel(level)

  def read(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(0), toPage: Option[Int] = None): (String, RawDocument) = {
    val document: Option[PDDocument] = Try(PDDocument.load(data)).toOption
    val stripper = new PDFTextStripper()
    var numberOfPages: Int = document.flatMap(doc => Try(doc.getNumberOfPages).toOption).getOrElse(0)
    numberOfPages = toPage.getOrElse(numberOfPages)

    val rawDocument: RawDocument = RawDocument(fileName, pages =
      (0 until numberOfPages).map(page => {
        stripper.setStartPage(page)
        stripper.setEndPage(page + 1)
        (page, Try(stripper.getText(document.get)).getOrElse(s"#getText error ${page}"))
      }).toSeq
    )

    document foreach (_.close())

    (fileName.split("/").last, rawDocument)
  }

  def readPages(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(0), toPage: Option[Int] = None): Seq[(String, Int, String)] = {
    read(fileName, data, fromPage, toPage)._2.pages.map((page) => (fileName.split("/").last, page._1, page._2))
  }

  def readAsPages(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(0), toPage: Option[Int] = None) = {
    readPages(fileName, data, fromPage, toPage).map(page => Page(page._1, page._2, page._3))
  }
}
