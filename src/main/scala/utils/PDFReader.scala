package utils

import java.io.DataInputStream
import models._
import org.apache.log4j.{Level, Logger}
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import scala.util.{Try}

object PDFReader {
  final val LEVEL = Level.OFF
  final val NEW_DOCUMENT_SPLIT = "\n"
  Logger.getLogger("org.apache.pdfbox").setLevel(Level.INFO)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDFont").setLevel(LEVEL)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.PDSimpleFont").setLevel(LEVEL)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font.FontManager").setLevel(LEVEL)
  Logger.getLogger("org.apache.pdfbox.pdmodel.font").setLevel(LEVEL)
  Logger.getLogger("org.apache.pdfbox.pdmodel").setLevel(LEVEL)
  Logger.getLogger("org.apache.pdfbox.pdfparser").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.fontbox.util").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.fontbox.ttf").setLevel(Level.ERROR)

  def read(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(1), toPage: Option[Int] = None): (String, PagesDocument) = {
    val document: Option[PDDocument] = Try(PDDocument.load(data)).toOption
    val stripper = new PDFTextStripper()
    var numberOfPages: Int = document.flatMap(doc => Try(doc.getNumberOfPages).toOption).getOrElse(0)
    numberOfPages = toPage.getOrElse(numberOfPages)

    val rawDocument: PagesDocument = PagesDocument(fileName, (fromPage.getOrElse(1) until numberOfPages + 1).map(page => {
      stripper.setStartPage(page)
      stripper.setEndPage(page + 1)
      (page, Try(stripper.getText(document.get)).getOrElse(s"#getText error ${page}"))
    }).toSeq)

    document foreach (_.close())

    (fileName.split("/").last, rawDocument)
  }

  def readPages(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(1), toPage: Option[Int] = None): Seq[(String, Int, String)] = {
    read(fileName, data, fromPage, toPage)._2.pages.map((page) => (fileName.split("/").last, page._1, page._2))
  }

  def readAsPages(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(1), toPage: Option[Int] = None) = {
    readPages(fileName, data, fromPage, toPage).map(page => Page(page._1, page._2, page._3))
  }

  def readAsDocument(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(1), toPage: Option[Int] = None) = {
    TextDocument(fileName.split("/").last, Try(read(fileName, data, fromPage, toPage)._2.pages.map(_._2).mkString(NEW_DOCUMENT_SPLIT)).toOption.getOrElse(""))
  }

  def readAsPagesTextDocument(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(1), toPage: Option[Int] = None) = {
    val pagesDocument: PagesDocument = read(fileName, data, fromPage, toPage)._2

    PagesTextDocument(
      fileName.split("/").last,
      pagesDocument.pages,
      pagesDocument.pages.mkString(NEW_DOCUMENT_SPLIT)
    )
  }
}