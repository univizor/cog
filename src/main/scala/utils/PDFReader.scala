package utils

import java.io.DataInputStream

import models.{RawDocument, Page}
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import scala.util.Try

object PDFReader {
  def read(fileName: String, data: DataInputStream, fromPage: Option[Int] = Some(0), toPage: Option[Int] = None): (String, RawDocument) = {
    val document: Option[PDDocument] = Try(PDDocument.load(data)).toOption
    val stripper = new PDFTextStripper()
    val numberOfPages = document.flatMap(doc => Try(doc.getNumberOfPages).toOption).getOrElse(0)

    val rawDocument: RawDocument = RawDocument(fileName, pages =
      (0 until numberOfPages).map(page => {
        stripper.setStartPage(page)
        stripper.setEndPage(page + 1)
        (page, Try(stripper.getText(document.get)).getOrElse(s"#getText error ${page}"))
      }).toSeq
    )

    document.map(_.close)

    (fileName.split("/").last, rawDocument)
  }

  def readPages(fileName: String, data: DataInputStream): Seq[(String, Int, String)] = {
    read(fileName, data)._2.pages.map((page) => (fileName.split("/").last, page._1, page._2))
  }

  def readAsPages(fileName: String, data: DataInputStream) = {
    readPages(fileName, data).map(page => Page(page._1, page._2, page._3))
  }
}
