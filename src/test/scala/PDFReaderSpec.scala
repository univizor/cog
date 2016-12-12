import java.io.DataInputStream

import models._
import org.apache.log4j.{Level, Logger, BasicConfigurator}
import org.scalatest.FunSpec
import scala.io.Source
import utils.PDFReader


class PDFReaderSpec extends FunSpec {
  var filename = "sample.pdf"

  BasicConfigurator.configure()

  def readTestFile(path: String): DataInputStream = {
    val stream = getClass.getResourceAsStream(s"/files/${path}")
    new DataInputStream(stream)
  }

  def file = readTestFile(filename)

  describe("read") {
    it("Reads PDFs") {
      val pages = PDFReader.read(filename, file)
      assertResult(filename)(pages._1)
      assertResult(79)(pages._2.pages.length)
    }

    it("Does not read non-PDFs") {
      val pages = PDFReader.read("test.txt", readTestFile("test.txt"))
      assertResult(0)(pages._2.pages.length)
    }
  }

  describe("readPages") {
    it("All pages by default") {
      val pages = PDFReader.readPages(filename, file)
      assertResult(79)(pages.length)
    }
    it("Reads between pages") {
      val pages = PDFReader.readPages(filename, file, Some(1), Some(2))
      assertResult(1)(pages.head._2)
    }
  }

  describe("readAsPages") {
    it("Reads as Page") {
      val pages = PDFReader.readAsPages(filename, file, Some(1), Some(2))
      assertResult(2)(pages.length)
      assertResult(1)(pages.head.number)
    }
  }
}
