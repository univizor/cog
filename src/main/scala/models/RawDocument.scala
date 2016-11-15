package models

sealed trait Document {
  def fileName: String
}

case class PagesDocument(fileName: String, pages: Seq[(Int, String)]) extends Document

case class TextDocument(fileName: String, document: String) extends Document

case class PagesTextDocument(fileName: String, pages: Seq[(Int, String)], document: String) extends Document

case class Page(fileName: String, number: Int, page: String) extends Document

case class Sentence(fileName: String, pageNumber: Int, sentence: String) extends Document