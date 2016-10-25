package models

case class RawDocument(fileName: String, pages: Seq[(Int, String)])

object RawDocument {
}

case class Page(name: String, number: Int, page: String) {

}

object Page {

}