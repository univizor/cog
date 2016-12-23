
case class Sentence(fileName: String, pageNumber: Int)

case class Point(fileNameA: String, fileNameB: String, pageNumberA: Int, pageNumberB: Int)

object Point {
  def apply(sentenceA: Sentence, sentenceB: Sentence) = new Point(sentenceA.fileName, sentenceB.fileName, sentenceA.pageNumber, sentenceB.pageNumber)
}

val a = Point(Sentence("file-a.pdf", 10), Sentence("file-b.pdf", 20))