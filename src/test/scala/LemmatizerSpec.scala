import nlp.{HlavkiLemmatizer, ZitnikLemmatizer, Lemmatizer}
import org.scalatest.FunSpec
import org.scalatest.Matchers._

class LemmatizerSpec extends FunSpec {
  describe("lemmatize") {
    val originalText =
      """
        |Z Mazzinijem smo se igrali vse dni.
        |Bilo je številke 12 in veliko prostora za posebne - | @ znake in '  '.""".stripMargin

    it("HlavkiLemmatizer") {
      object L extends HlavkiLemmatizer {}
      val text: String = L.lemmatize(originalText)
      assertResult(true)("(biti)".r.findFirstIn(text).getOrElse("") != "")
    }

    it("ZitnikLemmatizer") {
      object L extends ZitnikLemmatizer {}
      val text = L.lemmatize(originalText)

      text should include("biti")

      for (page <- 0 until 1000) {
        L.lemmatize(originalText) should include("biti")
      }
    }
  }
}
