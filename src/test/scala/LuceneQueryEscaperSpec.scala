import org.scalatest.FunSpec
import utils.LuceneQueryEscaper

class LuceneQueryEscaperSpec extends FunSpec {
  describe("escape") {
    it("escapes special characters") {
      assertResult("test")(LuceneQueryEscaper.escape("test"))
      assertResult("""test with \"""")(LuceneQueryEscaper.escape("""test with """"))
      assertResult("""\(1\+1\)\:2""")(LuceneQueryEscaper.escape("""(1+1):2"""))
    }
  }
}
