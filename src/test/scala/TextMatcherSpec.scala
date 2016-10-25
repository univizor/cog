import org.scalatest.FunSpec
import utils.TextMatcher

class TextMatcherSpec extends FunSpec {

  describe("matchOf") {
    it("should return first match position") {
      assertResult(17) {
        TextMatcher.matchOf("Tomorrow is nice DAY.", "(?i)(day)")
      }

      assertResult(-1) {
        TextMatcher.matchOf("Today is nice day", "monday")
      }
    }
  }
}
