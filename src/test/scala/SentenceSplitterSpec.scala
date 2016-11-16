import org.apache.log4j.{Level, Logger, BasicConfigurator}
import org.scalatest.FunSpec
import utils.SentenceSplitter


class SentenceSplitterSpec extends FunSpec {
  var filename = "sample.pdf"

  BasicConfigurator.configure()

  describe("Split") {
    it("Splits content to array of string") {
      val sentences = SentenceSplitter.sentences("Danes je lep dan. Tole je test")
      assertResult(2)(sentences.length)
    }
  }
}
