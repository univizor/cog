import org.scalatest.FunSpec
import utils.{StringMetric}

class StringMetricSpec extends FunSpec {

  describe("distance") {
    it("computes distance") {
      assertResult(true) {
        StringMetric.distance("jones", "johnson") > 0.0
      }

      assertResult(true) {
        StringMetric.distance("To je stavek", "") == 0.0
      }

      assertResult(true) {
        StringMetric.distance("", "") == 0.0
      }
    }
  }
}