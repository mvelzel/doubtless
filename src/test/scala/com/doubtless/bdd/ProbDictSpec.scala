import org.scalatest.funspec.FixtureAnyFunSpec
import com.doubtless.bdd._

class ProbDictSpec extends FixtureAnyFunSpec {
  type FixtureParam = ProbDict
  override def withFixture(test: OneArgTest) = {
    val dict = new ProbDict(
      "x=1 : 0.6; x=2 : 0.4; y=1 : 0.7; y=2 : 0.3; z=1 : 0.1; z=2 : 0.9;"
    )
    test(dict)
  }

  describe("A ProbDict") {
    it("should correctly instantiate") { dict =>
      assert(
        dict.toString() == "x=1:0.600000; x=2:0.400000; y=1:0.700000; y=2:0.300000; z=1:0.100000; z=2:0.900000;"
      )
    }

    it("should correctly remove and normalise records") { dict =>
      assert(dict(RandVar("y", 1)) == 0.7)

      val newDict = dict - RandVar("y", 2)
      assert(newDict(RandVar("y", 1)) == 1)
      assert(!(newDict contains RandVar("y", 2)))

      val finalDict = newDict - RandVar("y", 1)
      assert(!(finalDict contains RandVar("y", 1)))
    }
  }
}
