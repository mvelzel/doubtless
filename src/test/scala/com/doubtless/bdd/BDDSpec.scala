import org.scalatest.funspec.FixtureAnyFunSpec
import com.doubtless.bdd._

class BDDSpec extends FixtureAnyFunSpec {
  type FixtureParam = (BDD, BDD, ProbDict)
  override def withFixture(test: OneArgTest) = {
    val fixture = (
      new BDD("(x=1&y=1)|(z=2)"),
      new BDD("(x=2&z=1&y=2)"),
      new ProbDict(
        "x=1 : 0.6; x=2 : 0.4; y=1 : 0.7; y=2 : 0.3; z=1 : 0.1; z=2 : 0.9; "
      )
    )
    test(fixture)
  }

  describe("A BDD") {
    it("should correctly instantiate") { (bdd1, bdd2, _) =>
      assert(bdd1 == new BDD("(x=1&y=1)|(z=2)"))
      assert(bdd2 == new BDD("(x=2&z=1&y=2)"))
    }

    it("should provide the correct string representation") { (bdd1, bdd2, _) =>
      assert(bdd1.toString() == "Bdd(((x=1&(y=1|z=2))|(!x=1&z=2)))")
      assert(bdd2.toString() == "Bdd((x=2&(y=2&z=1)))")
    }

    it("should throw a relevant error with an invalid expression") { _ =>
      assertThrows[IllegalArgumentException](new BDD("bad argument"))
    }

    it("should correctly invert with ~") { (bdd1, bdd2, _) =>
      assert(~bdd1 == new BDD("!((x=1&y=1)|(z=2))"))
      assert(~bdd2 == new BDD("!(x=2&z=1&y=2)"))
    }

    it("should correctly calculate probabilities with a dict") {
      (bdd1, bdd2, dict) =>
        assert(bdd1.probability(dict) == 0.942)
        assert(bdd2.probability(dict) == 0.012)
    }

    describe("with another BDD") {
      it("should correctly determine equality") { (bdd1, bdd2, _) =>
        assert(bdd1 == new BDD("!(!(z=2)&!(x=1&y=1))"))
        assert(bdd2 == new BDD("!(!(!(!(x=2)|!(z=1)))|!(y=2))"))

        assert(bdd1 != bdd2)
        assert(bdd1 != null)
      }

      it("should correctly combine with |") { (bdd1, bdd2, _) =>
        assert((bdd1 | bdd2) == new BDD("(x=1&y=1)|(z=2)|(x=2&z=1&y=2)"))
      }

      it("should correctly combine with &") { (bdd1, bdd2, _) =>
        assert((bdd1 & bdd2) == new BDD("((x=1&y=1)|(z=2))&(x=2&z=1&y=2)"))
      }
    }
  }
}
