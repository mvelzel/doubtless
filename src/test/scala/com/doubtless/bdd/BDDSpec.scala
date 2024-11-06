import org.scalatest.funspec.FixtureAnyFunSpec
import com.doubtless.bdd._

class BDDSpec extends FixtureAnyFunSpec {
  case class FixtureParam(bdd1: BDD, bdd2: BDD, dict: ProbDict)

  override def withFixture(test: OneArgTest) = {
    val fixture = FixtureParam(
      BDD("(x=1&y=1)|(z=2)"),
      BDD("(x=2&z=1&y=2)"),
      ProbDict(
        RandVar("x", 1) -> 0.6,
        RandVar("x", 2) -> 0.4,
        RandVar("y", 1) -> 0.7,
        RandVar("y", 2) -> 0.3,
        RandVar("z", 1) -> 0.1,
        RandVar("z", 2) -> 0.9
      )
    )
    test(fixture)
  }

  describe("A BDD") {
    it("should correctly instantiate") { fixture =>
      assert(fixture.bdd1 == BDD("(x=1&y=1)|(z=2)"))
      assert(fixture.bdd2 == BDD("(x=2&z=1&y=2)"))
    }

    it("should provide the correct string representation") { fixture =>
        assert(fixture.bdd1.toString() == "BDD(((x=1&(y=1|z=2))|(!x=1&z=2)))")
        assert(fixture.bdd2.toString() == "BDD((x=2&(y=2&z=1)))")
    }

    it("should throw a relevant error with an invalid expression") { _ =>
      assertThrows[IllegalArgumentException](BDD("bad argument"))
    }

    it("should correctly invert with ~") { fixture =>
        assert(~fixture.bdd1 == BDD("!((x=1&y=1)|(z=2))"))
        assert(~fixture.bdd2 == BDD("!(x=2&z=1&y=2)"))
    }

    it("should correctly calculate probabilities with a dict") { fixture =>
        assert(fixture.bdd1.probability(fixture.dict) == 0.942)
        assert(fixture.bdd2.probability(fixture.dict) == 0.012)
    }

    describe("with another BDD") {
      it("should correctly determine equality") { fixture =>
          assert(fixture.bdd1 == BDD("!(!(z=2)&!(x=1&y=1))"))
          assert(fixture.bdd2 == BDD("!(!(!(!(x=2)|!(z=1)))|!(y=2))"))
          assert(BDD("(x=2&!x=1)|(0&x=2)") == BDD("(x=2)|(0&(x=2&!x=1))"))
          assert(BDD("x=2&!x=1") == BDD("x=2"))
          assert(BDD("x=2") == BDD("x=2&!x=1"))

          assert(fixture.bdd1 != fixture.bdd2)
          assert(fixture.bdd1 != null)
      }

      it("should correctly combine with |") { fixture =>
          assert((fixture.bdd1 | fixture.bdd2) == BDD("(x=1&y=1)|(z=2)|(x=2&z=1&y=2)"))
      }

      it("should correctly combine with &") { fixture =>
          assert((fixture.bdd1 & fixture.bdd2) == BDD("((x=1&y=1)|(z=2))&(x=2&z=1&y=2)"))
      }
    }
  }
}
