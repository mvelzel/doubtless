import org.scalatest.funspec.FixtureAnyFunSpec
import com.doubtless.bdd._
import org.scalactic.TripleEquals._
import org.scalactic.Tolerance._

class ProbDictSpec extends FixtureAnyFunSpec {
  type FixtureParam = ProbDict
  override def withFixture(test: OneArgTest) = {
    val dict = ProbDict(
      RandVar("x", 1) -> 0.6,
      RandVar("y", 1) -> 1.4,
      RandVar("y", 2) -> 0.6,
      RandVar("x", 2) -> 0.4,
      RandVar("z", 1) -> 0.1,
      RandVar("z", 2) -> 0.9
    );
    test(dict)
  }

  describe("A ProbDict") {
    it("should correctly instantiate and normalise") { dict =>
      assert(
        dict.toString() == "x=1:0.600000; x=2:0.400000; y=1:0.700000; y=2:0.300000; z=1:0.100000; z=2:0.900000;"
      )
    }

    it("should correctly retrieve probability values") { dict =>
      assert(dict(RandVar("x", 1)) == 0.6)
      assert(dict(RandVar("x", 2)) == 0.4)

      assert(dict(RandVar("y", 1)) == 0.7)
      assert(dict(RandVar("y", 2)) == 0.3)

      assert(dict(RandVar("z", 1)) == 0.1)
      assert(dict(RandVar("z", 2)) == 0.9)
    }

    it("should correctly remove and normalise random variables") { dict =>
      assert(dict(RandVar("y", 1)) == 0.7)

      val newDict = dict - RandVar("y", 2)
      assert(newDict(RandVar("y", 1)) == 1)
      assert(!(newDict contains RandVar("y", 2)))

      val finalDict = newDict - RandVar("y", 1)
      assert(!(finalDict contains RandVar("y", 1)))
    }

    it("should correctly add and normalise random variables") { dict =>
      assert(!(dict contains RandVar("w", 1)))

      val newDict = dict + (RandVar("w", 1) -> 0.5)
      assert(newDict(RandVar("w", 1)) == 1)
      val finalDict = newDict + (RandVar("w", 2) -> 1.0 / 3.0)
      assert(finalDict(RandVar("w", 2)) === 0.25 +- 0.001)
      assert(finalDict(RandVar("w", 1)) === 0.75 +- 0.001)

      val instDict = ProbDict(
        RandVar("x", 1) -> 0.3,
        RandVar("x", 2) -> 0.5,
        RandVar("x", 3) -> 0.2,
        RandVar("y", 1) -> 1.0,
        RandVar("y", 2) -> 1.5
      )
      assert(instDict(RandVar("x", 1)) === 0.3 +- 0.001)
      assert(instDict(RandVar("x", 2)) === 0.5 +- 0.001)
      assert(instDict(RandVar("x", 3)) === 0.2 +- 0.001)
      assert(instDict(RandVar("y", 1)) === 0.4 +- 0.001)
      assert(instDict(RandVar("y", 2)) === 0.6 +- 0.001)
    }

    it("should correctly update and normalise random variables") { dict =>
      assert(dict(RandVar("x", 1)) == 0.6)

      val newDict = dict + (RandVar("x", 1) -> 0.4)
      assert(newDict(RandVar("x", 1)) == 0.5)
      assert(newDict(RandVar("x", 2)) == 0.5)

      assert(dict(RandVar("y", 2)) == 0.3)

      val finalDict = newDict + (RandVar("y", 2) -> 0.0)
      assert(finalDict(RandVar("y", 1)) == 1.0)
      assert(finalDict(RandVar("y", 2)) == 0.0)
    }

    it("should correctly iterate the variables and their probabilities") { dict =>
      var total = 0
      dict.foreach({ case (v, p) =>
        assert(dict(v) == p) 
        total += 1
      })

      assert(total == dict.size)
    }

    describe("with another ProbDict") {
      it("should correctly merge and normalise their variables and probabilities") { dict =>
        val otherDict = ProbDict(
          RandVar("x", 1) -> 0.1,
          RandVar("x", 3) -> 0.9,
          RandVar("y", 2) -> 0.1,
          RandVar("y", 3) -> 0.5,
          RandVar("y", 4) -> 0.4
        )

        val newDict = dict ++ otherDict
        assert(newDict(RandVar("x", 1)) === 0.1 / 1.4 +- 0.001)
        assert(newDict(RandVar("x", 2)) === 0.4 / 1.4 +- 0.001)
        assert(newDict(RandVar("x", 3)) === 0.9 / 1.4 +- 0.001)

        assert(newDict(RandVar("y", 1)) === 0.7 / 1.7 +- 0.001)
        assert(newDict(RandVar("y", 2)) === 0.1 / 1.7 +- 0.001)
        assert(newDict(RandVar("y", 3)) === 0.5 / 1.7 +- 0.001)
        assert(newDict(RandVar("y", 4)) === 0.4 / 1.7 +- 0.001)
      }
    }
  }
}
