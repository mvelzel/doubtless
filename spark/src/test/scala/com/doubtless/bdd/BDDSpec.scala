import org.scalatest.funspec.FixtureAnyFunSpec
import org.scalatest.BeforeAndAfterEach
import com.typesafe.config.ConfigFactory
import com.doubtless.bdd._

class BDDSpec extends FixtureAnyFunSpec with BeforeAndAfterEach {
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

  val configPath = "com.doubtless.spark";
  var originalProperties: Map[String, String] = Map()
  override def beforeEach() = {
    originalProperties =
      sys.props.filter(tup => tup._1.startsWith(configPath)).toMap

    sys.props += s"$configPath.aggregations.prune-method" -> "each-operation"

    ConfigFactory.invalidateCaches()
  }

  override def afterEach() = {
    originalProperties.foreach { case (k, v) =>
      sys.props.update(k, v)
    }

    val testProps =
      sys.props.filter(tup => tup._1.startsWith(configPath)).keySet
    testProps.diff(originalProperties.keySet).foreach(sys.props.remove)

    ConfigFactory.invalidateCaches()
  }

  describe("A BDD") {
    it("should correctly instantiate") { fixture =>
      assert(fixture.bdd1 == BDD("(x=1&y=1)|(z=2)"))
      assert(fixture.bdd2 == BDD("(x=2&z=1&y=2)"))
    }

    it("should correctly determine falsehood") { _ =>
      val bdd1 = BDD("x=1")
      val bdd2 = BDD("x=2")
      val falseBdd = bdd1 & bdd2
      assert(!falseBdd.strictEquals(BDD.False))
      assert(falseBdd.equals(BDD.False))
      assert(falseBdd.isFalse())
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
