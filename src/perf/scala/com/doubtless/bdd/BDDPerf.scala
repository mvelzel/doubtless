import org.scalatest.funspec.AnyFunSpec
import com.doubtless.bdd._

class BDDPerf extends AnyFunSpec {
  describe("A BDD") {
    it("should be able to quickly be instantiated") {
      val exprs = Array(
        "1",
        "x=1",
        "(x=1|y=1)",
        "(x=1&y=1)|(z=5)",
        "(c=0&!b=0)",
        "x=1|(y=1 & x=2)",
        "(!(c=0|a=1)|!(b=2))",
        "(z=1)&!((x=1)&((y=1|y=2)&x=2))",
        "(x=8|x=2|x=4|x=3|x=9|(x=6|q=4&p=6)|x=7|x=1|x=5)",
        "(x=1&x=2|x=3&x=4|x=5&x=6|x=7&x=8|y=4)"
      )

      val instantiations = 10_000_000
      val size = exprs.size

      val t0 = System.currentTimeMillis()

      1 to instantiations foreach { i =>
        BDD(exprs(i % size))
      }

      val t1 = System.currentTimeMillis()

      println(s"Elapsed time: ${t1 - t0}ms")
    }

    it("should be able to quickly be combined") {
      val leftBDDs = Array(
        BDD("x=1"),
        BDD("x=1|(!y=3)"),
        BDD("(x=2&z=4)")
      )

      val rightBDDs = Array(
        BDD("y=1"),
        BDD("y=1&z=2"),
        BDD("(y=3|z=4)")
      )

      val repeat = 1_000_000
      var leftSize = leftBDDs.size
      var rightSize = rightBDDs.size

      val t0 = System.currentTimeMillis()

      1 to repeat foreach { _ =>
        leftBDDs.foreach { left =>
          rightBDDs.foreach { right =>
            left & right
            left | right
          }
        }
      }

      val t1 = System.currentTimeMillis()

      println(s"Elapsed time: ${t1 - t0}ms")
    }

    it("should be able to quickly be negated") {
      val bdds = Array(
        BDD("1"),
        BDD("x=1"),
        BDD("(x=1|y=1)"),
        BDD("(x=1&y=1)|(z=5)"),
        BDD("(c=0&!b=0)"),
        BDD("x=1|(y=1 & x=2)"),
        BDD("(!(c=0|a=1)|!(b=2))"),
        BDD("(z=1)&!((x=1)&((y=1|y=2)&x=2))"),
        BDD("(x=8|x=2|x=4|x=3|x=9|(x=6|q=4&p=6)|x=7|x=1|x=5)"),
        BDD("(x=1&x=2|x=3&x=4|x=5&x=6|x=7&x=8|y=4)")
      )

      val repeat = 1_000_000
      var size = bdds.size

      val t0 = System.currentTimeMillis()

      1 to repeat foreach { _ =>
        bdds.foreach { bdd =>
          ~bdd
        }
      }

      val t1 = System.currentTimeMillis()

      println(s"Elapsed time: ${t1 - t0}ms")
    }
  }
}
