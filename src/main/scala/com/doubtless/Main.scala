package com.doubtless

import scala.util._
import com.doubtless.bdd._

object Main extends App {
  val dictString =
    "x=1 : 0.6; x=2 : 0.4; y=1 : 0.7; y=2 : 0.3; z=1 : 0.1; z=2 : 0.9; "
  val dict = new ProbDict(dictString);

  println(s"The probability dictionary: $dict")

  val bdd1 = new BDD("(x=1&y=1)|(z=2)")
  println(s"The test bdd1 expr: $bdd1")
  println(s"The test bdd1 prob: ${bdd1.probability(dict)}")

  println(
    s"The test !bdd1 expr: ${~bdd1}"
  )
  println(
    s"The test !bdd1 prob: ${(~bdd1).probability(dict)}"
  )

  val bdd2 = new BDD("(x=2&z=1&y=2)")
  println(s"The test bdd2 expr: $bdd2")
  println(s"The test bdd2 prob: ${bdd2.probability(dict)}")

  println(
    s"The test !bdd2 expr: ${~bdd2}"
  )
  println(
    s"The test !bdd2 prob: ${(~bdd2).probability(dict)}"
  )

  val bddAnd = bdd1 & bdd2
  println(s"The test bdds after & expr: $bddAnd")
  println(s"The test bdds after & prob: ${bddAnd.probability(dict)}")

  val bddOr = bdd1 | bdd2
  println(s"The test bdds after | expr: $bddOr")
  println(s"The test bdds after | prob: ${bddOr.probability(dict)}")
}
