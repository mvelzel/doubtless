package com.doubtless

import scala.util._
import com.doubtless.bdd._

object Main extends App {
  val dictString =
    "x=1 : 0.6; x=2 : 0.4; y=1 : 0.7; y=2 : 0.3; z=1 : 0.1; z=2 : 0.9; "
  val dict = Native.createDict(dictString)

  val dictFormatted = Native.dict2string(dict)
  println(s"The probability dictionary: $dictFormatted")

  val bdd1 = Native.createBdd("(x=1&y=1)|(z=2)")
  println(s"The test bdd1 expr: ${Native.bdd2string(bdd1)}")
  println(s"The test bdd1 prob: ${Native.bddProb(dict, bdd1)}")

  println(
    s"The test !bdd1 expr: ${Native.bdd2string(Native.bddOperator("!", bdd1, null))}"
  )
  println(
    s"The test !bdd1 prob: ${Native.bddProb(dict, Native.bddOperator("!", bdd1, null))}"
  )

  val bdd2 = Native.createBdd("(x=2&z=1&y=2)")
  println(s"The test bdd2 expr: ${Native.bdd2string(bdd2)}")
  println(s"The test bdd2 prob: ${Native.bddProb(dict, bdd2)}")

  println(
    s"The test !bdd2 expr: ${Native.bdd2string(Native.bddOperator("!", bdd2, null))}"
  )
  println(
    s"The test !bdd2 prob: ${Native.bddProb(dict, Native.bddOperator("!", bdd2, null))}"
  )

  val bddAnd = Native.bddOperator("&", bdd1, bdd2)
  println(s"The test bdds after & expr: ${Native.bdd2string(bddAnd)}")
  println(s"The test bdds after & prob: ${Native.bddProb(dict, bddAnd)}")

  val bddOr = Native.bddOperator("|", bdd1, bdd2)
  println(s"The test bdds after | expr: ${Native.bdd2string(bddOr)}")
  println(s"The test bdds after | prob: ${Native.bddProb(dict, bddOr)}")

  val operatorError = Try { Native.bddOperator("badoperator", bdd1, bdd2); }
  operatorError match {
    case Failure(e) => println(s"Bad operator test gave exception: $e")
    case Success(_) => println("Should fail!")
  }

  val exprError = Try { Native.createBdd("badexpr"); }
  exprError match {
    case Failure(e) => println(s"Bad expression test gave exception: $e")
    case Success(_) => println("Should fail!")
  }

  val varDefsError = Try { Native.createDict("badvardefs"); }
  varDefsError match {
    case Failure(e) => println(s"Bad varDefs test gave exception: $e")
    case Success(_) => println("Should fail!")
  }
}
