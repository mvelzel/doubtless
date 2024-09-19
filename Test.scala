class Test {
  @native def createBdd(expr: String): Array[Byte]
  @native def bdd2string(bdd: Array[Byte]): String
  @native def bddOperator(operator: String, leftBdd: Array[Byte], rightBdd: Array[Byte]): Array[Byte]
}

object Test {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("Test")

    val test = new Test

    val bdd1 = test.createBdd("(x=1&y=1)|(z=5)")
    val bdd2 = test.createBdd("(x=2&z=1&y=3)")

    val expr = test.bdd2string(bdd1)
    println(s"The test bdd1 expr: $expr")
    val notExpr = test.bdd2string(test.bddOperator("!", bdd1, null))
    println(s"The test !bdd1 expr: $notExpr")

    val expr2 = test.bdd2string(bdd2)
    println(s"The test bdd2 expr: $expr2")
    val notExpr2 = test.bdd2string(test.bddOperator("!", bdd2, null))
    println(s"The test !bdd2 expr: $notExpr2")

    val bddAnd = test.bddOperator("&", bdd1, bdd2)
    val exprAnd = test.bdd2string(bddAnd)
    println(s"The test bdds after & expr: $exprAnd")

    val bddOr = test.bddOperator("|", bdd1, bdd2)
    val exprOr = test.bdd2string(bddOr)
    println(s"The test bdds after | expr: $exprOr")
  }
}
