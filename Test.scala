class Test {
  @native def createBdd(expr: String): Array[Byte]
  @native def bdd2string(bdd: Array[Byte]): String
  @native def bddOperator(operator: String, leftBdd: Array[Byte], rightBdd: Array[Byte]): Array[Byte]
  @native def bddProb(dict: Array[Byte], bdd: Array[Byte]): Double

  @native def createDict(varDefs: String): Array[Byte]
  @native def dict2string(dict: Array[Byte]): String
}

object Test {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("Test")

    val test = new Test

    val dictString = "x=1 : 0.6; x=2 : 0.4; y=1 : 0.7; y=2 : 0.3; z=1 : 0.1; z=2 : 0.9; "
    val dict = test.createDict(dictString)

    val dictFormatted = test.dict2string(dict)
    println(s"The probability dictionary: $dictFormatted\n")

    val bdd1 = test.createBdd("(x=1&y=1)|(z=2)")
    println(s"The test bdd1 expr: ${test.bdd2string(bdd1)}")
    println(s"The test bdd1 prob: ${test.bddProb(dict, bdd1)}\n")

    println(s"The test !bdd1 expr: ${test.bdd2string(test.bddOperator("!", bdd1, null))}")
    println(s"The test !bdd1 prob: ${test.bddProb(dict, test.bddOperator("!", bdd1, null))}")

    val bdd2 = test.createBdd("(x=2&z=1&y=2)")
    println(s"The test bdd2 expr: ${test.bdd2string(bdd2)}")
    println(s"The test bdd2 prob: ${test.bddProb(dict, bdd2)}")

    println(s"The test !bdd2 expr: ${test.bdd2string(test.bddOperator("!", bdd2, null))}")
    println(s"The test !bdd2 prob: ${test.bddProb(dict, test.bddOperator("!", bdd2, null))}")

    val bddAnd = test.bddOperator("&", bdd1, bdd2)
    println(s"The test bdds after & expr: ${test.bdd2string(bddAnd)}")
    println(s"The test bdds after & prob: ${test.bddProb(dict, bddAnd)}")

    val bddOr = test.bddOperator("|", bdd1, bdd2)
    println(s"The test bdds after | expr: ${test.bdd2string(bddOr)}")
    println(s"The test bdds after | prob: ${test.bddProb(dict, bddOr)}")
  }
}
