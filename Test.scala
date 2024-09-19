class Test {
  @native def createBdd(expr: String): Array[Byte]
  @native def bdd2string(bdd: Array[Byte]): String
}

object Test {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("Test")

    val test = new Test
    val bdd = test.createBdd("(x=1&y=1)|(z=5)")

    println(s"The test bdd: ${bdd.mkString(" ")}")

    val expr = test.bdd2string(bdd)

    println(s"The test bdd expr: $expr")
  }
}
