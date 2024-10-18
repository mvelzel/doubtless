package com.doubtless.bdd

class BDD private (private val buffer: Array[Byte]) {
  def this(expr: String) = this(Native.createBdd(expr))

  def |(that: BDD) = new BDD(Native.bddOperator("|", buffer.clone(), that.buffer.clone()))
  def &(that: BDD) = new BDD(Native.bddOperator("&", buffer.clone(), that.buffer.clone()))
  def unary_~ = new BDD(Native.bddOperator("!", buffer.clone(), null))

  def probability(dict: ProbDict) = Native.bddProb(dict.buffer, buffer)

  override def toString(): String = Native.bdd2string(buffer)

  override def equals(bdd: Any) = bdd match {
    case b: BDD => Native.bddEquiv(buffer, b.buffer)
    case _      => false
  }
}
