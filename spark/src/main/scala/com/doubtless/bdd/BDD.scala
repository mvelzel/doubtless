package com.doubtless.bdd

import com.doubtless.spark.BDDUDT
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[BDDUDT])
class BDD(val buffer: Array[Byte]) extends Serializable {
  def |(that: BDD) = {
    val bdd = new BDD(Native.bddOperator("|", buffer, that.buffer))
    if (bdd == BDD.False)
      BDD.False
    else
      bdd
  }

  def &(that: BDD) = {
    val bdd = new BDD(Native.bddOperator("&", buffer, that.buffer))
    if (bdd == BDD.False)
      BDD.False
    else
      bdd
  }

  def unary_~ = new BDD(Native.bddOperator("!", buffer, null))

  def probability(dict: ProbDict) = Native.bddProb(dict.buffer, buffer)

  def toExpr(): String = Native.bdd2string(buffer)

  def toDot(): String = Native.bddGenerateDot(buffer)

  override def toString(): String = s"BDD(${this.toExpr()})"

  override def equals(bdd: Any) = bdd match {
    case b: BDD =>
      // This ugly workaround is needed because BDDs with mismatching variables break the equivalence check.
      // Example: x=2 and x=2&!x=1 without the workaround returns false.
      // TODO Make a GitHub issue for this
      if (!Native.bddEquiv(this.buffer, b.buffer)) {
        val newLeft = BDD(s"(${this.toExpr()})|(0&(${b.toExpr}))")
        val newRight = BDD(s"(${b.toExpr()})|(0&(${this.toExpr}))")
        Native.bddEquiv(newLeft.buffer, newRight.buffer)
      } else {
        true
      }
    case _ => false
  }

  def strictEquals(bdd: BDD) = Native.bddEqual(this.buffer, bdd.buffer)
}

object BDD {
  def apply(expr: String) = new BDD(Native.createBdd(expr))

  val False = BDD("0")
  val True = BDD("1")
}
