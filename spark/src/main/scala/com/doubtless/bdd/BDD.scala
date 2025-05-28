package com.doubtless.bdd

import com.doubtless.spark.BDDUDT
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[BDDUDT])
class BDD(val buffer: Array[Byte]) extends Serializable {
  def |(that: BDD) = {
    //if (this.strictEquals(BDD.False))
    //  that
    //else if (that.strictEquals(BDD.False))
    //  this
    //else
      new BDD(Native.bddOperator("|", buffer, that.buffer))
  }

  def &(that: BDD) = {
    //if (this.strictEquals(BDD.True))
    //  that
    //else if (that.strictEquals(BDD.True))
    //  this
    //else
      new BDD(Native.bddOperator("&", buffer, that.buffer))
  }

  def unary_~ = new BDD(Native.bddOperator("!", buffer, null))

  def probability(dict: ProbDict) = Native.bddProb(dict.buffer, buffer)

  def toExpr(): String = Native.bdd2string(buffer)

  def toDot(): String = Native.bddGenerateDot(buffer)

  override def toString(): String = s"BDD(${this.toExpr()})"

  override def equals(bdd: Any) = bdd match {
    case b: BDD => Native.bddEquiv(this.buffer, b.buffer)
    case _      => false
  }

  def strictEquals(bdd: BDD) = Native.bddEqual(this.buffer, bdd.buffer)
}

object BDD {
  def apply(expr: String) = new BDD(Native.createBdd(expr))

  val False = BDD("0")
  val True = BDD("1")
}
