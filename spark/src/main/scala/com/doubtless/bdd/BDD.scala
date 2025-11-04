package com.doubtless.bdd

import com.doubtless.spark.BDDUDT
import org.apache.spark.sql.types.SQLUserDefinedType
import java.io.File

@SQLUserDefinedType(udt = classOf[BDDUDT])
class BDD(val buffer: Array[Byte]) extends Serializable {
  def |(that: BDD) = {
    new BDD(Native.bddOperator("|", buffer, that.buffer))
  }

  def &(that: BDD) = new BDD(Native.bddOperator("&", buffer, that.buffer))

  def &!(that: BDD) = {
    val newBdd = this & that
    if (newBdd.equals(BDD.False))
      BDD.False
    else
      newBdd
  }

  def unary_~ = new BDD(Native.bddOperator("!", buffer, null))

  def probability(dict: ProbDict) = Native.bddProb(dict.buffer, buffer)

  def probabilityFromBuffer(dict: Array[Byte]) = Native.bddProb(dict, buffer)

  def toExpr(): String = Native.bdd2string(buffer)

  def toDot(): String = Native.bddGenerateDot(buffer)

  def isFalse(): Boolean = Native.bddPropertyCheck(buffer, 0, null)

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
