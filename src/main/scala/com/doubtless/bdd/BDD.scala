package com.doubtless.bdd

import com.doubtless.spark.BDDUserDefinedType
import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[BDDUserDefinedType])
class BDD(val buffer: Array[Byte]) {
  def |(that: BDD) = new BDD(
    Native.bddOperator("|", buffer, that.buffer)
  )
  def &(that: BDD) = new BDD(
    Native.bddOperator("&", buffer, that.buffer)
  )
  def unary_~ = new BDD(Native.bddOperator("!", buffer, null))

  def probability(dict: ProbDict) = Native.bddProb(dict.buffer, buffer)

  override def toString(): String = Native.bdd2string(buffer)

  override def equals(bdd: Any) = bdd match {
    case b: BDD => Native.bddEquiv(buffer, b.buffer)
    case _      => false
  }
}

object BDD {
  def apply(expr: String) = new BDD(Native.createBdd(expr))
}
