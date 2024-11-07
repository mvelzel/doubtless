package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object BDDAggOrUDAF extends Aggregator[BDD, BDD, BDD] {
  def zero: BDD = BDD.False

  def reduce(b: BDD, a: BDD): BDD = a | b

  def merge(b1: BDD, b2: BDD): BDD = b1 | b2

  def finish(reduction: BDD): BDD = reduction

  def bufferEncoder: Encoder[BDD] = ExpressionEncoder()

  def outputEncoder: Encoder[BDD] = ExpressionEncoder()
}
