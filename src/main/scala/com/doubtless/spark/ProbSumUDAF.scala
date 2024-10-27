package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbSumUDAF
    extends Aggregator[(Double, BDD), Map[Double, BDD], Map[Double, BDD]] {
  def zero: Map[Double, BDD] = Map[Double, BDD]((0.0 -> BDD.True))

  // TODO maybe add the (& ~a._2) clause to the getOrElse part
  override def reduce(b: Map[Double, BDD], a: (Double, BDD)): Map[Double, BDD] = b.map({
    case (sum, bdd) => (sum -> (bdd & ~a._2))
  }) ++ b.map({ case (sum, bdd) =>
    ((sum + a._1) -> ((bdd & a._2) | ((b getOrElse (sum + a._1, BDD.False)) & ~a._2)))
  })

  override def merge(b1: Map[Double, BDD], b2: Map[Double, BDD]): Map[Double, BDD] =
    b1.toSeq
      .flatMap(tup1 => b2.toSeq.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[Double, BDD]())((acc, tups) => {
        acc + ((tups._1._1 + tups._2._1) -> ((tups._1._2 & tups._2._2) | (acc getOrElse (tups._1._1 + tups._2._1, BDD.False))))
      })

  override def finish(reduction: Map[Double, BDD]): Map[Double, BDD] =
    reduction.filter({ case (_, bdd) => bdd != BDD.False })

  def bufferEncoder: Encoder[Map[Double, BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[Map[Double, BDD]] = ExpressionEncoder()
}
