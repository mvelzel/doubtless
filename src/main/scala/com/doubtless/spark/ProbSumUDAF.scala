package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbSumUDAF
    extends Aggregator[(Int, BDD), Map[Int, BDD], Map[Int, BDD]] {
  def zero: Map[Int, BDD] = Map[Int, BDD]((0 -> BDD.True))

  // TODO maybe add the (& ~a._2) clause to the getOrElse part
  override def reduce(b: Map[Int, BDD], a: (Int, BDD)): Map[Int, BDD] = b.map({
    case (sum, bdd) => (sum -> (bdd & ~a._2))
  }) ++ b.map({ case (sum, bdd) =>
    ((sum + a._1) -> ((bdd & a._2) | ((b getOrElse (sum + a._1, BDD.False)) & ~a._2)))
  })

  override def merge(b1: Map[Int, BDD], b2: Map[Int, BDD]): Map[Int, BDD] =
    b1.toSeq
      .flatMap(tup1 => b2.toSeq.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[Int, BDD]())((acc, tups) => {
        acc + ((tups._1._1 + tups._2._1) -> ((tups._1._2 & tups._2._2) | (acc getOrElse (tups._1._1 + tups._2._1, BDD.False))))
      })

  override def finish(reduction: Map[Int, BDD]): Map[Int, BDD] =
    reduction.filter({ case (_, bdd) => bdd != BDD.False })

  def bufferEncoder: Encoder[Map[Int, BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[Map[Int, BDD]] = ExpressionEncoder()
}
