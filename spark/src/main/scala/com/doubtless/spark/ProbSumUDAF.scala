package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbSumUDAF
    extends Aggregator[(Double, BDD), Map[Double, BDD], Map[Double, BDD]] {
  def zero: Map[Double, BDD] = Map[Double, BDD]((0.0 -> BDD.True))

  override def reduce(b: Map[Double, BDD], a: (Double, BDD)): Map[Double, BDD] =
    b.map({ case (sum, bdd) =>
      (sum -> (bdd & ~a._2))
    }) ++ b.map({ case (sum, bdd) =>
      b get (sum + a._1) match {
        case Some(accBdd) => (sum + a._1) -> ((bdd & a._2) | (accBdd & ~a._2))
        case None         => (sum + a._1) -> (bdd & a._2)
      }
    })

  override def merge(
      b1: Map[Double, BDD],
      b2: Map[Double, BDD]
  ): Map[Double, BDD] =
    b1.toSeq
      .flatMap(tup1 => b2.toSeq.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[Double, BDD]())((acc, tups) =>
        acc get (tups._1._1 + tups._2._1) match {
          case Some(accBdd) =>
            acc + ((tups._1._1 + tups._2._1) -> ((tups._1._2 & tups._2._2) | accBdd))
          case None =>
            acc + ((tups._1._1 + tups._2._1) -> (tups._1._2 & tups._2._2))
        }
      )

  override def finish(reduction: Map[Double, BDD]): Map[Double, BDD] =
    reduction.filter({ case (_, bdd) => bdd != BDD.False })

  def bufferEncoder: Encoder[Map[Double, BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[Map[Double, BDD]] = ExpressionEncoder()
}
