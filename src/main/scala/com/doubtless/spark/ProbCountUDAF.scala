package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbCountUDAF extends Aggregator[BDD, Map[Int, BDD], Map[Int, BDD]] {
  def zero: Map[Int, BDD] = Map[Int, BDD]((0 -> BDD.True))

  override def reduce(b: Map[Int, BDD], a: BDD): Map[Int, BDD] =
    (b.map({ case (count, bdd) =>
      ((count + 1) -> ((bdd & a) | ((b getOrElse (count + 1, BDD.False)) & ~a)))
    }) + (0 -> (b(0) & (~a))))

  override def merge(
      b1: Map[Int, BDD],
      b2: Map[Int, BDD]
  ): Map[Int, BDD] = {
    val b1Max = b1.keys.max
    val b2Max = b2.keys.max
    val totalMax = b1Max + b2Max

    Map(0 to totalMax map { count =>
      (count ->
        (0 to count)
          .map(i =>
            (b1 getOrElse (count - i, BDD.False)) & (b2 getOrElse (i, BDD.False))
          )
          .reduce((bdd1: BDD, bdd2: BDD) => bdd1 | bdd2))
    }: _*)
  }

  override def finish(reduction: Map[Int, BDD]): Map[Int, BDD] =
    reduction.filter({ case (_, bdd) => bdd != BDD.False })

  def bufferEncoder: Encoder[Map[Int, BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[Map[Int, BDD]] = ExpressionEncoder()
}
