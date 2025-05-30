package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.{ConfigFactory}

object ProbCountUDAF extends Aggregator[BDD, List[BDD], List[BDD]] {
  val config = ConfigFactory.load().getConfig("com.doubtless.spark.prob-count")

  val filterOnFinish = config.getBoolean("filter-on-finish")

  def zero: List[BDD] = List[BDD](BDD.True)

  override def reduce(agg: List[BDD], inputBdd: BDD): List[BDD] =
    (agg(0) & ~inputBdd) :: agg
      .zipAll(agg.drop(1), null, null)
      .map({ case (curBdd, nextBdd) =>
        if (nextBdd == null)
          curBdd & inputBdd
        else
          (curBdd & inputBdd) | (nextBdd & ~inputBdd)
      })

  override def merge(
      agg: List[BDD],
      otherAgg: List[BDD]
  ): List[BDD] = {
    val totalMax = agg.length - 1 + otherAgg.length - 1

    (0 to totalMax)
      .map((count) =>
        (0 to count)
          .map(i =>
            otherAgg lift (i) match {
              case Some(otherBdd) => {
                agg lift (count - i) match {
                  case Some(bdd) => bdd & otherBdd
                  case None      => BDD.False
                }
              }
              case None => BDD.False
            }
          )
          .reduce((bdd1: BDD, bdd2: BDD) => bdd1 | bdd2)
      )
      .toList
  }

  override def finish(reduction: List[BDD]): List[BDD] = {
    if (filterOnFinish) {
      reduction.map((bdd) => if (bdd == BDD.False) null else bdd)
    } else {
      reduction
    }
  }

  def bufferEncoder: Encoder[List[BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[List[BDD]] = ExpressionEncoder()
}
