package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.{ConfigFactory}

object ProbCountUDAF extends Aggregator[BDD, List[BDD], List[BDD]] {
  val config =
    ConfigFactory.load().getConfig("com.doubtless.spark.aggregations")

  val pruneMethod = config.getString("prune-method")

  def zero: List[BDD] = List[BDD](BDD.True)

  override def reduce(agg: List[BDD], inputBdd: BDD): List[BDD] = {
    val res =
      (agg(0) & ~inputBdd) :: agg
        .zipAll(agg.drop(1), null, null)
        .map({
          case (null, null)      => null
          case (null, nextBdd)   => nextBdd & ~inputBdd
          case (curBdd, null)    => curBdd & inputBdd
          case (curBdd, nextBdd) => (curBdd & inputBdd) | (nextBdd & ~inputBdd)
        })

    if (pruneMethod == "each-operation")
      res.map((bdd) =>
        if (bdd == null || bdd.strictEquals(BDD.False)) null else bdd
      )
    else if (pruneMethod == "each-step")
      res.map((bdd) => if (bdd == null || bdd.equals(BDD.False)) null else bdd)
    else
      res
  }

  override def merge(
      agg: List[BDD],
      otherAgg: List[BDD]
  ): List[BDD] = {
    val totalMax = agg.length - 1 + otherAgg.length - 1

    val res = (0 to totalMax)
      .map((count) =>
        (0 to count)
          .map(i =>
            otherAgg lift (i) match {
              case Some(null) => BDD.False
              case Some(otherBdd) => {
                agg lift (count - i) match {
                  case Some(null) => BDD.False
                  case Some(bdd)  => bdd & otherBdd
                  case None       => BDD.False
                }
              }
              case None => BDD.False
            }
          )
          .reduce((bdd1: BDD, bdd2: BDD) => bdd1 | bdd2)
      )

    if (pruneMethod == "each-operation")
      res
        .map((bdd) =>
          if (bdd == null || bdd.strictEquals(BDD.False)) null else bdd
        )
        .toList
    else if (pruneMethod == "each-step")
      res
        .map((bdd) => if (bdd == null || bdd.equals(BDD.False)) null else bdd)
        .toList
    else
      res.toList
  }

  override def finish(reduction: List[BDD]): List[BDD] = {
    if (pruneMethod == "on-finish")
      reduction.map((bdd) => if (bdd.equals(BDD.False)) null else bdd)
    else
      reduction
  }

  def bufferEncoder: Encoder[List[BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[List[BDD]] = ExpressionEncoder()
}
