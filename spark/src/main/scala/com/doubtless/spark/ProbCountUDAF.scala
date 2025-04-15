package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.{ConfigFactory}

object ProbCountUDAF extends Aggregator[BDD, Map[Int, BDD], Map[Int, BDD]] {
  val config = ConfigFactory.load().getConfig("com.doubtless.spark.prob-count")

  val filterOnFinish = config.getBoolean("filter-on-finish")

  def zero: Map[Int, BDD] = Map[Int, BDD]((0 -> BDD.True))

  override def reduce(agg: Map[Int, BDD], inputBdd: BDD): Map[Int, BDD] =
    agg.map({ case (count, bdd) =>
      agg get (count + 1) match {
        case Some(accBdd) =>
          (count + 1) -> ((bdd & inputBdd) | (accBdd & ~inputBdd))
        case None => (count + 1) -> (bdd & inputBdd)
      }
    }) + (0 -> (agg(0) & ~inputBdd))

  override def merge(
      agg: Map[Int, BDD],
      otherAgg: Map[Int, BDD]
  ): Map[Int, BDD] = {
    val totalMax = agg.keys.max + otherAgg.keys.max

    Map(0 to totalMax map { count =>
      (count ->
        (0 to count)
          .map(i =>
            otherAgg get (i) match {
              case Some(otherBdd) => {
                agg get (count - i) match {
                  case Some(bdd) => bdd & otherBdd
                  case None      => BDD.False
                }
              }
              case None => BDD.False
            }
          )
          .reduce((bdd1: BDD, bdd2: BDD) => bdd1 | bdd2))
    }: _*)
  }

  override def finish(reduction: Map[Int, BDD]): Map[Int, BDD] = {
    if (filterOnFinish) {
      reduction.filter({ case (_, bdd) => bdd != BDD.False })
    } else {
      reduction
    }
  }

  def bufferEncoder: Encoder[Map[Int, BDD]] = ExpressionEncoder()

  def outputEncoder: Encoder[Map[Int, BDD]] = ExpressionEncoder()
}
