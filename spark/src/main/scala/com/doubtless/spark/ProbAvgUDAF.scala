package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.ConfigFactory

object ProbAvgUDAF
    extends Aggregator[
      (Double, BDD),
      List[(Int, Double, BDD)],
      List[(Option[Double], BDD)]
    ] {
  val config =
    ConfigFactory.load().getConfig("com.doubtless.spark.aggregations")

  val pruneMethod = config.getString("prune-method")

  def zero: List[(Int, Double, BDD)] =
    List[(Int, Double, BDD)]((0, 0.0, BDD.True))

  override def reduce(
      b: List[(Int, Double, BDD)],
      a: (Double, BDD)
  ): List[(Int, Double, BDD)] = {
    val newSums = b.map({ case (count, sum, bdd) =>
      (count + 1, sum + a._1, bdd & a._2)
    })
    val oldSums = b.map({
      case (count, sum, bdd) => {
        newSums find (tup => tup._1 == count && tup._2 == sum) match {
          case Some(newSum) => (count, sum, newSum._3 | (bdd & ~a._2))
          case None         => (count, sum, bdd & ~a._2)
        }
      }
    })

    val res = oldSums ++ newSums.filter(newS =>
      !oldSums.exists(oldS => oldS._1 == newS._1 && oldS._2 == newS._2)
    )

    if (pruneMethod == "each-operation")
      res.filter(tup => !tup._3.strictEquals(BDD.False))
    else if (pruneMethod == "each-step")
      res.filter(tup => !tup._3.equals(BDD.False))
    else
      res
  }

  override def merge(
      b1: List[(Int, Double, BDD)],
      b2: List[(Int, Double, BDD)]
  ): List[(Int, Double, BDD)] = {
    val res = b1
      .flatMap(tup1 => b2.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[(Int, Double), BDD]())((acc, tups) =>
        acc get ((tups._1._1 + tups._2._1, tups._1._2 + tups._2._2)) match {
          case Some(accBdd) =>
            acc + ((
              tups._1._1 + tups._2._1,
              tups._1._2 + tups._2._2
            ) -> ((tups._1._3 & tups._2._3) | accBdd))
          case None =>
            acc + ((
              tups._1._1 + tups._2._1,
              tups._1._2 + tups._2._2
            ) -> (tups._1._3 & tups._2._3))
        }
      )
      .map({ case ((count, sum), bdd) => (count, sum, bdd) })

    if (pruneMethod == "each-operation")
      res.filter(tup => !tup._3.strictEquals(BDD.False)).toList
    else if (pruneMethod == "each-step")
      res.filter(tup => !tup._3.equals(BDD.False)).toList
    else
      res.toList
  }

  override def finish(
      reduction: List[(Int, Double, BDD)]
  ): List[(Option[Double], BDD)] = {
    val newReduction =
      if (pruneMethod == "on-finish")
        reduction.filter({ case (_, _, bdd) => !bdd.equals(BDD.False) })
      else
        reduction

    newReduction
      .map({
        case (0, _, bdd)       => (None, bdd)
        case (count, sum, bdd) => (Some(sum / count), bdd)
      })
      .groupBy({ case (opt, _) =>
        opt
      })
      .map({ case (avgOpt, group) =>
        (avgOpt, group.foldLeft(BDD.False)((acc, tup) => acc | tup._2))
      })
      .toList
  }

  def bufferEncoder: Encoder[List[(Int, Double, BDD)]] = ExpressionEncoder()

  def outputEncoder: Encoder[List[(Option[Double], BDD)]] = ExpressionEncoder()
}
