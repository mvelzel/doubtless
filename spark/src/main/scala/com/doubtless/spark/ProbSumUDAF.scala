package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.ConfigFactory

object ProbSumUDAF
    extends Aggregator[
      (Double, BDD),
      List[(Double, BDD)],
      List[(Double, BDD)]
    ] {
  val config = ConfigFactory.load().getConfig("com.doubtless.spark.aggregations")

  val pruneMethod = config.getString("prune-method")

  def zero: List[(Double, BDD)] = List[(Double, BDD)]((0.0 -> BDD.True))

  override def reduce(
      b: List[(Double, BDD)],
      a: (Double, BDD)
  ): List[(Double, BDD)] = {
    if (a._1 == 0.0)
      b
    else {
      val newSums = b.map({ case (sum, bdd) =>
        (sum + a._1) -> (bdd & a._2)
      })
      val oldSums = b.map({
        case (sum, bdd) => {
          newSums find (tup => tup._1 == sum) match {
            case Some(newSum) => ((sum) -> (newSum._2 | (bdd & ~a._2)))
            case None         => ((sum) -> (bdd & ~a._2))
          }
        }
      })

      val res = oldSums ++ newSums.filter(newS =>
        !oldSums.exists(oldS => oldS._1 == newS._1)
      )

      if (pruneMethod == "each-operation")
        res.filter(tup => !tup._2.strictEquals(BDD.False))
      else if (pruneMethod == "each-step")
        res.filter(tup => !tup._2.equals(BDD.False))
      else
        res
    }
  }

  override def merge(
      b1: List[(Double, BDD)],
      b2: List[(Double, BDD)]
  ): List[(Double, BDD)] = {
    val res = b1
      .flatMap(tup1 => b2.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[Double, BDD]())((acc, tups) =>
        acc get (tups._1._1 + tups._2._1) match {
          case Some(accBdd) =>
            acc + ((tups._1._1 + tups._2._1) -> ((tups._1._2 & tups._2._2) | accBdd))
          case None =>
            acc + ((tups._1._1 + tups._2._1) -> (tups._1._2 & tups._2._2))
        }
      )

    if (pruneMethod == "each-operation")
      res.filter(tup => !tup._2.strictEquals(BDD.False)).toList
    else if (pruneMethod == "each-step")
      res.filter(tup => !tup._2.equals(BDD.False)).toList
    else
      res.toList
  }

  override def finish(reduction: List[(Double, BDD)]): List[(Double, BDD)] = {
    if (pruneMethod == "on-finish")
      reduction.filter({ case (_, bdd) => !bdd.equals(BDD.False) })
    else
      reduction
  }

  def bufferEncoder: Encoder[List[(Double, BDD)]] = ExpressionEncoder()

  def outputEncoder: Encoder[List[(Double, BDD)]] = ExpressionEncoder()
}
