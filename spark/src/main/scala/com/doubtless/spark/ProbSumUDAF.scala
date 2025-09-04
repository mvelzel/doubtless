package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbSumUDAF
    extends Aggregator[
      (Double, BDD, String),
      List[(Double, BDD, String)],
      List[(Double, BDD)]
    ] {

  def zero: List[(Double, BDD, String)] =
    List[(Double, BDD, String)]((0.0, BDD.True, null))

  override def reduce(
      b: List[(Double, BDD, String)],
      a: (Double, BDD, String)
  ): List[(Double, BDD, String)] = {
    val pruneMethod = a._3

    if (a._1 == 0.0)
      b.map(tup => (tup._1, tup._2, pruneMethod))
    else {
      val newSums = b.map({ case (sum, bdd, _) =>
        if (pruneMethod == "each-operation")
          (sum + a._1, bdd &! a._2, pruneMethod)
        else (sum + a._1, bdd & a._2, pruneMethod)
      })
      val oldSums = b.map({
        case (sum, bdd, _) => {
          newSums find (tup => tup._1 == sum) match {
            case Some(newSum) =>
              if (pruneMethod == "each-operation")
                (sum, newSum._2 | (bdd &! ~a._2), pruneMethod)
              else (sum, newSum._2 | (bdd & ~a._2), pruneMethod)
            case None =>
              if (pruneMethod == "each-operation")
                (sum, bdd &! ~a._2, pruneMethod)
              else (sum, bdd & ~a._2, pruneMethod)
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
      b1: List[(Double, BDD, String)],
      b2: List[(Double, BDD, String)]
  ): List[(Double, BDD, String)] = {
    val pruneMethod = if (b1(0)._3 != null) b1(0)._3 else b2(0)._3

    val res = b1
      .flatMap(tup1 => b2.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[Double, BDD]())((acc, tups) =>
        acc get (tups._1._1 + tups._2._1) match {
          case Some(accBdd) =>
            acc + ((tups._1._1 + tups._2._1) -> (if (
                                                   pruneMethod == "each-operation"
                                                 ) ((tups._1._2 &! tups._2._2) | accBdd)
                                                 else
                                                   ((tups._1._2 & tups._2._2) | accBdd)))
          case None =>
            acc + ((tups._1._1 + tups._2._1) -> (if (
                                                   pruneMethod == "each-operation"
                                                 ) (tups._1._2 &! tups._2._2)
                                                 else
                                                   (tups._1._2 & tups._2._2)))
        }
      )

    if (pruneMethod == "each-operation")
      res
        .filter(tup => !tup._2.strictEquals(BDD.False))
        .map(tup => (tup._1, tup._2, pruneMethod))
        .toList
    else if (pruneMethod == "each-step")
      res
        .filter(tup => !tup._2.equals(BDD.False))
        .map(tup => (tup._1, tup._2, pruneMethod))
        .toList
    else
      res.map(tup => (tup._1, tup._2, pruneMethod)).toList
  }

  override def finish(
      reduction: List[(Double, BDD, String)]
  ): List[(Double, BDD)] = {
    val pruneMethod = reduction(0)._3

    if (pruneMethod == "on-finish")
      reduction
        .filter(tup => !tup._2.equals(BDD.False))
        .map(tup => (tup._1, tup._2))
    else
      reduction.map(tup => (tup._1, tup._2))
  }

  def bufferEncoder: Encoder[List[(Double, BDD, String)]] = ExpressionEncoder()

  def outputEncoder: Encoder[List[(Double, BDD)]] = ExpressionEncoder()
}
