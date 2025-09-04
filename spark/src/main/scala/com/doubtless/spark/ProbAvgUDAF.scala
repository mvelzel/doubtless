package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbAvgUDAF
    extends Aggregator[
      (Double, BDD, String),
      List[(Int, Double, BDD, String)],
      List[(Option[Double], BDD)]
    ] {

  def zero: List[(Int, Double, BDD, String)] =
    List[(Int, Double, BDD, String)]((0, 0.0, BDD.True, null))

  override def reduce(
      b: List[(Int, Double, BDD, String)],
      a: (Double, BDD, String)
  ): List[(Int, Double, BDD, String)] = {
    val pruneMethod = a._3

    val newSums = b.map({ case (count, sum, bdd, _) =>
      (
        count + 1,
        sum + a._1,
        if (pruneMethod == "each-operation") bdd &! a._2 else bdd & a._2,
        pruneMethod
      )
    })
    val oldSums = b.map({
      case (count, sum, bdd, _) => {
        newSums find (tup => tup._1 == count && tup._2 == sum) match {
          case Some(newSum) =>
            (
              count,
              sum,
              if (pruneMethod == "each-operation") newSum._3 | (bdd &! ~a._2)
              else newSum._3 | (bdd & ~a._2),
              pruneMethod
            )
          case None =>
            (
              count,
              sum,
              if (pruneMethod == "each-operation") bdd &! ~a._2
              else bdd & ~a._2,
              pruneMethod
            )
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
      b1: List[(Int, Double, BDD, String)],
      b2: List[(Int, Double, BDD, String)]
  ): List[(Int, Double, BDD, String)] = {
    val pruneMethod = if (b1(0)._4 != null) b1(0)._4 else b2(0)._4

    val res = b1
      .flatMap(tup1 => b2.map(tup2 => (tup1, tup2)))
      .foldLeft(Map[(Int, Double), BDD]())((acc, tups) =>
        acc get ((tups._1._1 + tups._2._1, tups._1._2 + tups._2._2)) match {
          case Some(accBdd) =>
            acc + ((
              tups._1._1 + tups._2._1,
              tups._1._2 + tups._2._2
            ) -> (if (pruneMethod == "each-operation")
                    ((tups._1._3 &! tups._2._3) | accBdd)
                  else ((tups._1._3 & tups._2._3) | accBdd)))
          case None =>
            acc + ((
              tups._1._1 + tups._2._1,
              tups._1._2 + tups._2._2
            ) -> (if (pruneMethod == "each-operation")
                    (tups._1._3 &! tups._2._3)
                  else (tups._1._3 & tups._2._3)))
        }
      )
      .map({ case ((count, sum), bdd) => (count, sum, bdd, pruneMethod) })

    if (pruneMethod == "each-operation")
      res.filter(tup => !tup._3.strictEquals(BDD.False)).toList
    else if (pruneMethod == "each-step")
      res.filter(tup => !tup._3.equals(BDD.False)).toList
    else
      res.toList
  }

  override def finish(
      reduction: List[(Int, Double, BDD, String)]
  ): List[(Option[Double], BDD)] = {
    val pruneMethod = reduction(0)._4

    val newReduction =
      if (pruneMethod == "on-finish")
        reduction.filter({ case (_, _, bdd, _) => !bdd.equals(BDD.False) })
      else
        reduction

    newReduction
      .map({
        case (0, _, bdd, _)       => (None, bdd)
        case (count, sum, bdd, _) => (Some(sum / count), bdd)
      })
      .groupBy({ case (opt, _) =>
        opt
      })
      .map({ case (avgOpt, group) =>
        (avgOpt, group.foldLeft(BDD.False)((acc, tup) => acc | tup._2))
      })
      .toList
  }

  def bufferEncoder: Encoder[List[(Int, Double, BDD, String)]] =
    ExpressionEncoder()

  def outputEncoder: Encoder[List[(Option[Double], BDD)]] = ExpressionEncoder()
}
