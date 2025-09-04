package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.{ConfigFactory}
import java.io.File

object ProbCountUDAF
    extends Aggregator[(BDD, String), List[(BDD, String)], List[BDD]] {
  def zero: List[(BDD, String)] = {
    List[(BDD, String)]((BDD.True, null))
  }

  override def reduce(
      agg: List[(BDD, String)],
      a: (BDD, String)
  ): List[(BDD, String)] = {
    val pruneMethod = a._2
    val inputBdd = a._1

    val newZero =
      if (pruneMethod == "each-operation") agg(0)._1 &! ~inputBdd
      else agg(0)._1 & ~inputBdd

    val res =
      newZero :: agg
        .zipAll(agg.drop(1), (null, pruneMethod), (null, pruneMethod))
        .map({
          case ((null, _), (null, _)) => null
          case ((null, _), (nextBdd, _)) =>
            if (pruneMethod == "each-operation") nextBdd &! ~inputBdd
            else nextBdd & ~inputBdd
          case ((curBdd, _), (null, _)) =>
            if (pruneMethod == "each-operation") curBdd &! inputBdd
            else curBdd & inputBdd
          case ((curBdd, _), (nextBdd, _)) =>
            if (pruneMethod == "each-operation")
              (curBdd &! inputBdd) | (nextBdd &! ~inputBdd)
            else (curBdd & inputBdd) | (nextBdd & ~inputBdd)
        })

    if (pruneMethod == "each-operation")
      res.map(bdd =>
        if (bdd == null || bdd.strictEquals(BDD.False)) (null, pruneMethod)
        else (bdd, pruneMethod)
      )
    else if (pruneMethod == "each-step")
      res.map((bdd) =>
        if (bdd == null || bdd.equals(BDD.False)) (null, pruneMethod)
        else (bdd, pruneMethod)
      )
    else
      res.map(bdd => (bdd, pruneMethod))
  }

  override def merge(
      agg: List[(BDD, String)],
      otherAgg: List[(BDD, String)]
  ): List[(BDD, String)] = {
    val pruneMethod = if (agg(0)._2 != null) agg(0)._2 else otherAgg(0)._2

    val totalMax = agg.length - 1 + otherAgg.length - 1

    val res = (0 to totalMax)
      .map((count) =>
        (0 to count)
          .map(i =>
            otherAgg lift (i) match {
              case Some((null, _)) => BDD.False
              case Some((otherBdd, _)) => {
                agg lift (count - i) match {
                  case Some((null, _)) => BDD.False
                  case Some((bdd, _)) =>
                    if (pruneMethod == "each-operation") bdd &! otherBdd
                    else bdd & otherBdd
                  case None => BDD.False
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
          if (bdd == null || bdd.strictEquals(BDD.False)) (null, pruneMethod)
          else (bdd, pruneMethod)
        )
        .toList
    else if (pruneMethod == "each-step")
      res
        .map((bdd) =>
          if (bdd == null || bdd.equals(BDD.False)) (null, pruneMethod)
          else (bdd, pruneMethod)
        )
        .toList
    else
      res.map(bdd => (bdd, pruneMethod)).toList
  }

  override def finish(reduction: List[(BDD, String)]): List[BDD] = {
    val pruneMethod = reduction(0)._2

    if (pruneMethod == "on-finish")
      reduction.map((tup) => if (tup._1.equals(BDD.False)) null else tup._1)
    else
      reduction.map(tup => tup._1)
  }

  def bufferEncoder: Encoder[List[(BDD, String)]] = ExpressionEncoder()

  def outputEncoder: Encoder[List[BDD]] = ExpressionEncoder()
}
