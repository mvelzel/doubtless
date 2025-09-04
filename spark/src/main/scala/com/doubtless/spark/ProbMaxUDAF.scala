package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object ProbMaxUDAF
    extends Aggregator[(Option[Double], BDD, String), List[
      (Option[Double], BDD, String)
    ], List[(Option[Double], BDD)]] {

  def zero: List[(Option[Double], BDD, String)] =
    List[(Option[Double], BDD, String)]((None, BDD.True, null))

  override def reduce(
      b: List[(Option[Double], BDD, String)],
      a: (Option[Double], BDD, String)
  ): List[(Option[Double], BDD, String)] = {
    val pruneMethod = a._3

    val newBdd = b.foldLeft(a._2)((nBdd, tup) =>
      if (tup._1.getOrElse(Double.NaN) > a._1.getOrElse(Double.NaN))
        if (pruneMethod == "each-operation") nBdd &! ~tup._2 else nBdd & ~tup._2
      else
        nBdd
    )

    var newBddIncluded = false
    val newMap = b.map({ case (max, bdd, _) =>
      if (a._1.getOrElse(Double.NaN) > max.getOrElse(Double.NaN) || max.isEmpty)
        (
          max,
          if (pruneMethod == "each-operation") bdd &! ~a._2 else bdd & ~a._2,
          pruneMethod
        )
      else if (a._1 == max) {
        newBddIncluded = true

        (max, bdd | newBdd, pruneMethod)
      } else
        (max, bdd, pruneMethod)
    })

    val res =
      if (!newBddIncluded) (a._1, newBdd, pruneMethod) :: newMap
      else
        newMap

    if (pruneMethod == "each-operation")
      res.filter(tup => !tup._2.strictEquals(BDD.False))
    else if (pruneMethod == "each-step")
      res.filter(tup => !tup._2.equals(BDD.False))
    else
      res
  }

  override def merge(
      b1: List[(Option[Double], BDD, String)],
      b2: List[(Option[Double], BDD, String)]
  ): List[(Option[Double], BDD, String)] = {
    val pruneMethod = if (b1(0)._3 != null) b1(0)._3 else b2(0)._3

    var b1Null: BDD = null
    var b2Null: BDD = null
    val (leftMap, rightMap) = b1
      .flatMap(tup1 => b2.map(tup2 => (tup1, tup2)))
      .foldLeft((Map[Option[Double], BDD](), Map[Option[Double], BDD]()))(
        (acc, tups) => {
          if (tups._1._1.isEmpty)
            b1Null = tups._1._2
          if (tups._2._1.isEmpty)
            b2Null = tups._2._2
          (
            tups._1._1 match {
              case None => acc._1
              case Some(tups11) =>
                acc._1 + ((tups._1._1) -> (
                  if (
                    tups11 < tups._2._1
                      .getOrElse(Double.NaN)
                  )
                    if (pruneMethod == "each-operation")
                      acc._1.getOrElse(tups._1._1, tups._1._2) &! ~tups._2._2
                    else acc._1.getOrElse(tups._1._1, tups._1._2) & ~tups._2._2
                  else
                    acc._1.getOrElse(tups._1._1, tups._1._2)
                ))
            },
            tups._2._1 match {
              case None => acc._2
              case Some(tups21) =>
                acc._2 + ((tups._2._1) -> (
                  if (
                    tups21 < tups._1._1
                      .getOrElse(Double.NaN)
                  )
                    if (pruneMethod == "each-operation")
                      acc._2.getOrElse(tups._2._1, tups._2._2) &! ~tups._1._2
                    else acc._2.getOrElse(tups._2._1, tups._2._2) & ~tups._1._2
                  else
                    acc._2.getOrElse(tups._2._1, tups._2._2)
                ))
            }
          )
        }
      )

    val res = (leftMap.keySet ++ rightMap.keySet)
      .map(key =>
        leftMap get (key) match {
          case Some(leftBdd) =>
            rightMap get (key) match {
              case Some(rightBdd) => ((key) -> (leftBdd | rightBdd))
              case None           => ((key) -> leftBdd)
            }
          case None =>
            ((key) -> rightMap(key))
        }
      )

    val combinedNulls =
      if (pruneMethod == "each-operation") b1Null &! b2Null else b1Null & b2Null

    if (pruneMethod == "each-operation")
      (None, combinedNulls, pruneMethod) :: res
        .filter(tup => !tup._2.strictEquals(BDD.False))
        .map(tup => (tup._1, tup._2, pruneMethod))
        .toList
    else if (pruneMethod == "each-step")
      (None, combinedNulls, pruneMethod) :: res
        .filter(tup => !tup._2.equals(BDD.False))
        .map(tup => (tup._1, tup._2, pruneMethod))
        .toList
    else
      (None, combinedNulls, pruneMethod) :: res
        .map(tup => (tup._1, tup._2, pruneMethod))
        .toList
  }

  override def finish(
      reduction: List[(Option[Double], BDD, String)]
  ): List[(Option[Double], BDD)] = {
    val pruneMethod = reduction(0)._3

    if (pruneMethod == "on-finish")
      reduction
        .filter({ case (_, bdd, _) => !bdd.equals(BDD.False) })
        .map(tup => (tup._1, tup._2))
    else
      reduction
        .map(tup => (tup._1, tup._2))
  }

  def bufferEncoder: Encoder[List[(Option[Double], BDD, String)]] =
    ExpressionEncoder()

  def outputEncoder: Encoder[List[(Option[Double], BDD)]] =
    ExpressionEncoder()
}
