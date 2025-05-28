package com.doubtless.spark

import org.apache.spark.sql.expressions.Aggregator
import com.doubtless.bdd._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import com.typesafe.config.ConfigFactory

object ProbMaxUDAF
    extends Aggregator[(Option[Double], BDD), List[
      (Option[Double], BDD)
    ], List[(Option[Double], BDD)]] {
  val config = ConfigFactory.load().getConfig("com.doubtless.spark.prob-max")

  val filterOnFinish = config.getBoolean("filter-on-finish")

  def zero: List[(Option[Double], BDD)] =
    List[(Option[Double], BDD)]((None, BDD.True))

  override def reduce(
      b: List[(Option[Double], BDD)],
      a: (Option[Double], BDD)
  ): List[(Option[Double], BDD)] = {
    val newBdd = b.foldLeft(a._2)((nBdd, tup) =>
      if (tup._1.getOrElse(Double.NaN) > a._1.getOrElse(Double.NaN))
        nBdd & ~tup._2
      else
        nBdd
    )

    var newBddIncluded = false
    val newMap = b.map({ case (max, bdd) =>
      if (a._1.getOrElse(Double.NaN) > max.getOrElse(Double.NaN) || max.isEmpty)
        (max) -> (bdd & ~a._2)
      else if (a._1 == max) {
        newBddIncluded = true

        (max) -> (bdd | newBdd)
      } else
        (max) -> bdd
    })

    if (!newBddIncluded)
      ((a._1) -> newBdd) :: newMap
    else
      newMap
  }

  override def merge(
      b1: List[(Option[Double], BDD)],
      b2: List[(Option[Double], BDD)]
  ): List[(Option[Double], BDD)] = {
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
                    acc._1.getOrElse(tups._1._1, tups._1._2) & ~tups._2._2
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
                    acc._2.getOrElse(tups._2._1, tups._2._2) & ~tups._1._2
                  else
                    acc._2.getOrElse(tups._2._1, tups._2._2)
                ))
            }
          )
        }
      )

    (None, b1Null & b2Null) :: (leftMap.keySet ++ rightMap.keySet)
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
      .toList
  }

  override def finish(
      reduction: List[(Option[Double], BDD)]
  ): List[(Option[Double], BDD)] = {
    if (filterOnFinish) {
      reduction.filter({ case (_, bdd) => bdd != BDD.False })
    } else {
      reduction
    }
  }

  def bufferEncoder: Encoder[List[(Option[Double], BDD)]] =
    ExpressionEncoder()

  def outputEncoder: Encoder[List[(Option[Double], BDD)]] =
    ExpressionEncoder()
}
