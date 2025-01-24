package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import scala.jdk.CollectionConverters._

/*
Example SQL:

with grouped as (
  select prob_count(bdd) as map from experiments.my_first_dbt_model group by group
)
select key, bdd_to_string(value) from grouped lateral view explode(map) explodeVal as key, value;

 */

class HiveProbSumUDAF extends UDAF {}
object HiveProbSumUDAF {
  class HiveProbSumEvaluator extends UDAFEvaluator {
    var agg: Map[Double, BDD] = Map[Double, BDD]((0.0 -> BDD.True))

    override def init() = {
      this.agg = Map[Double, BDD]((0.0 -> BDD.True))
    }

    def iterate(number: Double, bytes: BytesWritable) = {
      val inputBdd = new BDD(bytes.getBytes())
      val inputNumber = number

      this.agg = this.agg.map({ case (sum, bdd) =>
        (sum -> (bdd & ~inputBdd))
      }) ++ this.agg.map({ case (sum, bdd) =>
        this.agg get (sum + inputNumber) match {
          case Some(accBdd) =>
            (sum + inputNumber) -> ((bdd & inputBdd) | (accBdd & ~inputBdd))
          case None => (sum + inputNumber) -> (bdd & inputBdd)
        }
      })

      true
    }

    def terminatePartial(): java.util.Map[java.lang.Double, BytesWritable] =
      terminate()

    def merge(inter: java.util.Map[java.lang.Double, BytesWritable]) = {
      val scalaInter = inter.asScala

      this.agg = this.agg.toSeq
        .flatMap(tup1 =>
          scalaInter.toSeq.map(tup2 =>
            (tup1, (tup2._1 -> new BDD(tup2._2.getBytes())))
          )
        )
        .foldLeft(Map[Double, BDD]())((acc, tups) =>
          acc get (tups._1._1 + tups._2._1) match {
            case Some(accBdd) =>
              acc + ((tups._1._1 + tups._2._1) -> ((tups._1._2 & tups._2._2) | accBdd))
            case None =>
              acc + ((tups._1._1 + tups._2._1) -> (tups._1._2 & tups._2._2))
          }
        )

      true
    }

    def terminate(): java.util.Map[java.lang.Double, BytesWritable] =
      this.agg
        .filter({ case (_, bdd) => bdd != BDD.False })
        .map({ case (num, bdd) =>
          (new java.lang.Double(num)) -> new BytesWritable(bdd.buffer)
        })
        .asJava
  }
}
