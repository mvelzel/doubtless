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

class HiveProbCountUDAF extends UDAF {}
object HiveProbCountUDAF {
  class HiveProbCountEvaluator extends UDAFEvaluator {
    var agg: Map[Int, BDD] = Map[Int, BDD]((0 -> BDD.True))

    override def init() = {
      this.agg = Map[Int, BDD]((0 -> BDD.True))
    }

    def iterate(bytes: BytesWritable) = {
      val inputBdd = new BDD(bytes.getBytes())
      this.agg = agg.map({ case (count, bdd) =>
        agg get (count + 1) match {
          case Some(accBdd) =>
            (count + 1) -> ((bdd & inputBdd) | (accBdd & ~inputBdd))
          case None => (count + 1) -> (bdd & inputBdd)
        }
      }) + (0 -> (agg(0) & ~inputBdd))
      true
    }

    def terminatePartial(): java.util.Map[IntWritable, BytesWritable] =
      terminate()

    def merge(inter: java.util.Map[IntWritable, BytesWritable]) = {
      val scalaInter = inter.asScala
      val totalMax = this.agg.keys.max + scalaInter.keys.max.get()
      val otherAgg = scalaInter.map({ case (count, bytes) =>
        (count) -> new BDD(bytes.getBytes())
      })

      this.agg = Map(0 to totalMax map { count =>
        (count ->
          (0 to count)
            .map(i =>
              scalaInter get (new IntWritable(i)) match {
                case Some(bytes) =>
                  (this.agg getOrElse (count - i, BDD.False)) & (new BDD(
                    bytes.getBytes()
                  ))
                case None => BDD.False
              }
            )
            .reduce((bdd1: BDD, bdd2: BDD) => bdd1 | bdd2))
      }: _*)
      true
    }

    def terminate(): java.util.Map[IntWritable, BytesWritable] =
      this.agg
        .filter({ case (_, bdd) => bdd != BDD.False })
        .map({ case (count, bdd) =>
          (new IntWritable(count)) -> new BytesWritable(bdd.buffer)
        })
        .asJava
  }
}
