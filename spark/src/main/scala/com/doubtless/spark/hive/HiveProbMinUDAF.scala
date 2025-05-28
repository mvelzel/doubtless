package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import scala.jdk.CollectionConverters._
import com.doubtless.spark.ProbCountUDAF
import com.doubtless.spark.ProbMinUDAF

/*
Example SQL:

with grouped as (
  select prob_count(bdd) as map from experiments.my_first_dbt_model group by group
)
select key, bdd_to_string(value) from grouped lateral view explode(map) explodeVal as key, value;

 */

class ProbMinResult(val min: java.lang.Double, val bdd: BytesWritable) {}

class HiveProbMinUDAF extends UDAF {}
object HiveProbMinUDAF {
  class HiveProbMinEvaluator extends UDAFEvaluator {
    var agg: List[(Option[Double], BDD)] = ProbMinUDAF.zero

    override def init() = {
      this.agg = ProbMinUDAF.zero
    }

    def iterate(number: java.lang.Double, bytes: BytesWritable) = {
      val inputBdd = new BDD(bytes.getBytes())
      this.agg = ProbMinUDAF
        .reduce(this.agg, (Option(number), inputBdd))
      true
    }

    def merge(
        inter: java.util.List[
          ProbMinResult
        ]
    ) = {
      val otherAgg = inter.asScala
        .map(entry =>
          Option(entry.min * 1.0) -> new BDD(
            entry.bdd.getBytes()
          )
        )
        .toList

      this.agg = ProbMinUDAF
        .merge(this.agg, otherAgg)

      true
    }

    def terminatePartial(): java.util.List[
      ProbMinResult
    ] = {
      println(this.agg)
      terminate()
    }

    def terminate(): java.util.List[
      ProbMinResult
    ] = {
      ProbMinUDAF
        .finish(this.agg)
        .map({
          case (num, bdd) => {
            val number: java.lang.Double = num match {
              case Some(dub) => new java.lang.Double(dub)
              case None      => null
            }
            new ProbMinResult(number, new BytesWritable(bdd.buffer))
          }
        })
        .asJava
    }
  }
}
