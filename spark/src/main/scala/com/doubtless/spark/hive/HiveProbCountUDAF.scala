package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import scala.jdk.CollectionConverters._
import com.doubtless.spark.ProbCountUDAF

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
    var agg: Map[Int, BDD] = ProbCountUDAF.zero

    override def init() = {
      this.agg = ProbCountUDAF.zero
    }

    def iterate(bytes: BytesWritable) = {
      val inputBdd = new BDD(bytes.getBytes())
      this.agg = ProbCountUDAF.reduce(this.agg, inputBdd)
      true
    }

    def merge(inter: java.util.Map[IntWritable, BytesWritable]) = {
      val otherAgg = inter.asScala
        .map({ case (count, bytes) =>
          (count.get()) -> new BDD(bytes.getBytes())
        })
        .toMap

      this.agg = ProbCountUDAF.merge(this.agg, otherAgg)
      true
    }

    def terminatePartial(): java.util.Map[IntWritable, BytesWritable] =
      terminate()

    def terminate(): java.util.Map[IntWritable, BytesWritable] =
      this.agg
        .filter({ case (_, bdd) => bdd != BDD.False })
        .map({ case (count, bdd) =>
          (new IntWritable(count)) -> new BytesWritable(bdd.buffer)
        })
        .asJava
  }
}
