package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import scala.jdk.CollectionConverters._
import com.doubtless.spark.ProbSumUDAF

class HiveProbSumUDAF extends UDAF {}
object HiveProbSumUDAF {
  class HiveProbSumEvaluator extends UDAFEvaluator {
    var agg: Map[Double, BDD] = ProbSumUDAF.zero

    override def init() = {
      this.agg = ProbSumUDAF.zero
    }

    def iterate(number: Double, bytes: BytesWritable) = {
      val inputBdd = new BDD(bytes.getBytes())

      this.agg = ProbSumUDAF.reduce(this.agg, (number, inputBdd))

      true
    }

    def merge(inter: java.util.Map[java.lang.Double, BytesWritable]) = {
      val otherAgg = inter.asScala
        .map({ case (number, bytes) =>
          (number * 1.0) -> new BDD(bytes.getBytes())
        })
        .toMap

      this.agg = ProbSumUDAF.merge(this.agg, otherAgg)

      true
    }

    def terminatePartial(): java.util.Map[java.lang.Double, BytesWritable] =
      terminate()

    def terminate(): java.util.Map[java.lang.Double, BytesWritable] =
      ProbSumUDAF
        .finish(this.agg)
        .map({ case (num, bdd) =>
          (new java.lang.Double(num)) -> new BytesWritable(bdd.buffer)
        })
        .asJava
  }
}
