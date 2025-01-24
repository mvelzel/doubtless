package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.BytesWritable

class HiveBDDAggOrUDAF extends UDAF { }
object HiveBDDAggOrUDAF {
  class HiveBDDAggOrEvaluator extends UDAFEvaluator {
    var bdd: BDD = BDD("0")

    override def init() = {
      this.bdd = BDD("0")
    }

    def iterate(bytes: BytesWritable) = {
      this.bdd = this.bdd | (new BDD(bytes.getBytes()))
      true
    }

    def terminatePartial() = new BytesWritable(this.bdd.buffer)

    def merge(bytes: BytesWritable) = {
      this.bdd = this.bdd | (new BDD(bytes.getBytes()))
      true
    }

    def terminate() = new BytesWritable(this.bdd.buffer)
  }
}
