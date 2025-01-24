package com.doubtless.spark.hive

import com.doubtless.bdd.BDD
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.io.BytesWritable

class HiveBDDAggOrUDAF extends UDAF { }
object HiveBDDAggOrUDAF {
  class HiveBDDAggOrEvaluator extends UDAFEvaluator {
    var bdd: BDD = BDD.False

    override def init() = {
      this.bdd = BDD.False
    }

    def iterate(bytes: BytesWritable) = {
      this.bdd = this.bdd | (new BDD(bytes.getBytes()))
      true
    }

    def terminatePartial() = terminate()

    def merge(bytes: BytesWritable) = {
      this.bdd = this.bdd | (new BDD(bytes.getBytes()))
      true
    }

    def terminate() = new BytesWritable(this.bdd.buffer)
  }
}
