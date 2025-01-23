package com.doubtless.spark.hive

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.{Text, BytesWritable}
import com.doubtless.bdd.BDD

class HiveBDD extends UDF {
  def evaluate(input: Text): Array[Byte] = {
    if (input == null) return null

    BDD(input.toString()).buffer
  }
}

class HiveBDDToString extends UDF {
  def evaluate(input: BytesWritable): String = {
    val bdd = new BDD(input.getBytes())
    bdd.toString()
  }
}
