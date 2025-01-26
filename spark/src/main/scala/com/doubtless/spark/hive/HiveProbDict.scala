package com.doubtless.spark.hive

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.{Text, BytesWritable}
import com.doubtless.bdd.ProbDict

class HiveProbDict extends UDF {
  def evaluate(input: Text): Array[Byte] = {
    if (input == null) return null

    ProbDict(input.toString()).buffer
  }
}

class HiveProbDictToString extends UDF {
  def evaluate(input: BytesWritable): String = {
    if (input == null) return null

    val dict = new ProbDict(input.getBytes())
    dict.toString()
  }
}
