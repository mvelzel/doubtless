package com.doubtless.spark

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text
import com.doubtless.bdd.BDD

class TestHiveUDF extends UDF {
  // Simple UDF that converts a string to uppercase
  def evaluate(input: Text): BDD = {
    if (input == null) return null
    BDD(input.toString())
  }
}
