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
    if (input == null) return null

    val bdd = new BDD(input.getBytes())
    bdd.toString()
  }
}

class HiveBDDAnd extends UDF {
  def evaluate(left: BytesWritable, right: BytesWritable): Array[Byte] = {
    if (left == null || right == null) return null

    val leftBdd = new BDD(left.getBytes())
    val rightBdd = new BDD(right.getBytes())
    (leftBdd & rightBdd).buffer
  }
}

class HiveBDDOr extends UDF {
  def evaluate(left: BytesWritable, right: BytesWritable): Array[Byte] = {
    if (left == null || right == null) return null

    val leftBdd = new BDD(left.getBytes())
    val rightBdd = new BDD(right.getBytes())
    (leftBdd | rightBdd).buffer
  }
}

class HiveBDDNot extends UDF {
  def evaluate(input: BytesWritable): Array[Byte] = {
    if (input == null) return null

    val bdd = new BDD(input.getBytes())
    (~bdd).buffer
  }
}
