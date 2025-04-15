package com.doubtless

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import com.doubtless.spark._
import com.doubtless.bdd.BDD

package object spark {
  def createSparkSession(appName: String, isLocal: Boolean): SparkSession = {
    val spark = if (isLocal) {
      SparkSession
        .builder()
        .config("spark.sql.caseSensitive", value = false)
        .config("spark.sql.session.timeZone", value = "UTC")
        .config("spark.driver.memory", value = "18G")
        .appName(appName)
        .master("local[*]")
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .config("spark.sql.caseSensitive", value = false)
        .config("spark.sql.session.timeZone", value = "UTC")
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()
    }

    spark.sparkContext.setLogLevel("WARN")

    spark.udf.register("BDDAggOr", functions.udaf(BDDAggOrUDAF))
    spark.udf.register("BDDAggAnd", functions.udaf(BDDAggAndUDAF))
    spark.udf.register("ProbCount", functions.udaf(ProbCountUDAF))
    spark.udf.register("ProbSum", functions.udaf(ProbSumUDAF))
    spark.udf.register("BDD", (expr: String) => { BDD(expr) })

    spark
  }
}
