package com.doubtless
import com.doubtless.spark._

import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

object DoubtlessThriftServer {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession("DoubtlessThriftServer", isLocal=false) 
    import spark.implicits._

    val test = spark.sql("select BDD('0')")
    test.show()

    spark.sql("SHOW FUNCTIONS").show(100, truncate = false)

    HiveThriftServer2.startWithContext(spark.sqlContext)
  }
}
