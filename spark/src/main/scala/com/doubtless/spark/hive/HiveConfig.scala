package com.doubtless.spark.hive

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.{Text, BytesWritable}
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import java.io.File

class HiveConfig extends UDF {
  def evaluate(): String = {
    val config =
      ConfigFactory
        .parseFile(
          new File(
            "/Users/mvelzel/doubtless/spark/src/main/resources/application.conf"
          )
        )
        .getConfig("com.doubtless.spark")

      config.root().render(ConfigRenderOptions.concise())
  }
}
