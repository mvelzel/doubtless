package com.doubtless.spark.hive

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.{Text, BytesWritable}
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions

class HiveConfig extends UDF {
  val config = ConfigFactory.load().getConfig("com.doubtless.spark")

  def evaluate(): String = {
    config.root().render(ConfigRenderOptions.concise())
  }
}
