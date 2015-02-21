package org.apache.spark.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.broadcast.Broadcast

/**
 * TODO
 */
trait S3InputFormat {
  var sparkContext: SparkContext = _
}
