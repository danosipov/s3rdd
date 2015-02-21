package org.apache.spark.rdd

import java.net.URI

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil

class S3RDD[K, V](
  @transient sc: SparkContext,
  broadcastedConf: Broadcast[SerializableWritable[Configuration]],
  initLocalJobConfFuncOpt: Option[JobConf => Unit],
  inputFormatClass: Class[_ <: InputFormat[K, V]],
  keyClass: Class[K],
  valueClass: Class[V],
  path: String
  ) extends HadoopRDD[K, V](
    sc, broadcastedConf, initLocalJobConfFuncOpt,
    inputFormatClass, keyClass, valueClass, 1) {

//  def this(
//            sc: SparkContext,
//            conf: JobConf,
//            inputFormatClass: Class[_ <: InputFormat[K, V]],
//            keyClass: Class[K],
//            valueClass: Class[V],
//            minPartitions: Int) = {
//    this(
//      sc,
//      sc.broadcast(new SerializableWritable(conf))
//        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
//      None /* initLocalJobConfFuncOpt */,
//      inputFormatClass,
//      keyClass,
//      valueClass,
//      minPartitions)
//  }

  checkConfig

//  val (bucket, s3path) = parsePath()
//  println(bucket)
//  println(s3path)

// TODO: Override OutputCommitter
// http://mail-archives.apache.org/mod_mbox/spark-user/201410.mbox/%3C543E33FA.2000802@entropy.be%3E
// https://issues.apache.org/jira/browse/SPARK-3595

  // Override getPartitions
  // See if it can be done as a task on the cluster
  // sc.parallelize().map.collect()
  override def getPartitions() = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    
    val inputFormat = getInputFormat(jobConf)
    if (inputFormat.isInstanceOf[Configurable]) {
      inputFormat.asInstanceOf[Configurable].setConf(jobConf)
    }
    if (inputFormat.isInstanceOf[S3InputFormat]) {
      inputFormat.asInstanceOf[S3InputFormat].sparkContext = sc
      //inputFormat.asInstanceOf[S3InputFormat].broadcastedConf = broadcastedConf
    }

    
    val inputSplits = inputFormat.getSplits(jobConf, 1)

    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }

  //assert(false, "S3 support is not implemented yet!")

  def checkConfig = {
    val conf = getJobConf()
    val s3aConf = conf.get("fs.s3a.impl")
    if (s3aConf == null) {
      throw new Exception("S3A File System disabled. \n" +
        "Enable by adding the following properties to core-site.xml:\n" +
        "<property>\n  <name>fs.s3a.access.key</name>\n  <value>...</value>\n</property>\n\n" +
        "<property>\n  <name>fs.s3a.secret.key</name>\n  <value>...</value>\n</property>\n\n" +
        "<property>\n  <name>fs.s3a.impl</name>\n  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>\n</property>")
    }
  }

//  def parsePath() = {
//    val uri = new URI(path)
//    uri.getScheme match {
//      case "s3" => {
//        log.info(this.getClass.toString, "Rewriting URL to use S3A protocol")
//        uri.toString.replace("s3://", "s3a://")
//      }
//      case "s3n" => {
//        log.info(this.getClass.toString, "Rewriting URL to use S3A protocol")
//        uri.toString.replace("s3n://", "s3a://")
//      }
//      case "s3a" => uri.toString
//      case _ => throw new Exception("Unsupported URL provided: " + path + ", Scheme not supported")
//    }
////    val relativePath = uri.getPath.head match {
////      case '/' => uri.getPath.substring(1)
////      case _ => uri.getPath
////    }
////    (uri.getHost, relativePath)
//  }
}

object S3RDD {
  def textFile(path: String, sc: SparkContext) = {
    // Provide proper configuration, if not set.
    // Alternative is to fail, but this minimizes friction in deployment
    val jobConf = sc.hadoopConfiguration
    val accessKey: String = jobConf.getStrings("fs.s3n.awsAccessKeyId").headOption.getOrElse {
      jobConf.getStrings("fs.s3.awsAccessKeyId").headOption.getOrElse {
        jobConf.getStrings("fs.s3a.awsAccessKeyId").headOption.orNull
      }
    }
    val secretKey = jobConf.getStrings("fs.s3n.awsSecretAccessKey").headOption.getOrElse {
      jobConf.getStrings("fs.s3.awsSecretAccessKey").headOption.getOrElse {
        jobConf.getStrings("fs.s3a.awsSecretAccessKey").headOption.orNull
      }
    }
    jobConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    jobConf.set("fs.s3a.access.key", accessKey)
    jobConf.set("fs.s3a.secret.key", secretKey)

    val newPath = parsePath(path)

    // mimic hadoopFile in SparkContext
    val confBroadcast = sc.broadcast(new SerializableWritable(jobConf))
    val setInputPathsFunc = (passedJobConf: JobConf) => FileInputFormat.setInputPaths(passedJobConf, newPath)
    new S3RDD(
      sc,
      confBroadcast,
      Some(setInputPathsFunc),
      classOf[S3TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      newPath).map(pair => pair._2.toString).setName(path)
  }

  def parsePath(path: String) = {
    val uri = new URI(path)
    uri.getScheme match {
      case "s3" => uri.toString.replace("s3://", "s3a://")
      case "s3n" => uri.toString.replace("s3n://", "s3a://")
      case "s3a" => uri.toString
      case _ => throw new Exception("Unsupported URL provided: " + path + ", Scheme not supported")
    }
  }
}