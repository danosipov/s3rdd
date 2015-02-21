package org.apache.spark.rdd

import java.io.IOException
import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.spark.SerializableWritable

import scala.annotation.tailrec

/**
 * TODO
 */
class S3TextInputFormat extends TextInputFormat with S3InputFormat {

  /** List input directories.
    * Subclasses may override to, e.g., select only files matching a regular
    * expression.
    * TODO: Reimplement filtering (if needed)
    *
    * @param job the job to list input paths for
    * @return array of FileStatus objects
    * @throws IOException if zero items.
    */
  override protected def listStatus(job: JobConf): Array[FileStatus] = {
    import scala.collection.JavaConversions._

    val dirs: Array[Path] = FileInputFormat.getInputPaths(job)
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job")
    }
    TokenCache.obtainTokensForNamenodes(job.getCredentials, dirs, job)
    val result: util.ArrayList[FileStatus] = new util.ArrayList[FileStatus]
    var errors: List[IOException] = List.empty[IOException]

    for (p <- dirs) {
      val fs: FileSystem = p.getFileSystem(job)
      setMinSplitSize(fs.getDefaultBlockSize) // FIXME: Hack, Move to a more appropriate place
      val matches: Array[FileStatus] = fs.globStatus(p)
      if (matches == null) {
        errors = errors :+ new IOException("Input path does not exist: " + p)
      }
      else if (matches.length == 0) {
        errors = errors :+ new IOException("Input Pattern " + p + " matches 0 files")
      }
      else {
        val directories = matches.filter(_.isDir).map(_.getPath.toString)
        matches.filter(!_.isDir).foreach(s => result.add(s))

        val localJob = sparkContext.broadcast(new SerializableWritable(job))
        sparkContext.parallelize(directories)
          .flatMap { d =>
            println("Processing " + d)
            def loopOverDir(p: Path): Array[FileStatus] = {
              val localFs = p.getFileSystem(localJob.value.value)
              val status = localFs.listStatus(p)
              val subStatus = status.filter(_.isDir).flatMap(d => loopOverDir(d.getPath))
              status.filter(!_.isDir) ++ subStatus
            }
            val dir = new Path(new URI(d))
            loopOverDir(dir).map(f => SerializableFileStatus.fromFileStatus(f))
          }
          .collect()
          .foreach(s => result.add(s.toS3AFileStatus))
      }
    }

    if (!errors.isEmpty) {
      throw new InvalidInputException(errors)
    }

    result.toArray(new Array[FileStatus](result.size))
  }
}