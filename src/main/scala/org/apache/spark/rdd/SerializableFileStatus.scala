package org.apache.spark.rdd

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.fs.s3a.S3AFileStatus

case class SerializableFileStatus(path: String, isDirectory: Boolean, isEmptyDirectory: Boolean,
                                  length: Option[Long], mofidicationTime: Option[Long],
                                  blocksize: Option[Long]) {
  def toS3AFileStatus = {
    if (isDirectory) {
      new S3AFileStatus(isDirectory, isEmptyDirectory, new Path(new URI(path)))
    } else {
      new S3AFileStatus(length.getOrElse(0L), mofidicationTime.getOrElse(0L), new Path(new URI(path)))
    }
  }
}

object SerializableFileStatus {
  def fromS3AFileStatus(file: S3AFileStatus): SerializableFileStatus = {
    SerializableFileStatus(file.getPath.toString, file.isDir, file.isEmptyDirectory,
      if (!file.isDir) Some(file.getLen) else None,
      if (!file.isDir) Some(file.getModificationTime) else None,
      None)
  }

  def default(file: FileStatus): SerializableFileStatus = {
    SerializableFileStatus(file.getPath.toString, file.isDir, false,
    if (!file.isDir) Some(file.getLen) else None,
    if (!file.isDir) Some(file.getModificationTime) else None,
    if (!file.isDir) Some(file.getBlockSize) else None)
  }

  def fromFileStatus(file: FileStatus): SerializableFileStatus = file match {
    case file: S3AFileStatus => fromS3AFileStatus(file)
    case file: FileStatus => default(file)
    case _ => throw new Exception("Unsupported operation")
  }
}