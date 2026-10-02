package com.apilytics.core.checkpoint

import cats.effect.IO
import com.apilytics.core.config.CheckpointConfig
import io.circe.parser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** Storage abstraction for checkpoint state.
  *
  * Uses Java NIO for plain local paths and Hadoop's FileSystem API for any path with a
  * URI scheme (HDFS, S3, GCS, ADLS, DBFS, `file://`).
  */
trait CheckpointStore {
  def read(tableName: String): IO[Option[CheckpointState]]
  def write(tableName: String, state: CheckpointState): IO[Unit]
}

object CheckpointStore {

  /** No-op store for when checkpointing is disabled. */
  val disabled: CheckpointStore = new CheckpointStore {
    override def read(tableName: String): IO[Option[CheckpointState]] = IO.pure(None)
    override def write(tableName: String, state: CheckpointState): IO[Unit] = IO.unit
  }

  /** Create a checkpoint store from config. Returns disabled store if config is absent or disabled. */
  def fromConfig(config: Option[CheckpointConfig], hadoopConf: Configuration): CheckpointStore =
    config.filter(_.enabled) match {
      case Some(cc) if isRemotePath(cc.path) => new HadoopCheckpointStore(cc.path, hadoopConf)
      case Some(cc)                          => new LocalCheckpointStore(cc.path)
      case None                              => disabled
    }

  /** Whether a path goes through Hadoop's FileSystem: any path with a URI scheme.
    *
    * Only `hdfs://`, `s3://`, `s3a://` and `gs://` used to: `abfss://`, `wasbs://`, `dbfs:/`
    * and `file://` fell to the local store, where `Paths.get` made them relative paths under
    * the executor's working directory, so the checkpoint didn't persist (#316). Hadoop
    * resolves every scheme it has a connector for. A scheme is two or more characters, so a
    * Windows drive such as `C:\` stays local, and so do plain paths, including Databricks'
    * `/Volumes/...` mounts.
    */
  private[checkpoint] def isRemotePath(path: String): Boolean =
    path.matches("^[A-Za-z][A-Za-z0-9+.-]+:.*")

  /** Local filesystem checkpoint store using Java NIO. */
  private class LocalCheckpointStore(basePath: String) extends CheckpointStore {

    private def checkpointFile(tableName: String): java.nio.file.Path =
      Paths.get(basePath, s"$tableName.checkpoint.json")

    override def read(tableName: String): IO[Option[CheckpointState]] = IO {
      val file = checkpointFile(tableName)
      if (Files.exists(file)) {
        val content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
        parser
          .parse(content)
          .flatMap(CheckpointState.fromJson)
          .toOption
      } else None
    }

    override def write(tableName: String, state: CheckpointState): IO[Unit] = IO {
      val file = checkpointFile(tableName)
      val parent = file.getParent
      if (!Files.exists(parent)) Files.createDirectories(parent)
      Files.write(file, state.toJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      ()
    }
  }

  /** Hadoop-based checkpoint store for remote filesystems (HDFS, S3). */
  private class HadoopCheckpointStore(basePath: String, hadoopConf: Configuration)
      extends CheckpointStore {

    private def getFs: FileSystem = {
      val path = new Path(basePath)
      FileSystem.get(path.toUri, hadoopConf)
    }

    private def checkpointFile(tableName: String): Path =
      new Path(basePath, s"$tableName.checkpoint.json")

    override def read(tableName: String): IO[Option[CheckpointState]] = IO {
      val fs = getFs
      val path = checkpointFile(tableName)
      if (fs.exists(path)) {
        val stream = fs.open(path)
        val content = try {
          scala.io.Source.fromInputStream(stream)(scala.io.Codec.UTF8).mkString
        } finally {
          stream.close()
        }
        parser
          .parse(content)
          .flatMap(CheckpointState.fromJson)
          .toOption
      } else None
    }

    override def write(tableName: String, state: CheckpointState): IO[Unit] = IO {
      val fs = getFs
      val path = checkpointFile(tableName)
      val parent = path.getParent
      if (!fs.exists(parent)) fs.mkdirs(parent)

      val out = fs.create(path, true) // overwrite
      try {
        out.write(state.toJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      } finally {
        out.close()
      }
    }
  }
}
