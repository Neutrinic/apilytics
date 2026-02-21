package com.apilytics.core.checkpoint

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{CheckpointConfig, CheckpointMode}
import munit.FunSuite
import org.apache.hadoop.conf.Configuration

import java.nio.file.{Files, Path}

class CheckpointStoreSuite extends FunSuite {

  /** Create a Hadoop Configuration that uses RawLocalFileSystem (no winutils needed on Windows). */
  private def testHadoopConf: Configuration = {
    val conf = new Configuration()
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)
    conf.setBoolean("fs.file.impl.disable.cache", true)
    conf
  }

  /** Fixture that creates a temp directory and cleans it up after the test. */
  private val tmpDirFixture = FunFixture[Path](
    setup = _ => Files.createTempDirectory("checkpoint-test"),
    teardown = tmpDir => {
      // Delete all files in the temp dir, then the dir itself
      if (Files.exists(tmpDir)) {
        Files.list(tmpDir).forEach(f => Files.deleteIfExists(f))
        Files.deleteIfExists(tmpDir)
      }
    }
  )

  test("disabled store returns None on read") {
    val result = CheckpointStore.disabled.read("test_table").unsafeRunSync()
    assertEquals(result, None)
  }

  test("disabled store write is no-op") {
    // Should not throw
    CheckpointStore.disabled.write("test_table", CheckpointState.CursorValue("abc")).unsafeRunSync()
  }

  test("fromConfig returns disabled when config is None") {
    val store = CheckpointStore.fromConfig(None, testHadoopConf)
    val result = store.read("test_table").unsafeRunSync()
    assertEquals(result, None)
  }

  test("fromConfig returns disabled when enabled is false") {
    val config = CheckpointConfig(enabled = false, path = "/tmp/test")
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)
    val result = store.read("test_table").unsafeRunSync()
    assertEquals(result, None)
  }

  tmpDirFixture.test("write and read cursor checkpoint") { tmpDir =>
    val config = CheckpointConfig(enabled = true, path = tmpDir.toString, mode = CheckpointMode.Cursor)
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)

    val state = CheckpointState.CursorValue("next-page-cursor")
    store.write("my_table", state).unsafeRunSync()

    val result = store.read("my_table").unsafeRunSync()
    assertEquals(result, Some(state))
  }

  tmpDirFixture.test("write and read offset checkpoint") { tmpDir =>
    val config = CheckpointConfig(enabled = true, path = tmpDir.toString, mode = CheckpointMode.Offset)
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)

    val state = CheckpointState.OffsetValue(500L)
    store.write("events", state).unsafeRunSync()

    val result = store.read("events").unsafeRunSync()
    assertEquals(result, Some(state))
  }

  tmpDirFixture.test("write and read timestamp checkpoint") { tmpDir =>
    val config = CheckpointConfig(enabled = true, path = tmpDir.toString, mode = CheckpointMode.Timestamp)
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)

    val state = CheckpointState.TimestampValue("2024-01-15T10:30:00Z")
    store.write("orders", state).unsafeRunSync()

    val result = store.read("orders").unsafeRunSync()
    assertEquals(result, Some(state))
  }

  tmpDirFixture.test("read returns None for non-existent table") { tmpDir =>
    val config = CheckpointConfig(enabled = true, path = tmpDir.toString)
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)

    val result = store.read("non_existent").unsafeRunSync()
    assertEquals(result, None)
  }

  tmpDirFixture.test("write overwrites previous checkpoint") { tmpDir =>
    val config = CheckpointConfig(enabled = true, path = tmpDir.toString)
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)

    store.write("table1", CheckpointState.CursorValue("first")).unsafeRunSync()
    store.write("table1", CheckpointState.CursorValue("second")).unsafeRunSync()

    val result = store.read("table1").unsafeRunSync()
    assertEquals(result, Some(CheckpointState.CursorValue("second")))
  }

  tmpDirFixture.test("multiple tables have independent checkpoints") { tmpDir =>
    val config = CheckpointConfig(enabled = true, path = tmpDir.toString)
    val store = CheckpointStore.fromConfig(Some(config), testHadoopConf)

    store.write("table_a", CheckpointState.CursorValue("cursor-a")).unsafeRunSync()
    store.write("table_b", CheckpointState.OffsetValue(100L)).unsafeRunSync()

    assertEquals(store.read("table_a").unsafeRunSync(), Some(CheckpointState.CursorValue("cursor-a")))
    assertEquals(store.read("table_b").unsafeRunSync(), Some(CheckpointState.OffsetValue(100L)))
  }
}
