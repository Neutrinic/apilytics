package com.apilytics

import com.apilytics.core.config.Loader
import com.apilytics.core.rest.RestSourceCatalog

/** Loads configs and parses their specs on the classpath a Spark user actually has:
  * `spark-sql`'s dependencies plus ours, and no test-only libraries.
  *
  * The unit tests cannot catch a missing runtime dependency when a test library happens
  * to bring it. That is how a YAML spec parsed in every test while the published
  * dependencies had no YAML module at all: WireMock's jackson stack supplied one, and only
  * to the tests (#259). Run with `sbt checkRuntimeClasspath`, which builds that classpath.
  *
  * A plain `main`, not a munit suite, because munit is exactly the kind of test library
  * that must be absent.
  */
object RuntimeClasspathCheck {
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "usage: RuntimeClasspathCheck <config.conf>...")
    args.foreach { path =>
      val tables = new RestSourceCatalog(Loader.load(path)).tableNames
      require(tables.nonEmpty, s"$path produced no tables")
      println(s"ok: $path (${tables.size} tables)")
    }
  }
}
