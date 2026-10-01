package com.apilytics

import com.apilytics.core.config.Loader
import com.apilytics.core.openapi.Parser
import com.apilytics.core.rest.RestSourceCatalog

/** Loads configs and parses their specs on the classpath a Spark user actually has: the
  * published, shaded jar and `spark-sql`'s own dependencies, and no test-only libraries.
  *
  * The unit tests cannot catch a missing runtime dependency when a test library happens
  * to bring it. That is how a YAML spec parsed in every test while the published
  * dependencies had no YAML module at all: WireMock's jackson stack supplied one, and only
  * to the tests (#259). Run with `sbt checkRuntimeClasspath`, which builds that classpath;
  * `-DplatformJars=a.jar,b.jar` puts real platform jars ahead of ours (#264).
  *
  * A plain `main`, not a munit suite, because munit is exactly the kind of test library
  * that must be absent.
  */
object RuntimeClasspathCheck {
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "usage: RuntimeClasspathCheck <config.conf>...")

    // The HTTP stack first: a clash there is what hung on Databricks DBR 18 (#264).
    com.apilytics.core.runtime.Preflight.verify()
    println("ok: HTTP stack initialises")

    args.foreach { path =>
      val config = Loader.load(path)

      // What the spec itself yields. The table list alone would not show a failed parse:
      // it always includes the tables the config names, spec or no spec.
      val endpoints = Parser.parse(config.openapi).endpoints
      require(endpoints.nonEmpty, s"$path: the spec parsed to no endpoints")

      // Every table's columns come from the spec. A table the spec does not describe falls
      // back to an empty schema, so an empty one here means the spec was not read.
      val source  = new RestSourceCatalog(config)
      val tables  = source.tableNames
      val noSchema = tables.filter(t => source.table(t).forall(_.schema.properties.isEmpty))
      require(noSchema.isEmpty, s"$path: no schema from the spec for ${noSchema.mkString(", ")}")

      println(s"ok: $path (${endpoints.size} endpoints, ${tables.size} tables with schemas)")
    }
  }
}
