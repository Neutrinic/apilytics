package com.apilytics.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import munit.FunSuite

/** Guards the coupling between our jackson-databind pin and Spark's Jackson line.
  *
  * `build.sbt` pins jackson-databind via `dependencyOverrides` to stay ahead of CVEs.
  * Spark ships its own jackson-module-scala, which validates the databind version at
  * registration time and refuses anything outside a narrow range. Pin below that range
  * and `ErrorClassesJsonReader` dies on class-init, taking all Spark error construction
  * with it (see #185).
  *
  * That failure is invisible to the rest of the suite — nothing else registers the Scala
  * module — so without this test a bad pin ships green. Re-check on every Spark upgrade:
  * the pin has to track Spark's Jackson line, not just the CVE floor.
  */
class JacksonCompatibilitySuite extends FunSuite {

  test("jackson-databind pin satisfies Spark's jackson-module-scala version range") {
    // Exactly what Spark's ErrorClassesJsonReader does on init. Throws
    // JsonMappingException("Scala module X requires Jackson Databind version >= ...")
    // when the versions have drifted apart.
    new ObjectMapper().registerModule(DefaultScalaModule)
  }
}
