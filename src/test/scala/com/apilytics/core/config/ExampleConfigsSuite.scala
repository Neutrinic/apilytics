package com.apilytics.core.config

import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import munit.FunSuite

/** The shipped example configs must be internally consistent.
  *
  * These files are the project's front door: `pokeapi-config.conf` is the Docker image's
  * default, and the README points at the others. They are also easy to break silently —
  * a config naming a spec that is not where it says produces a runtime failure that no
  * unit test of the loader would catch, and that nobody sees until someone runs the image.
  *
  * The slack config carried exactly that bug: it named `/opt/spark/examples/...` while the
  * image copies examples to `/opt/apilytics/examples/`, so it had been broken in every
  * published image.
  */
class ExampleConfigsSuite extends FunSuite {

  private val examplesDir = new java.io.File("examples")

  private def exampleConfigs: List[java.io.File] =
    Option(examplesDir.listFiles())
      .getOrElse(Array.empty)
      .filter(_.isDirectory)
      .flatMap(dir => Option(dir.listFiles()).getOrElse(Array.empty))
      .filter(_.getName.endsWith(".conf"))
      .sortBy(_.getPath)
      .toList

  /** The `openapi` value plus the directory it should resolve against.
    *
    * Substitutions are left unresolved: examples that read credentials from the
    * environment (`${SLACK_BOT_TOKEN}`) cannot fully resolve on a machine that has not
    * set them, and that is by design — but their spec path is still worth checking.
    */
  private def specLocationOf(file: java.io.File): String = {
    val parsed = ConfigFactory
      .parseFile(file)
      .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
    Loader.resolveSpecLocation(parsed.getString("openapi"), Option(file.getAbsoluteFile.getParentFile))
  }

  private def isUrl(location: String): Boolean =
    location.matches("^[A-Za-z][A-Za-z0-9+.\\-]+:.*")

  test("there are example configs to check") {
    // Guards the checks below from passing vacuously if the layout ever moves.
    assert(exampleConfigs.nonEmpty, s"found no .conf files under ${examplesDir.getAbsolutePath}")
  }

  test("every example config is parseable and declares a spec") {
    exampleConfigs.foreach { file =>
      try assert(specLocationOf(file).nonEmpty, s"${file.getPath} declares an empty openapi")
      catch {
        case e: Throwable => fail(s"${file.getPath} failed to parse: ${e.getMessage}")
      }
    }
  }

  test("every locally-bundled spec exists where its config says") {
    // Configs pointing at a third-party spec over HTTP (GitHub's, petstore) are the
    // author's call and are not checked here — only specs we ship ourselves.
    val checked = exampleConfigs.filterNot(f => isUrl(specLocationOf(f)))

    checked.foreach { file =>
      val spec = specLocationOf(file)
      assert(new java.io.File(spec).isFile, s"${file.getPath} points at a missing spec: $spec")
    }

    assert(checked.nonEmpty, "no config bundles a local spec; this check would pass vacuously")
  }

  test("no config fetches its own bundled spec from our repo at runtime") {
    // A spec that ships beside its config should be read from disk. Pulling it from
    // github/main instead makes a released example depend on the tip of the default
    // branch, so editing the spec retroactively changes what an old release does.
    exampleConfigs.foreach { file =>
      val spec = specLocationOf(file)
      assert(
        !spec.contains("githubusercontent.com/Neutrinic/apilytics"),
        s"${file.getPath} fetches its own bundled spec over the network: $spec"
      )
    }
  }

  test("configs that need no environment load end to end") {
    // The full Loader path, for every example that does not depend on unset credentials.
    val loaded = exampleConfigs.flatMap { file =>
      try Some(file -> Loader.load(file.getPath))
      catch { case _: com.typesafe.config.ConfigException.UnresolvedSubstitution => None }
    }

    assert(loaded.nonEmpty, "no example config loaded without environment setup")
    loaded.foreach { case (file, config) =>
      assert(config.openapi.nonEmpty, s"${file.getPath} loaded with an empty openapi")
    }
  }
}
