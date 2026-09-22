ThisBuild / organization := "io.github.neutrinic"
// No `version` here on purpose. sbt-ci-release derives it from the git tag via dynver,
// and a literal setting silently outranks that: releases would carry whatever was written
// here rather than what was tagged.
// Must be >= the scala-library the dependency classpath pulls in (SIP-51):
// the bumped deps bring 2.13.18, and the compiler cannot be older than that.
ThisBuild / scalaVersion := "2.13.18"
// Tells sbt/coursier how to order versions when reporting evictions.
ThisBuild / versionScheme := Some("semver-spec")

// Publishing settings for Maven Central
ThisBuild / homepage := Some(url("https://github.com/Neutrinic/apilytics"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    id = "neutrinic",
    name = "Neutrinic",
    email = "neutrinic@users.noreply.github.com",
    url = url("https://github.com/Neutrinic")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Neutrinic/apilytics"),
    "scm:git@github.com:Neutrinic/apilytics.git"
  )
)

// Spark to build against. Overridable so the next Spark can be tried without editing
// the build: sbt -DsparkVersion=4.3.0 test
val sparkVersion    = sys.props.getOrElse("sparkVersion", "4.2.0")

/** Spark line this build targets, e.g. "4.2". Used to pick line-specific dependency pins
  * and to label matrix builds; it is deliberately NOT part of the artifact name. One jar
  * serves the whole Spark 4.x line — see the compatibility table in the README. */
val sparkMajorMinor = sparkVersion.split('.').take(2).mkString(".")

/** Libraries every supported Spark distribution already ships.
  *
  * Excluded from our dependencies so the published POM names only what a Spark
  * classpath lacks. Declaring them anyway does not change what runs — Spark's jars load
  * first — but it puts a second, different version beside Spark's, which breaks the moment
  * anything loads user jars first, and it means our tests exercise versions production
  * never sees. With them excluded, compile and test use Spark's own copies.
  *
  * Checked against the jars directories of Spark 4.0.4, 4.1.3 and 4.2.0. Not excluded:
  * jakarta.activation / validation / xml.bind 2.x, whose artifact names match Spark's but
  * whose packages do not (`javax.*` versus Spark's `jakarta.*`), so nothing overlaps; and
  * cats-kernel, where Spark's copy is too old for http4s.
  */
val onSparkClasspath = Seq(
  ExclusionRule("com.fasterxml.jackson.core"),
  ExclusionRule("com.fasterxml.jackson.dataformat"),
  ExclusionRule("com.fasterxml.jackson.datatype"),
  ExclusionRule("com.google.guava"),
  ExclusionRule("com.google.code.findbugs"),
  ExclusionRule("com.google.errorprone"),
  ExclusionRule("com.google.j2objc"),
  ExclusionRule("org.checkerframework"),
  ExclusionRule("commons-io"),
  ExclusionRule("commons-codec"),
  ExclusionRule("org.apache.commons", "commons-lang3"),
  ExclusionRule("org.apache.httpcomponents"),
  ExclusionRule("org.yaml", "snakeyaml"),
  ExclusionRule("org.slf4j"),
  ExclusionRule("joda-time"),
)

/** jackson-databind pinned to whatever Spark's bundled jackson-module-scala accepts.
  *
  * module-scala enforces a narrow databind range and refuses to initialise outside it,
  * taking Spark's error machinery down with it. The pin cannot be dropped: swagger-parser
  * pulls 2.22.0, which is outside every Spark 4.x range.
  *
  * Keyed on the full version, not the line, because the requirement moves *within* a line.
  * Spark 4.1.0 and 4.1.1 ship module-scala 2.20.0 wanting [2.20, 2.21); 4.1.2 moved to
  * 2.21.2, wanting [2.21, 2.22). Keying on "4.1" alone built against 4.1.0 and failed
  * against everything from 4.1.2 on.
  *
  * Read the boundary from the POMs rather than interpolating between two known versions —
  * the first attempt at this put the shift at 4.1.3 because only 4.1.0 and 4.1.3 had been
  * sampled, and 4.1.2 was silently wrong.
  *
  * Verify a new entry rather than guessing — the range lives in the `jackson-module-scala`
  * version of that Spark's spark-core POM:
  *   curl -s https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.13/<v>/spark-core_2.13-<v>.pom
  *
  * JacksonCompatibilitySuite asserts the pin satisfies the Spark on the test classpath, so
  * a wrong entry fails loudly instead of surfacing as NoClassDefFoundError in
  * RDDOperationScope, which is nowhere near the cause. */
val jacksonDatabind = {
  // sbt build files compile on Scala 2.12, so no toIntOption here.
  val patch = scala.util.Try(sparkVersion.split('.')(2).takeWhile(_.isDigit).toInt).getOrElse(-1)

  (sparkMajorMinor, patch) match {
    case ("4.0", p) if p >= 0          => "2.18.10" // module-scala 2.18.x across the line
    case ("4.1", p) if p >= 0 && p < 2 => "2.20.2"  // 4.1.0-4.1.1 ship module-scala 2.20.0
    case ("4.1", p) if p >= 2          => "2.21.5"  // 4.1.2 moved to 2.21.2 mid-line
    case ("4.2", p) if p >= 0          => "2.21.5"
    case _ =>
      // Deliberately fatal rather than falling back to a guess. An unverified pin does not
      // fail where you can see it: module-scala refuses to initialise and Spark surfaces
      // NoClassDefFoundError in RDDOperationScope, nowhere near the cause. That is exactly
      // how the 4.1.3 breakage hid (#246).
      sys.error(
        s"No jackson-databind pin is known for Spark '$sparkVersion'. Trying a new Spark " +
          "is a one-line edit rather than a guess: read the jackson-module-scala version " +
          s"from https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.13/" +
          s"$sparkVersion/spark-core_2.13-$sparkVersion.pom and add a case above with a " +
          "databind version inside the range it enforces."
      )
  }
}

lazy val root = (project in file("."))
  .settings(
    // The artifact name carries the Spark line, the version stays semver for our own
    // changes (#219). Upgrading Spark therefore means editing the dependency
    // coordinate — the loudest possible signal, and one no auto-bumper can cross.
    // One artifact per Spark major: 1.x targets Spark 4.x, 2.x will target Spark 5.
    // The minor line is not in the name because a single jar covers 4.0 through 4.2.
    name := "apilytics",
    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-sql"          % sparkVersion % "provided",
      "org.apache.spark" %% "spark-catalyst"      % sparkVersion % "provided",

      // HTTP + JSON
      // Their logging (log4s) asks for slf4j 1.7; Spark always ships 2.x, which keeps
      // the 1.7 API, so it is excluded along with the rest of `onSparkClasspath`.
      "org.http4s"       %% "http4s-ember-client" % "0.23.36" excludeAll (onSparkClasspath: _*),
      "org.http4s"       %% "http4s-circe"        % "0.23.36" excludeAll (onSparkClasspath: _*),
      "io.circe"         %% "circe-core"          % "0.14.16",
      "io.circe"         %% "circe-generic"       % "0.14.16",
      "io.circe"         %% "circe-parser"        % "0.14.16",
      "io.circe"         %% "circe-pointer"       % "0.14.16",

      // OpenAPI. Its chain also drags in libraries every Spark distribution already
      // ships, which are excluded below: see `onSparkClasspath`.
      "io.swagger.parser.v3" % "swagger-parser"   % "2.1.45" excludeAll (onSparkClasspath: _*),

      // Arrow is deliberately not declared: it comes from spark-sql, so we compile
      // against the Arrow the target Spark ships. ArrowColumnVector hands our buffers
      // straight to Spark's own Arrow, so there must only ever be Spark's copy.

      // Config
      "com.typesafe"      % "config"              % "1.4.9",

      // Streaming
      "co.fs2"           %% "fs2-core"            % "3.13.0",

      // Test
      "org.scalameta"    %% "munit"               % "1.3.5"  % Test,
      "org.typelevel"    %% "munit-cats-effect"   % "2.2.0"  % Test,
      "org.wiremock"      % "wiremock"            % "3.13.2" % Test,
    ),

    // Build-scoped only, and deliberately not published.
    //
    // jackson now reaches the build only through Spark, but test dependencies
    // (wiremock) carry their own and would evict Spark's upward, out of the narrow
    // databind window Spark's jackson-module-scala enforces (#185). Holding databind
    // at the pin keeps the test classpath a classpath Spark can actually start on.
    // JacksonCompatibilitySuite fails the build if this drifts out of range.
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabind,
    ),

    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-deprecation",
      "-feature",
      "-unchecked",
    ),

    // Spark 4 requires Java 17+
    javacOptions ++= Seq("-source", "17", "-target", "17"),

    // Arrow requires --add-opens on Java 17+
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
    ),
    Test / fork := true,

    // Assembly settings
    assembly / assemblyJarName := "apilytics.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*)           => MergeStrategy.discard
      case "module-info.class"                     => MergeStrategy.discard
      case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto")               => MergeStrategy.first
      case "reference.conf"                        => MergeStrategy.concat
      case _                                       => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption)
      .value
      .withIncludeScala(false)
      .withIncludeDependency(true),
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { jar =>
        val name = jar.data.getName
        name.contains("spark-") ||
          name.contains("scala-library") ||
          name.contains("scala-reflect") ||
          name.contains("hadoop-")
      }
    },
    assembly / test := {},
  )

// OWASP dependency check
import net.nmoncho.sbt.dependencycheck.settings._
dependencyCheckFailBuildOnCVSS := 7  // fail on high + critical (7+)
// Only scan compile + runtime scope — skip provided (Spark, Hadoop, and their
// transitive deps) and test deps since we don't ship them in our assembly JAR.
dependencyCheckScopes := ScopesSettings(
  compile  = true,
  optional = false,
  provided = false,
  runtime  = true,
  test     = false
)
dependencyCheckSuppressions := SuppressionSettings(
  files = SuppressionFilesSettings.files()(file("dependency-check-suppression.xml"))
)
dependencyCheckOutputDirectory := target.value / "dependency-check"
dependencyCheckFormats := {
  import org.owasp.dependencycheck.reporting.ReportGenerator.Format
  Seq(Format.HTML, Format.JSON)
}
dependencyCheckNvdApi := {
  import net.nmoncho.sbt.dependencycheck.settings.NvdApiSettings.DataFeed
  val key = sys.env.getOrElse("NVD_API_KEY", "")

  // Prefer the bulk data feed over the NVD API. Paging the API walks ~300k CVEs at
  // 2000 per request and inserts each one into the embedded H2 database — that build,
  // not the network, is what pushed scans past 3 hours and timed them out. The feed
  // is prebuilt gzipped per-year archives over plain HTTP (~25MB/year), needs no API
  // key and has no rate limit. {0} is substituted with the year.
  //
  // The API key still applies to incremental "modified" lookups; requestDelay only
  // matters on that path now.
  NvdApiSettings(
    apiKey = key,
    requestDelay = Some(java.time.Duration.ofSeconds(1)),
    dataFeed = DataFeed(
      // sbt's `url(...)` helper parses via URI and rejects the `{0}` placeholder,
      // so construct the URL directly.
      url = Some(new java.net.URL("https://nvd.nist.gov/feeds/json/cve/2.0/nvdcve-2.0-{0}.json.gz"))
    )
  )
}

addCommandAlias("build", "assembly")
