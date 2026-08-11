ThisBuild / organization := "io.github.neutrinic"
ThisBuild / version := "0.8.0"
// Must be >= the scala-library the dependency classpath pulls in (SIP-51):
// the bumped deps bring 2.13.18, and the compiler cannot be older than that.
ThisBuild / scalaVersion := "2.13.18"

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

lazy val root = (project in file("."))
  .settings(
    name := "apilytics",
    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-sql"          % "4.1.3" % "provided",
      "org.apache.spark" %% "spark-catalyst"      % "4.1.3" % "provided",

      // HTTP + JSON
      "org.http4s"       %% "http4s-ember-client" % "0.23.36",
      "org.http4s"       %% "http4s-circe"        % "0.23.36",
      "io.circe"         %% "circe-core"          % "0.14.16",
      "io.circe"         %% "circe-generic"       % "0.14.16",
      "io.circe"         %% "circe-parser"        % "0.14.16",
      "io.circe"         %% "circe-pointer"       % "0.14.16",

      // OpenAPI. 2.1.45 is what fixes #188: its chain carries jackson-databind
      // 2.22.0 and rhino 1.7.15.1, both free of the CVEs that forced the old pins,
      // so consumers inherit safe versions without us publishing any constraint.
      "io.swagger.parser.v3" % "swagger-parser"   % "2.1.45",

      // Arrow — keep in lockstep with the version Spark bundles, since
      // ArrowColumnVector hands our buffers straight to Spark's own Arrow.
      "org.apache.arrow"  % "arrow-vector"        % "18.3.0",
      "org.apache.arrow"  % "arrow-memory-netty"  % "18.3.0",

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
    // swagger-parser's chain wants jackson-databind 2.22.0, which is outside the
    // [2.21.0, 2.22.0) window Spark's jackson-module-scala enforces (#185). Our
    // assembly bundles jackson, so it has to sit inside that window.
    //
    // Consumers are a different case and need no constraint from us: 2.22.0 carries
    // the CVE fixes, and on a cluster Spark's own jackson-databind takes precedence
    // anyway. Forcing 2.21.5 on them is not achievable cleanly regardless — sbt
    // resolves highest-wins, so a published lower bound would simply lose.
    // JacksonCompatibilitySuite fails the build if this drifts out of range.
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.21.5",

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
  val key = sys.env.getOrElse("NVD_API_KEY", "")
  if (key.nonEmpty) NvdApiSettings(apiKey = key, requestDelay = Some(java.time.Duration.ofSeconds(4)))
  else NvdApiSettings()
}

addCommandAlias("build", "assembly")
