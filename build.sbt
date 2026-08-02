ThisBuild / organization := "io.github.neutrinic"
ThisBuild / version := "0.8.0"
ThisBuild / scalaVersion := "2.13.16"

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
      "org.apache.spark" %% "spark-sql"          % "4.0.0" % "provided",
      "org.apache.spark" %% "spark-catalyst"      % "4.0.0" % "provided",

      // HTTP + JSON
      "org.http4s"       %% "http4s-ember-client" % "0.23.30",
      "org.http4s"       %% "http4s-circe"        % "0.23.30",
      "io.circe"         %% "circe-core"          % "0.14.10",
      "io.circe"         %% "circe-generic"       % "0.14.10",
      "io.circe"         %% "circe-parser"        % "0.14.10",
      "io.circe"         %% "circe-pointer"       % "0.14.10",

      // OpenAPI
      "io.swagger.parser.v3" % "swagger-parser"   % "2.1.22",

      // Arrow
      "org.apache.arrow"  % "arrow-vector"        % "18.1.0",
      "org.apache.arrow"  % "arrow-memory-netty"  % "18.1.0",

      // Config
      "com.typesafe"      % "config"              % "1.4.3",

      // Streaming
      "co.fs2"           %% "fs2-core"            % "3.11.0",

      // Test
      "org.scalameta"    %% "munit"               % "1.0.3"  % Test,
      "org.typelevel"    %% "munit-cats-effect"   % "2.0.0"  % Test,
      "org.wiremock"      % "wiremock"            % "3.10.0" % Test,
    ),

    // Security bumps for transitive deps pulled in by swagger-parser.
    dependencyOverrides ++= Seq(
      // CVE-2026-54512/54513/54514/54515 (polymorphic type validator bypasses,
      // eager DNS resolution). Fixed in 2.18.8 / 2.18.9 — stay on the 2.18 line
      // to match the Jackson that Spark 4.0.0 provides at runtime.
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.9",
      // CVE-2025-66453 (DoS via Number.toFixed on crafted floats), reached
      // through json-schema-validator's ECMA-262 regex format checker.
      // Fixed in 1.7.14.1 / 1.7.15.1 / 1.8.1.
      "org.mozilla" % "rhino" % "1.7.15.1",
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
  val key = sys.env.getOrElse("NVD_API_KEY", "")
  if (key.nonEmpty) NvdApiSettings(apiKey = key, requestDelay = Some(java.time.Duration.ofSeconds(4)))
  else NvdApiSettings()
}

addCommandAlias("build", "assembly")
