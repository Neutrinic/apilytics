lazy val root = (project in file("."))
  .settings(
    name := "apilytics",
    organization := "com.apilytics",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.13.16",
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
    assembly / assemblyJarName := s"apilytics-${version.value}.jar",
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

addCommandAlias("build", "assembly")