name := "apilytics"
organization := "com.apilytics"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.17"

val sparkVersion = "4.0.0"
val http4sVersion = "0.23.27"
val catsEffectVersion = "3.5.4"
val jsoniterVersion = "2.30.1"

libraryDependencies ++= Seq(
  // Spark (provided - users bring their own)
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",

  // HTTP Client
  "org.http4s" %% "http4s-ember-client" % http4sVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,

  // Cats Effect
  "org.typelevel" %% "cats-effect" % catsEffectVersion,

  // JSON (we'll start with jsoniter-scala)
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % jsoniterVersion,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "provided",

  // OpenAPI parsing
  "io.swagger.parser.v3" % "swagger-parser" % "2.1.22",

  // Arrow (we'll add this after basics work)
  // "org.apache.arrow" % "arrow-vector" % "15.0.0",
  // "org.apache.arrow" % "arrow-memory-netty" % "15.0.0",

  // Delta Lake (we'll add this for caching later)
  // "io.delta" %% "delta-spark" % "3.2.0",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
)

// Compiler options
scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

// Assembly settings (for building fat JAR later)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}