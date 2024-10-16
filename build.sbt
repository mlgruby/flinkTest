ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.10"

val flinkVersion      = "1.15.2"
val flinkKafkaVersion = "1.15.2"
val slf4jVersion      = "2.0.7"

lazy val root = (project in file("."))
  .settings(
    name := "flink_assignment",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-runtime-web"     % flinkVersion,
      "org.apache.flink" % "flink-core"            % flinkVersion,
      "org.apache.flink" % "flink-streaming-java"  % flinkVersion,
      "org.apache.flink" % "flink-connector-kafka" % flinkKafkaVersion,
      "org.apache.flink" % "flink-connector-base"  % flinkVersion,
      "org.slf4j"        % "slf4j-api"             % slf4jVersion,
      "org.slf4j"        % "slf4j-jdk14"           % slf4jVersion,
      "com.typesafe" % "config"         % "1.4.3",
      "org.json4s"   %% "json4s-native" % "4.0.7",
      "org.scalaj"   %% "scalaj-http"   % "2.4.2",
      // Test dependencies
      "org.scalatest"    %% "scalatest"                 % "3.2.19"     % Test,
      "org.scalamock"    %% "scalamock"                 % "6.0.0"      % Test,
      "org.apache.flink" % "flink-test-utils"           % flinkVersion % Test,
      "org.apache.flink" % "flink-runtime"              % flinkVersion % Test,
      "org.apache.flink" % "flink-connector-test-utils" % flinkVersion % Test,
      "org.apache.flink" % "flink-streaming-java"       % flinkVersion % Test classifier "tests"
    ),
    assembly / mainClass := Some("org.flink.assignment.FlinkKafkaAssignmentJob"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf"            => MergeStrategy.concat
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    // Exclude Scala library from the assembled JAR
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
    // Scalafix settings
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    // Scalafmt settings
    scalafmtOnCompile := true,
    // Scoverage settings
    coverageEnabled := true,
    coverageMinimumStmtTotal := 70,
    coverageFailOnMinimum := false,
    // Add JVM arguments for tests
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
  )

// Scalafix task
addCommandAlias("fmt", "scalafmt; Test / scalafmt; sFix;")
addCommandAlias("fmtCheck", "scalafmtCheck; Test / scalafmtCheck; sFixCheck")
addCommandAlias("sFix", "scalafix OrganizeImports; Test / scalafix OrganizeImports")
addCommandAlias(
  "sFixCheck",
  "scalafix --check OrganizeImports; Test / scalafix --check OrganizeImports"
)
