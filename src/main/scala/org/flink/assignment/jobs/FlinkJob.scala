package org.flink.assignment.jobs

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.environment.{ CheckpointConfig, StreamExecutionEnvironment }
import org.slf4j.LoggerFactory

import java.util.Properties

trait FlinkJob[C <: FlinkJobConfig] {

  private lazy val log = LoggerFactory.getLogger(getClass)

  /**
    * Build the flink job within the execution environment.
    * @param config The topology config needed
    * @param env StreamExecutionEnvironment provided by the Flink runtime.
    */
  final def build(config: C, env: StreamExecutionEnvironment): Unit = {
    configFlink(config, env)
    defineJob(config, env)
    log.info(flinkEnvConfigSerializedForLogging(env))
  }

  /* Environment config as human-readable string (for logging) */
  private def flinkEnvConfigSerializedForLogging(env: StreamExecutionEnvironment): String = {
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    val checkpointingConfigString: String =
      s"""Mode:${checkpointConfig.getCheckpointingMode}
         | Checkpointing Interval:${checkpointConfig.getCheckpointInterval}
         | getCheckpointTimeout:${checkpointConfig.getCheckpointTimeout}
         | getMaxConcurrentCheckpoints:${checkpointConfig.getMaxConcurrentCheckpoints}
         | Min Pause Between Checkpoints:${checkpointConfig.getMinPauseBetweenCheckpoints}
         | """.stripMargin

    val executionConfig: ExecutionConfig = env.getConfig
    val executionConfigString =
      s"""Mode:${executionConfig.getExecutionMode}
         | AutoWaterMark Interval:${executionConfig.getAutoWatermarkInterval}
         | LatencyTracking Interval:${executionConfig.getLatencyTrackingInterval}
         | """.stripMargin

    s"""
       | Parallelism [Max=${env.getMaxParallelism}, Set=${env.getParallelism}]
       | Time Characteristic ${env.getStreamTimeCharacteristic}
       | Env Config $executionConfigString
       | Checkpointing Config $checkpointingConfigString
       | Sources Config
      """.stripMargin
  }

  /* The placeholder to init any relevant Flink env setting as checkpointing etc */
  def configFlink(config: C, env: StreamExecutionEnvironment): Unit

  /* This is where flink job should define its specific pipelines*/
  def defineJob(config: C, env: StreamExecutionEnvironment): Unit

  /**
    * Get a property from a properties object or throw an exception with a failure message.
    * @param properties The properties object from which to get the property.
    * @param key The key of the property in the properties object.
    * @param failureMessage The message to include in the exception if the property is not found.
    * @return The property value.
    * @throws Exception If the property is not found.
    */
  def property(properties: Properties, key: String, failureMessage: String): AnyRef = {
    Option(properties.getProperty(key))
      .getOrElse(throw new Exception(s"Could not find property = '$key': $failureMessage"))
  }
}

/**
  * The base config for Flink jobs.
  * */
trait FlinkJobConfig
