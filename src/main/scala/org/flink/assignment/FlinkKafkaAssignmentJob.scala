package org.flink.assignment

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.flink.assignment.jobs.FlinkJobConfig
import org.flink.assignment.jobs.flinkKafka.{ FlinkKafkaJob, FlinkKafkaJobConfig }
import org.slf4j.LoggerFactory

object FlinkKafkaAssignmentJob extends App with FlinkJobConfig {
  private val log = LoggerFactory.getLogger(getClass)

  private val topology: String =
    Option(System.getenv("TOPOLOGY")).getOrElse("FlinkKafkaJob")

  private val flinkEnv = {
    val conf = new Configuration()
    conf.setString("taskmanager.memory.network.max", "256m")
    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  }

  topology match {
    case "FlinkKafkaJob" =>
      val config = FlinkKafkaJobConfig.toConfig(
        flinkEnv.getParallelism,
        "flink-kafka-job.properties"
      )
      FlinkKafkaJob.build(config, flinkEnv)

    case _ => log.error(s"Unrecognised SIGMA topology - $topology")
  }

  flinkEnv.execute(topology)
}
