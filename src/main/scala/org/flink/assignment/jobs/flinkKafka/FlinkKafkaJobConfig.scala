package org.flink.assignment.jobs.flinkKafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.flink.assignment.config.ConfigManager
import org.flink.assignment.jobs.FlinkJobConfig

import java.util.Properties

/**
  * Configuration class for Flink Kafka jobs, which includes Kafka source and sink properties.
  *
  * @param streamExecutionEnvironmentParallelism The parallelism level for the Flink execution environment.
  * @param sourceKafkaProperties Configuration properties for the Kafka source.
  * @param sinkKafkaProperties Configuration properties for the Kafka sink.
  */
case class FlinkKafkaJobConfig(
    streamExecutionEnvironmentParallelism: Int,
    sourceKafkaProperties: SourceKafkaProperties,
    sinkKafkaProperties: SinkKafkaProperties,
    debouncingProperties: DebouncingProperties,
    transformationParallelism: Int,
    enrichmentProperties: EnrichmentProperties
) extends FlinkJobConfig

/**
  * Configuration properties for Kafka source.
  *
  * @param kafkaTopic The Kafka topic to read from.
  * @param kafkaProperties The properties for configuring the Kafka consumer.
  * @param sourceParallelism The parallelism level for the Kafka source.
  */
case class SourceKafkaProperties(
    kafkaTopic: String,
    kafkaProperties: Properties,
    sourceParallelism: Int
)

/**
  * Configuration properties for Kafka sink.
  *
  * @param kafkaTopic The Kafka topic to write to.
  * @param kafkaSinkProperties The properties for configuring the Kafka producer.
  * @param sinkParallelism The parallelism level for the Kafka sink.
  */
case class SinkKafkaProperties(
    kafkaTopic: String,
    kafkaSinkProperties: Properties,
    sinkParallelism: Int
)

/**
  * Configuration properties for debouncing.
  *
  * @param debouncingWindowSizeInSeconds The window size for debouncing in seconds.
  * @param debouncingParallelism The parallelism level for debouncing.
  */
case class DebouncingProperties(
    debouncingWindowSizeInSeconds: Long,
    debouncingParallelism: Int
)

/**
  * Configuration properties for enrichment.
  *
  * @param enrichmentParallelism The parallelism level for enrichment.
  * @param apiUrl The URL for the enrichment API.
  * @param timeoutMs The timeout in milliseconds for the enrichment API.
  * @param capacity The capacity for the enrichment API.
  * @param maxRetryAttempts The maximum number of retry attempts for the enrichment API.
  */
case class EnrichmentProperties(
    enrichmentParallelism: Int,
    apiUrl: String,
    timeoutMs: Long,
    capacity: Int,
    maxRetryAttempts: Int
)

/**
  * Object to generate FlinkKafkaJobConfig from provided configuration properties.
  */
object FlinkKafkaJobConfig {

  /**
    * Generates a FlinkKafkaJobConfig object from configuration properties.
    *
    * @param streamExecutionEnvironmentParallelism The parallelism level for the Flink execution environment.
    * @param configPropsName The name of the configuration file containing Kafka properties.
    * @return A FlinkKafkaJobConfig object with populated properties.
    */
  def toConfig(
      streamExecutionEnvironmentParallelism: Int,
      configPropsName: String
  ): FlinkKafkaJobConfig = {

    ConfigManager.loadConfig(configPropsName)

    val kafkaSourceProperties = new Properties()

    // Set the properties for the Kafka consumer
    kafkaSourceProperties.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      ConfigManager.getProperty("kafka.bootstrap.servers")
    )

    kafkaSourceProperties.put(
      ConsumerConfig.GROUP_ID_CONFIG,
      ConfigManager.getProperty("source.kafka.consumer.group.id")
    )

    kafkaSourceProperties.put(
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
      ConfigManager.getProperty("source.kafka.consumer.auto.offset.reset")
    )

    val sourceKafkaProperties = SourceKafkaProperties(
      kafkaProperties = kafkaSourceProperties,
      kafkaTopic = ConfigManager.getProperty("source.kafka.topic"),
      sourceParallelism = ConfigManager.getProperty("source.kafka.parallelism").toInt
    )

    // Set kafka sink properties
    val kafkaSinkProperties = new Properties()

    kafkaSinkProperties.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      ConfigManager.getProperty("kafka.bootstrap.servers")
    )

    kafkaSinkProperties.put(
      ProducerConfig.ACKS_CONFIG,
      "all"
    )

    val sinkKafkaProperties = SinkKafkaProperties(
      kafkaSinkProperties = kafkaSinkProperties,
      kafkaTopic = ConfigManager.getProperty("sink.kafka.topic"),
      sinkParallelism = ConfigManager.getProperty("sink.kafka.parallelism").toInt
    )

    val debouncingProperties = DebouncingProperties(
      debouncingWindowSizeInSeconds = ConfigManager.getProperty("debouncing.window.size.seconds").toLong,
      debouncingParallelism = ConfigManager.getProperty("debouncing.parallelism").toInt
    )

    val enrichmentProperties = EnrichmentProperties(
      enrichmentParallelism = ConfigManager.getProperty("enrichment.parallelism").toInt,
      apiUrl = ConfigManager.getProperty("enrichment.api.url"),
      timeoutMs = ConfigManager.getProperty("enrichment.timeout.ms").toLong,
      capacity = ConfigManager.getProperty("enrichment.capacity").toInt,
      maxRetryAttempts = ConfigManager.getProperty("enrichment.max.retries").toInt
    )

    FlinkKafkaJobConfig(
      streamExecutionEnvironmentParallelism = streamExecutionEnvironmentParallelism,
      sourceKafkaProperties = sourceKafkaProperties,
      sinkKafkaProperties = sinkKafkaProperties,
      debouncingProperties = debouncingProperties,
      transformationParallelism = ConfigManager.getProperty("transformation.parallelism").toInt,
      enrichmentProperties = enrichmentProperties
    )
  }
}
