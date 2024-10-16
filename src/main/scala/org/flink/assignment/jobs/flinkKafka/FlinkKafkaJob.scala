package org.flink.assignment.jobs.flinkKafka

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{ AsyncDataStream, SingleOutputStreamOperator }
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.flink.assignment.jobs.FlinkJob
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.duration.DurationInt

/**
  * FlinkKafkaJob is a Flink job that reads data from a Kafka topic using a KafkaSource.
  * It configures Flink's environment settings and defines the data processing pipeline.
  */
object FlinkKafkaJob extends FlinkJob[FlinkKafkaJobConfig] {

  lazy val log: Logger                 = LoggerFactory.getLogger(getClass)
  private lazy val MaxParallelism: Int = 256

  /**
    * Defines the data processing pipeline for the job.
    *
    * @param config The configuration settings for the Flink job.
    * @param env The Flink StreamExecutionEnvironment.
    */
  override def defineJob(config: FlinkKafkaJobConfig, env: StreamExecutionEnvironment): Unit = {

    // Configure Kafka source with custom JSON deserialization schema
    val kafkaSource = KafkaSource
      .builder[InputMessage]()
      .setTopics(config.sourceKafkaProperties.kafkaTopic)
      .setProperties(config.sourceKafkaProperties.kafkaProperties)
      .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONSerde(config.sourceKafkaProperties.kafkaTopic)))
      .build()

    // Create a DataStream using the KafkaSource
    val inputStream: SingleOutputStreamOperator[InputMessage] = env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks[InputMessage](), "Kafka Source")
      .setParallelism(config.sourceKafkaProperties.sourceParallelism)
      .name("kafka-source")
      .uid("kafka-source")
      .disableChaining()

    val debouncedStream: SingleOutputStreamOperator[InputMessage] = inputStream
      .keyBy(new IdKeySelector)
      .window(
        TumblingProcessingTimeWindows.of(Time.seconds(config.debouncingProperties.debouncingWindowSizeInSeconds))
      )
      .process(new DedupWindowFunction())
      .setParallelism(config.debouncingProperties.debouncingParallelism)
      .name("windowed-stream-with-debounce-and-dedup")
      .uid("windowed-stream-with-debounce-and-dedup")
      .disableChaining()

    val transformedStream: SingleOutputStreamOperator[InputMessage] = debouncedStream
      .process(new MessageTransformer)
      .setParallelism(config.transformationParallelism)
      .name("message-transformer")
      .uid("message-transformer")
      .disableChaining()

    val enrichedStream: SingleOutputStreamOperator[InputMessage] = AsyncDataStream
      .unorderedWait(
        transformedStream,
        new HttpEnrichmentFunction(config.enrichmentProperties.apiUrl),
        config.enrichmentProperties.timeoutMs,
        java.util.concurrent.TimeUnit.MILLISECONDS,
        config.enrichmentProperties.capacity
      )
      .setParallelism(config.enrichmentProperties.enrichmentParallelism)
      .name("http-enrichment")
      .uid("http-enrichment")
      .disableChaining()

    val kafkaSink = KafkaSink
      .builder[InputMessage]()
      .setKafkaProducerConfig(config.sinkKafkaProperties.kafkaSinkProperties)
      .setRecordSerializer(new JSONSerde(config.sinkKafkaProperties.kafkaTopic))
      .build()

    enrichedStream
      .sinkTo(kafkaSink)
      .setParallelism(config.sinkKafkaProperties.sinkParallelism)
      .name("kafka-sink")
      .uid("kafka-sink")
      .disableChaining()
  }

  /**
    * Configures the Flink environment for the job.
    *
    * @param config The configuration settings for the Flink job.
    * @param env The Flink StreamExecutionEnvironment.
    */
  override def configFlink(config: FlinkKafkaJobConfig, env: StreamExecutionEnvironment): Unit = {

    env.enableCheckpointing(1.seconds.toMillis, CheckpointingMode.AT_LEAST_ONCE)
    env.setMaxParallelism(MaxParallelism)
    env.getCheckpointConfig.setCheckpointInterval(1.minute.toMillis)
  }

  /* A KeySelector is an interface that needs to implement a single method getKey, which extracts a key from the input
   * object.This is used with KeyBy operator to partition the stream of events based on the key extracted from the
   * event. In this case, we are extracting the appsflyerId from the AppsflyerEvent and using it as the key to route
   * events with the same appsflyerId to the same instance of the KeyedProcessFunction.
   */
  private class IdKeySelector extends KeySelector[InputMessage, String] {
    def getKey(value: InputMessage): String = {
      value.id
    }
  }
}
