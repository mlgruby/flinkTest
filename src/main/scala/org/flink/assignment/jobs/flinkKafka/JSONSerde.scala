package org.flink.assignment.jobs.flinkKafka

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
import org.json4s._
import org.slf4j.{ Logger, LoggerFactory }

import java.lang
import java.nio.charset.StandardCharsets

/**
  * JSONSerde is a serialization and deserialization schema for Kafka that handles
  * converting InputMessage objects to and from JSON format.
  *
  * @param topic The Kafka topic to which messages are produced.
  */
class JSONSerde(topic: String)
    extends KafkaDeserializationSchema[InputMessage]
    with KafkaRecordSerializationSchema[InputMessage] {

  implicit val formats: DefaultFormats.type = DefaultFormats
  lazy val log: Logger                      = LoggerFactory.getLogger(getClass)

  /**
    * Provides the TypeInformation for the InputMessage type used in Flink.
    *
    * @return TypeInformation for InputMessage.
    */
  override def getProducedType: TypeInformation[InputMessage] =
    TypeInformation.of(classOf[InputMessage])

  /**
    * Serializes an InputMessage to a ProducerRecord for Kafka.
    *
    * @param element The InputMessage to serialize.
    * @param context The KafkaSinkContext providing context for serialization.
    * @param timestamp The timestamp for the record.
    * @return A ProducerRecord containing the serialized message.
    */
  override def serialize(
      element: InputMessage,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] = {
    try {
      implicit val formats: DefaultFormats.type = DefaultFormats

      val valueBytes = write(element).getBytes(StandardCharsets.UTF_8)
      new ProducerRecord[Array[Byte], Array[Byte]](topic, valueBytes)
    } catch {
      case e: Exception =>
        log.error(s"Failed to serialize message: $element", e)
        throw e
    }
  }

  /**
    * Deserializes a ConsumerRecord from Kafka into an InputMessage.
    *
    * @param record The ConsumerRecord to deserialize.
    * @return The deserialized InputMessage.
    */
  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]]
  ): InputMessage = {
    try {
      val value = new String(record.value(), StandardCharsets.UTF_8)

      parse(value).extract[InputMessage]
    } catch {
      case e: Exception =>
        log.error(s"Failed to deserialize message: $record", e)
        throw e
    }
  }

  /**
    * Indicates whether the stream has ended.
    *
    * @param nextElement The next InputMessage element.
    * @return false, as the stream is never-ending in this context.
    */
  override def isEndOfStream(nextElement: InputMessage): Boolean = false

}

/**
  * Case class representing the structure of messages consumed and produced by Kafka.
  *
  * @param id The ID of the message.
  * @param timestamp The timestamp of when the message was created.
  * @param data The data content of the message.
  * @param processingTimestamp The processing timestamp of the message.
  * @param enrichmentData Optional enrichment data for the message.
  */
case class InputMessage(
    id: String,
    timestamp: Long,
    data: String,
    processingTimestamp: Option[Long] = None,
    enrichmentData: Option[String] = None
)
