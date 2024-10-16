package org.flink.assignment.jobs.flinkKafka

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

/**
  * MessageTransformer is a ProcessFunction that transforms an InputMessage by adding a processing timestamp.
  */
class MessageTransformer extends ProcessFunction[InputMessage, InputMessage] {
  override def processElement(
      input: InputMessage,
      ctx: ProcessFunction[InputMessage, InputMessage]#Context,
      out: Collector[InputMessage]
  ): Unit = {
    val processingTime = ctx.timerService().currentProcessingTime()
    val transformedMessage = InputMessage(
      id = input.id,
      timestamp = input.timestamp,
      data = input.data,
      processingTimestamp = Some(processingTime)
    )
    out.collect(transformedMessage)
  }
}
