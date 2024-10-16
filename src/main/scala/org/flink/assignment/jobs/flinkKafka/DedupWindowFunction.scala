package org.flink.assignment.jobs.flinkKafka
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.jdk.CollectionConverters._

/**
  * DedupWindowFunction is a ProcessWindowFunction that deduplicates messages by selecting the latest message for each ID.
  */
class DedupWindowFunction extends ProcessWindowFunction[InputMessage, InputMessage, String, TimeWindow] {

  override def process(
      key: String,
      context: ProcessWindowFunction[InputMessage, InputMessage, String, TimeWindow]#Context,
      elements: lang.Iterable[InputMessage],
      out: Collector[InputMessage]
  ): Unit = {
    // Convert Java Iterable to Scala Iterable
    val scalaElements = elements.asScala

    // Group elements by their ID and select the latest message for each ID
    val dedupedElements = scalaElements
      .groupBy(_.id)
      .map {
        case (_, messages) =>
          messages.maxBy(_.timestamp)
      }

    // Emit the deduplicated messages
    dedupedElements.foreach(out.collect)
  }
}
