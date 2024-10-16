package org.flink.assignment.jobs.flinkKafka

import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class MessageTransformerSuite extends AnyFlatSpec with Matchers {

  "MessageTransformer" should "add processing timestamp to InputMessage" in {

    val ID        = "1"
    val TIMESTAMP = 1000L

    val PROCESSING_TIMESTAMP = 2000L
    val PROCESSING_TIME      = 1000L

    // Create an instance of your MessageTransformer
    val messageTransformer = new MessageTransformer()

    // Create a test harness for the MessageTransformer
    val testHarness = ProcessFunctionTestHarnesses.forProcessFunction(messageTransformer)

    // Create a sample InputMessage
    val inputMessage = InputMessage(ID, TIMESTAMP, "test data")

    // Set the processing time to a known value
    testHarness.setProcessingTime(PROCESSING_TIMESTAMP)

    // Process the input message
    testHarness.processElement(inputMessage, PROCESSING_TIME)

    // Retrieve the output from the test harness
    val output = testHarness.extractOutputValues()

    // Verify the output
    output should have size 1
    val transformedMessage = output.get(0)
    transformedMessage.id should be("1")
    transformedMessage.timestamp should be(TIMESTAMP)
    transformedMessage.data should be("test data")
    transformedMessage.processingTimestamp should be(Some(PROCESSING_TIMESTAMP))
  }
}
