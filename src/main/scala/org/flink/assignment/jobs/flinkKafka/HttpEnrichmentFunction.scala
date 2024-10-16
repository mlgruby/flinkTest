package org.flink.assignment.jobs.flinkKafka

import org.apache.flink.streaming.api.functions.async.{ AsyncFunction, ResultFuture }
import org.slf4j.{ Logger, LoggerFactory }
import scalaj.http._

import java.util.{ Collections => JCollections }
import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

/**
  * HttpEnrichmentFunction is an AsyncFunction that enriches a TransformedMessage by making an HTTP request.
  *
  * @param apiUrl The URL to which the HTTP request is made.
  * @param maxRetries The maximum number of times to retry the HTTP request.
  */
class HttpEnrichmentFunction(apiUrl: String, maxRetries: Int = 3) extends AsyncFunction[InputMessage, InputMessage] {

  lazy val log: Logger              = LoggerFactory.getLogger(getClass)
  private val BACKOFF_INITIAL: Long = 1000 // 1 second

  @tailrec
  private def retryWithBackoff(retriesLeft: Int, backoff: Long)(
      f: => Try[InputMessage]
  ): Try[InputMessage] = {
    f match {
      case Success(result) => Success(result)
      case Failure(exception) if retriesLeft > 0 =>
        log.warn(s"Retrying after error: ${exception.getMessage}")
        Thread.sleep(backoff)
        retryWithBackoff(retriesLeft - 1, backoff * 2)(f)
      case Failure(exception) => Failure(exception)
    }
  }

  override def asyncInvoke(input: InputMessage, resultFuture: ResultFuture[InputMessage]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global

    Future {
      retryWithBackoff(maxRetries, BACKOFF_INITIAL) { // Start with 1 second backoff
        Try {
          val response: HttpResponse[String] = Http(s"$apiUrl?user_id=${input.id}").asString
          if (response.isSuccess) {
            // Enrich the message with the response body
            input.copy(enrichmentData = Some(response.body))
          } else {
            throw new Exception(s"HTTP request failed with status ${response.code}")
          }
        }
      }
    }.onComplete {
      case Success(tryResult) =>
        val result = tryResult.getOrElse {
          log.error("All retry attempts failed. Using original message.")
          input
        }
        resultFuture.complete(JCollections.singletonList(result))
      case Failure(exception) =>
        log.error("Failed to enrich message", exception)
        resultFuture.complete(JCollections.singletonList(input))
    }
  }
}
