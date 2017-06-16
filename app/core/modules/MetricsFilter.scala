package core.modules

import javax.inject._

import akka.stream.Materializer
import core.util.Metrics
import play.api.http.Status
import com.codahale.metrics._
import com.codahale.metrics.MetricRegistry.name
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class MetricsFilter @Inject() (implicit val ec: ExecutionContext, val mat: Materializer) extends Filter {
  /** Specify a meaningful prefix for metrics
    *
    * Defaults to classOf[MetricsFilter].getName for backward compatibility as
    * this was the original set value.
    *
    */
  def labelPrefix: String = classOf[MetricsFilter].getName

  /** Specify which HTTP status codes have individual metrics
    *
    * Statuses not specified here are grouped together under otherStatuses
    *
    * Defaults to 200, 400, 401, 403, 404, 409, 201, 304, 307, 500, which is compatible
    * with prior releases.
    */
  def knownStatuses = Seq(Status.OK, Status.BAD_REQUEST, Status.FORBIDDEN, Status.NOT_FOUND,
    Status.CREATED, Status.TEMPORARY_REDIRECT, Status.INTERNAL_SERVER_ERROR, Status.CONFLICT,
    Status.UNAUTHORIZED, Status.NOT_MODIFIED)


  def statusCodes: Map[Int, Meter] = knownStatuses.map(s => s -> Metrics.metricRegistry.meter(name(labelPrefix, s.toString))).toMap

  def requestsTimer: Timer = Metrics.metricRegistry.timer(name(labelPrefix, "requestTimer"))
  def activeRequests: Counter = Metrics.metricRegistry.counter(name(labelPrefix, "activeRequests"))
  def otherStatuses: Meter = Metrics.metricRegistry.meter(name(labelPrefix, "other"))

  def apply(nextFilter: (RequestHeader) => Future[Result])
           (rh: RequestHeader): Future[Result] = {
    val context = requestsTimer.time()

    def logCompleted(result: Result): Unit = {
      activeRequests.dec()
      context.stop()
      statusCodes.getOrElse(result.header.status, otherStatuses).mark()
    }

    activeRequests.inc()
    nextFilter(rh).transform(
      result => {
        logCompleted(result)
        result
      },
      exception => {
        logCompleted(Results.InternalServerError)
        exception
      }
    )
  }

}
