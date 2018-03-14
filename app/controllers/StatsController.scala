package controllers

import java.io.StringWriter
import java.util.concurrent.TimeUnit
import javax.inject._

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.{ObjectMapper, ObjectWriter}
import core.util.Instrumented
import play.api.libs.json.{JsObject, Json}
import play.api.mvc._
import hydra.build.BuildInfo
import play.api.Configuration

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

@Singleton
class StatsController @Inject() (@Named("subscription-manager") subMgr: ActorRef, @Named("expander-manager") expMgr: ActorRef, config: Configuration)(implicit ec: ExecutionContext) extends Controller with Instrumented {
  import actors.ManagerMessages._

  val mapper = new ObjectMapper()
  mapper.registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))

  def getMetrics = Action {
    val writer: ObjectWriter = mapper.writerWithDefaultPrettyPrinter()
    val stringWriter = new StringWriter()
    writer.writeValue(stringWriter, metricRegistry)
    Ok(stringWriter.toString).as("application/json").withHeaders("Cache-Control" -> "must-revalidate,no-cache,no-store")
  }

  implicit val timeout = Timeout(1.second)
  def getBuild = Action.async {
    val buildInfo = Json.parse(BuildInfo.toJson).as[JsObject]
    val readOnlyMode = config.getBoolean("hydra.readOnlyMode") match {
      case Some(true) => Json.obj("readOnlyMode" -> true)
      case _ => Json.obj()
    }

    def response(status: Int, extraInfo: JsObject) : Result =
      Status(status)(buildInfo ++ readOnlyMode ++ extraInfo).withHeaders("Cache-Control" -> "must-revalidate,no-cache,no-store")

    Future.sequence(Seq(
      (subMgr ? CheckLiveliness).mapTo[Liveliness],
      (expMgr ? CheckLiveliness).mapTo[Liveliness]
    )).map {
      case Seq(
        Liveliness(subLiveliness),
        Liveliness(expLiveliness)
      ) =>
        response(
          if (subLiveliness && expLiveliness) OK else INTERNAL_SERVER_ERROR,
          Json.obj("liveliness" -> Json.obj(
            "subscription" -> subLiveliness,
            "expander" -> expLiveliness
          ))
        )
    }.recover {
      case _ => response(INTERNAL_SERVER_ERROR, Json.obj("liveliness" -> "unknown"))
    }
  }
}
