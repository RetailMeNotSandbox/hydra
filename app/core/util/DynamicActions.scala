package core.util

import javax.inject.Inject

import com.google.inject.ImplementedBy
import controllers.ValidationTypes
import play.api.Configuration
import play.api.http.{Status, Writeable}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc._

import scala.collection.JavaConverters._
import scala.concurrent.Future


@ImplementedBy(classOf[ConfigDynamicActions])
abstract class DynamicActions() {
  def isKeyAllowed(key: String) : Boolean
  def isReadOnlyMode : Boolean

  object AuthenticatedAction extends ActionBuilder[Request] {
    def invokeBlock[A](request: Request[A], block: (Request[A]) => Future[Result]) = {
      if (request.headers.get("key").exists(isKeyAllowed))
        block(request)
      else
        Future.successful(Results.Forbidden)
    }
  }
  protected object ReadOnlyMode extends ActionFilter[Request] {
    protected def filter[A](request: Request[A]): Future[Option[Result]] = Future.successful(
      if (isReadOnlyMode) {
        Some(Result(
          ResponseHeader(Status.SERVICE_UNAVAILABLE),
          implicitly[Writeable[JsValue]].toEntity(Json.toJson(
            ValidationTypes.ClientErrorResponse(Status.SERVICE_UNAVAILABLE, "Hydra is in read-only mode", "Read Only")
          ))
        ))
      } else {
        None
      }
    )
  }

  val WriteAuthAction = AuthenticatedAction andThen ReadOnlyMode
}

class ConfigDynamicActions @Inject()(config: Configuration) extends DynamicActions {
  val allowed : Set[String] = config.getStringList("hydra.apiKeys").map(_.asScala.toSet).getOrElse(Set.empty)
  val readOnly: Boolean = config.getBoolean("hydra.readOnlyMode").get
  def isKeyAllowed(key: String) = allowed.contains(key)
  def isReadOnlyMode: Boolean = readOnly
}

class BlindDynamicActions extends DynamicActions {
  def isKeyAllowed(key: String): Boolean = true
  def isReadOnlyMode: Boolean = false
}
