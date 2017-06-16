package controllers

import core.util.BlindDynamicActions
import dal.DocumentRepository
import com.rmn.jsonapi.models.ResourceIdentifier
import org.specs2.mutable.Specification
import play.api.http.{HeaderNames, HttpVerbs, Status}
import play.api.libs.json.{JsValue, Json}
import play.api.test.{DefaultAwaitTimeout, FakeRequest, ResultExtractors, Writeables}
import testutil.CallingThreadExecutionContext

import scala.concurrent.Future

class DocumentControllerSpec extends Specification
  with HeaderNames
  with Status
  with HttpVerbs
  with ResultExtractors
  with DefaultAwaitTimeout
  with Writeables {

  class TestDocumentRepository extends DocumentRepository {
    var nextResource : Future[Option[JsValue]] = null
    var nextResources : Future[JsValue] = null
    def resourceToDocument(id: ResourceIdentifier): Future[Option[JsValue]] = nextResource
    def resourcesToDocument(ids: Seq[ResourceIdentifier]): Future[JsValue] = nextResources
  }
  val auth = new BlindDynamicActions

  "DocumentController#get" should {
    "return 200 OK with item json when found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestDocumentRepository
      val controller = new DocumentController(repo, auth)

      repo.nextResource = Future.successful(Some(Json.obj("foo" -> "bar1")))

      val t = controller.get("foo", "bar1")(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 200
      //Must use utf-8 charset in content type
      contentType(t).getOrElse("failed") mustEqual "application/json"
      charset(t).getOrElse("failed") mustEqual "utf-8"
      contentAsJson(t) mustEqual Json.obj(
        "foo" -> "bar1"
      )
    }

    "return 404 Not Found when the item is not found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestDocumentRepository
      val controller = new DocumentController(repo, auth)

      repo.nextResource = Future.successful(None)

      val t = controller.get("foo", "bar1")(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 404
      contentAsJson(t).asOpt[controller.NotFoundErrorResponse].isDefined mustEqual true
    }
  }

  "DocumentController#multiGet" should {
    "return 200 OK with the document built in postgres" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestDocumentRepository
      val controller = new DocumentController(repo, auth)

      repo.nextResources = Future.successful(Json.obj("foo" -> "bar"))

      val t = controller.multiGet("foo", List("bar1", "bar2", "bar3", "bar4"))(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 200
      contentAsJson(t) mustEqual Json.obj(
        "foo" -> "bar"
      )
    }
  }
}
