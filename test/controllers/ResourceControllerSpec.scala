package controllers

import controllers.ResourceController._
import core.util.BlindDynamicActions
import dal.ResourceRepository
import dal.ResourceRepository.SaveResult
import com.rmn.jsonapi.models.ResourceIdentifier
import org.specs2.mutable.Specification
import play.api.http.{HeaderNames, HttpVerbs, Status}
import play.api.libs.json.Json
import play.api.mvc.{Request, Result}
import play.api.test.{DefaultAwaitTimeout, FakeRequest, ResultExtractors, Writeables}
import testutil.CallingThreadExecutionContext

import scala.concurrent.Future

class ResourceControllerSpec extends Specification
  with HeaderNames
  with Status
  with HttpVerbs
  with ResultExtractors
  with DefaultAwaitTimeout
  with Writeables {

  class TestResourceRepository extends ResourceRepository {
    var nextSave : Future[SaveResult] = null
    def save(resource: SequencedResource): Future[SaveResult] = nextSave

    var nextSaveAll : Future[Map[ResourceIdentifier, SaveResult]] = null
    def saveAll(resources: Seq[SequencedResource]): Future[Map[ResourceIdentifier, SaveResult]] = nextSaveAll

    var nextDelete : Future[SaveResult] = null
    def delete(resourceType: String, resourceId: String, sequenceNumber: Option[Long]): Future[SaveResult] = nextDelete

    var nextGet : Future[Option[SequencedResource]] = null
    def get(identifier: ResourceIdentifier): Future[Option[SequencedResource]] = nextGet

    var nextGetIncoming : Future[Seq[SequencedResource]] = null
    def getIncoming(resourceType: String, resourceId: String, incomingType: String): Future[Seq[SequencedResource]] = nextGetIncoming
  }

  val auth = new BlindDynamicActions

  "ResourceController#put" should {
    "return 201 Created when the put creates a record" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo", "bar1", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.successful(ResourceRepository.Created)
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 201
    }
    "return 200 OK when the put updates a record" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo", "bar1", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.successful(ResourceRepository.Updated)
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 200
    }
    "return 204 No Content when the put does not make any changes" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo", "bar1", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.successful(ResourceRepository.NotModified)
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 204
    }
    "return 409 Conflict when the put had the wrong sequence number" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo", "bar1", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.successful(ResourceRepository.OutOfSequence)
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 409
    }
    "return 422 Unprocessable Entity when the put has the wrong id" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo", "bar", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.successful(ResourceRepository.OutOfSequence)
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 422
    }
    "return 422 Unprocessable Entity when the put has the wrong type" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo1", "bar1", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.successful(ResourceRepository.OutOfSequence)
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 422
    }
    "return 500 Internal Server Error when the put encounters an exception" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[SingleResourceDoc] = FakeRequest().withBody(SingleResourceDoc(
        SequencedResource("foo", "bar1", None, None),
        None
      )).withHeaders("key" -> "my-key")

      repo.nextSave = Future.failed(new IllegalArgumentException("foo bar"))
      val t : Future[Result] = controller.put("foo", "bar1").apply(r)

      status(t) mustEqual 500
    }
  }
  "ResourceController#multiPut" should {
    "return 200 OK when the puts are all successful" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[MultiResourceDoc] = FakeRequest().withBody(MultiResourceDoc(
        Seq(SequencedResource("foo", "bar"),
            SequencedResource("foo1", "bar1"),
            SequencedResource("foo2", "bar2")),
        None
      )).withHeaders("key" -> "my-key")
      repo.nextSaveAll = Future.successful(
        Map(
          ResourceIdentifier("foo", "bar") -> ResourceRepository.Created,
          ResourceIdentifier("foo1", "bar1") -> ResourceRepository.Updated,
          ResourceIdentifier("foo2", "bar2") -> ResourceRepository.NotModified
        )
      )
      status(controller.multiPut().apply(r)) mustEqual 200
    }
    "read contentSequence from the entity metadata correctly" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository {
        override def saveAll(resources: Seq[SequencedResource]): Future[Map[ResourceIdentifier, SaveResult]] = {
          val result0 = if (resources(0).meta.get.sequenceNumber.contains(19L)) ResourceRepository.Created else ResourceRepository.OutOfSequence
          val result1 = if (resources(1).meta.get.sequenceNumber.contains(20L)) ResourceRepository.Updated else ResourceRepository.OutOfSequence
          val result2 = if (resources(2).meta.isEmpty) ResourceRepository.NotModified else ResourceRepository.OutOfSequence
          Future.successful(
            Map(
              ResourceIdentifier(resources(0).`type`, resources(0).id) -> result0,
              ResourceIdentifier(resources(1).`type`, resources(1).id) -> result1,
              ResourceIdentifier(resources(2).`type`, resources(2).id) -> result2
            )
          )
        }
      }
      val controller = new ResourceController(repo, auth)

      val r : Request[MultiResourceDoc] = FakeRequest().withBody(MultiResourceDoc(
        Seq(SequencedResource("foo", "bar", meta = Some(SequencedMeta(Some(19L)))),
            SequencedResource("foo1", "bar1", meta = Some(SequencedMeta(Some(20L)))),
            SequencedResource("foo2", "bar2")),
        None
      )).withHeaders("key" -> "my-key")

      status(controller.multiPut().apply(r)) mustEqual 200
    }
    "return 409 Conflict when any of the puts failed" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      val r : Request[MultiResourceDoc] = FakeRequest().withBody(MultiResourceDoc(
        Seq(SequencedResource("foo", "bar"),
            SequencedResource("foo1", "bar1"),
            SequencedResource("foo2", "bar2"),
            SequencedResource("foo3", "bar3"),
            SequencedResource("foo4", "bar4"),
            SequencedResource("foo5", "bar5")),
        None
      )).withHeaders("key" -> "my-key")
      repo.nextSaveAll = Future.successful(
        Map(
          ResourceIdentifier("foo", "bar") -> ResourceRepository.Created,
          ResourceIdentifier("foo1", "bar1") -> ResourceRepository.Updated,
          ResourceIdentifier("foo2", "bar2") -> ResourceRepository.NotModified,
          ResourceIdentifier("foo3", "bar3") -> ResourceRepository.OutOfSequence,
          ResourceIdentifier("foo4", "bar4") -> ResourceRepository.Updated,
          ResourceIdentifier("foo5", "bar5") -> ResourceRepository.OutOfSequence
       )
      )

      val response = controller.multiPut().apply(r)
      status(response) mustEqual 409
      val errorDoc = contentAsJson(response).as[ValidationTypes.ValidationErrorDoc]

      errorDoc.error(0).source.get.pointer.get mustEqual "/data(3)"
      errorDoc.error(0).detail.get mustEqual Json.toJson(ResourceIdentifier("foo3", "bar3")).toString
      errorDoc.error(1).source.get.pointer.get mustEqual "/data(5)"
      errorDoc.error(1).detail.get mustEqual Json.toJson(ResourceIdentifier("foo5", "bar5")).toString
   }
  }
  "ResourceController#delete" should {
    "return 200 OK when the delete creates the record in the db" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      repo.nextDelete = Future.successful(ResourceRepository.Created)
      val t: Future[Result] = controller.delete("foo", "bar1").apply(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 200
    }
    "return 200 OK when the delete modifies the record in the db" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      repo.nextDelete = Future.successful(ResourceRepository.Updated)
      val t: Future[Result] = controller.delete("foo", "bar1").apply(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 200
    }
    "return 204 No Content when the delete does not change the record" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)


      repo.nextDelete = Future.successful(ResourceRepository.NotModified)
      val t: Future[Result] = controller.delete("foo", "bar1").apply(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 204
    }
    "return 409 Conflict when the delete had the wrong sequence number" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      repo.nextDelete = Future.successful(ResourceRepository.OutOfSequence)
      val t: Future[Result] = controller.delete("foo", "bar1").apply(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 409
    }
    "return 500 Internal Server Error when the delete encounters an exception" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      repo.nextDelete = Future.failed(new IllegalArgumentException("foo bar"))
      val t: Future[Result] = controller.delete("foo", "bar1").apply(FakeRequest().withHeaders("key" -> "my-key"))

      status(t) mustEqual 500
    }
  }
  "ResourceController#get" should {
    "have application/json content type and utf-8 charset" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = new TestResourceRepository
      val controller = new ResourceController(repo, auth)

      repo.nextGet = Future.successful(Some(SequencedResource("foo","bar1")))
      repo.nextGetIncoming = Future.successful(Seq(SequencedResource("foo","bar1")))
      val response: Future[Result] = controller.getIncoming("foo","bar1","foobar").apply(FakeRequest().withHeaders("key" -> "my-key"))

      contentType(response).getOrElse("failed") mustEqual "application/json"
      charset(response).getOrElse("failed") mustEqual "utf-8"
      status(response) mustEqual 200
    }
  }
}
