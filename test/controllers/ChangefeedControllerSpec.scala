package controllers

import actors.ChangefeedSource.ChangefeedEvent
import akka.stream.scaladsl.Source
import core.util.BlindDynamicActions
import dal.{ChangeHistoryRow, ChangefeedRepository, ChangefeedRow, DocumentRepository}
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import play.api.http.{ContentTypes, HeaderNames, HttpVerbs, Status}
import play.api.libs.json.{JsNull, Json}
import play.api.test._
import testutil.{CallingThreadExecutionContext, SuiteActorSystem}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class ChangefeedControllerSpec extends PlaySpec
  with HeaderNames
  with Status
  with HttpVerbs
  with ResultExtractors
  with DefaultAwaitTimeout
  with Writeables
  with EssentialActionCaller
  with SuiteActorSystem {

  object TestChangefeedRepository {
    def apply(listR: Future[Seq[(ChangefeedRow, Long, Option[DateTime])]] = Future.failed(new NotImplementedError()),
              getR: Future[Option[(ChangefeedRow, Long, Option[DateTime])]] = Future.failed(new NotImplementedError()),
              simpleGetR: Future[Option[ChangefeedRow]] = Future.failed(new NotImplementedError()),
              createR: Future[Int] = Future.failed(new NotImplementedError()),
              deleteR: Future[Int] = Future.failed(new NotImplementedError()),
              ackR: Future[Int] = Future.failed(new NotImplementedError())
             ): ChangefeedRepository =
      new ChangefeedRepository {
        def simpleGet(id: String) = simpleGetR
        def get(id: String) = getR
        def delete(id: String) = deleteR
        def list(page: Int, size: Int) = listR
        def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]) = createR
        def ack(id: String, ack: Long) = ackR
      }
  }
  val t0 = new LocalDateTime(2017, 9, 21, 6, 0, 0).toDateTime(DateTimeZone.UTC)
  val t1 = new LocalDateTime(2017, 2, 21, 12, 0, 0).toDateTime(DateTimeZone.UTC)
  val t2 = new LocalDateTime(2016, 10, 21, 18, 0, 0).toDateTime(DateTimeZone.UTC)
  val source = Source(immutable.Seq(
    ChangefeedEvent("event", Some(ChangeHistoryRow("foo", "bar", 0, t0))),
    ChangefeedEvent("event", Some(ChangeHistoryRow("fizz", "buzz", 1, t1))),
    ChangefeedEvent("event", Some(ChangeHistoryRow("hello", "world", 2, t2)))
  ))
  val auth = new BlindDynamicActions
  val builder = new ChangeSourceBuilder {
    def buildSource(changefeed: ChangefeedRow, maxUnackedCount: Int, delay: FiniteDuration) = source
  }

  "ChangefeedController#list" should {
    "return 403 when the auth key isn't valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(0, 25)(FakeRequest())

      status(response) mustEqual 403
    }
    "return 200 when the auth key is valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(listR = Future.successful(Seq.empty))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(0, 25)(FakeRequest().withHeaders("key" -> "foo"))

      contentType(response).getOrElse("failed") mustEqual "application/json"
      charset(response).getOrElse("failed") mustEqual "utf-8"
      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(),
        "links" -> Json.obj(
          "self" -> "/changefeed",
          "first" -> "/changefeed"
        )
      )
    }
    "constrain the page param to at least 0" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(listR = Future.successful(Seq.empty))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(-100, 25)(FakeRequest().withHeaders("key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(),
        "links" -> Json.obj(
          "self" -> "/changefeed",
          "first" -> "/changefeed"
        )
      )
    }
    "use a valid page param" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(listR = Future.successful(Seq.empty))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(20, 25)(FakeRequest().withHeaders("key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(),
        "links" -> Json.obj(
          "self" -> "/changefeed?page=20",
          "first" -> "/changefeed",
          "prev" -> "/changefeed?page=19"
        )
      )
    }

    "include the next link if this page is full" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(listR = Future.successful(Seq((ChangefeedRow("foo", created = now, lastAck = now), 100, Some(now)))))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(1, 1)(FakeRequest().withHeaders("key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(
          Json.obj(
            "type" -> "changefeed",
            "id" -> "foo",
            "attributes" -> Json.obj(
              "maxAck" -> 0,
              "parentMaxAck" -> 100,
              "maxEventTime" -> "2017-02-06T17:12:48.000Z",
              "created" -> "2017-02-06T17:12:48.000Z",
              "lastAck" -> "2017-02-06T17:12:48.000Z"
            ),
            "relationships" -> Json.obj()
          )
        ),
        "links" -> Json.obj(
          "self" -> "/changefeed?page=1&size=1",
          "first" -> "/changefeed?size=1",
          "next" -> "/changefeed?page=2&size=1",
          "prev" -> "/changefeed?size=1"
        )
      )
    }

    "constrain the size param to at least 1" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(listR = Future.successful(Seq.empty))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(0, -10)(FakeRequest().withHeaders("key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(),
        "links" -> Json.obj(
          "self" -> "/changefeed?size=1",
          "first" -> "/changefeed?size=1"
        )
      )
    }
    "constrain the size param to at most 100" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(listR = Future.successful(Seq.empty))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(0, 200)(FakeRequest().withHeaders("key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(),
        "links" -> Json.obj(
          "self" -> "/changefeed?size=100",
          "first" -> "/changefeed?size=100"
        )
      )
    }
    "use a valid size param" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(listR = Future.successful(Seq.empty))
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.list(0, 10)(FakeRequest().withHeaders("key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.arr(),
        "links" -> Json.obj(
          "self" -> "/changefeed?size=10",
          "first" -> "/changefeed?size=10"
        )
      )
    }
  }

  "ChangefeedController#create" should {
    "return 415 when the content type is incorrect" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val response = call(controller.create, FakeRequest())

      status(response) mustEqual 415
    }
    "return 400 if the body is incorrectly formatted" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val body = Json.obj(
        "data" -> Json.obj(
          "type" -> "changefeed",
          "id" -> "foo",
          "attributes" -> Json.obj(
            "typeFilter" -> 123 // should be a list of strings
          ),
          "relationships" -> Json.obj(
            "parent" -> Json.obj(
              "data" -> Json.obj(
                "type" -> "changefeed",
                "id" -> "bar"
              )
            )
          )
        )
      )
      val response = call(controller.create, FakeRequest().withJsonBody(body).withHeaders("Content-Type" -> ResourceParser.JSON_API_MEDIA_TYPE))

      status(response) mustEqual 400
    }
    "return 403 when the auth key isn't valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val body = Json.obj(
        "data" -> Json.obj(
          "type" -> "changefeed",
          "id" -> "foo",
          "attributes" -> Json.obj(
            "typeFilter" -> List("filter")
          ),
          "relationships" -> Json.obj(
            "parent" -> Json.obj(
              "data" -> Json.obj(
                "type" -> "changefeed",
                "id" -> "bar"
              )
            )
          )
        )
      )

      val response = call(controller.create, FakeRequest().withJsonBody(body).withHeaders("Content-Type" -> ResourceParser.JSON_API_MEDIA_TYPE))

      status(response) mustEqual 403
    }
    "return 200 when the request is valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        createR = Future.successful(1),
        getR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 0, now, now), 100, None))
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val body = Json.obj(
        "data" -> Json.obj(
          "type" -> "changefeed",
          "id" -> "foo",
          "attributes" -> Json.obj(
            "typeFilter" -> List("filter")
          ),
          "relationships" -> Json.obj(
            "parent" -> Json.obj(
              "data" -> Json.obj(
                "type" -> "changefeed",
                "id" -> "bar"
              )
            )
          )
        )
      )
      val request = FakeRequest()
        .withJsonBody(body)
        .withHeaders("Content-Type" -> ResourceParser.JSON_API_MEDIA_TYPE)
        .withHeaders("Key" -> "foo")

      val response = call(controller.create, request)

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.obj(
          "type" -> "changefeed",
          "id" -> "foo",
          "attributes" -> Json.obj(
            "maxAck" -> 0,
            "parentMaxAck" -> 100,
            "typeFilter" -> List("filter"),
            "created" -> "2017-02-06T17:12:48.000Z",
            "lastAck" -> "2017-02-06T17:12:48.000Z"
          ),
          "relationships" -> Json.obj(
            "parent" -> Json.obj(
              "data" -> Json.obj(
                "type" -> "changefeed",
                "id" -> "bar"
              )
            )
          )
        )
      )
    }
  }

  "ChangefeedController#get" should {
    "return 403 when the auth key isn't valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.get("foo")(FakeRequest())

      status(response) mustEqual 403
    }

    "return 404 when the id cannot be found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(
        getR = Future.successful(None)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.get("foo")(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 404
    }

    "return 200 when the changefeed is found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        getR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 0, now, now), 100, Some(now)))
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.get("foo")(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.obj(
          "type" -> "changefeed",
          "id" -> "foo",
          "attributes" -> Json.obj(
            "maxAck" -> 0,
            "parentMaxAck" -> 100,
            "maxEventTime" -> "2017-02-06T17:12:48.000Z",
            "typeFilter" -> List("filter"),
            "created" -> "2017-02-06T17:12:48.000Z",
            "lastAck" -> "2017-02-06T17:12:48.000Z"
          ),
          "relationships" -> Json.obj(
            "parent" -> Json.obj(
              "data" -> Json.obj(
                "type" -> "changefeed",
                "id" -> "bar"
              )
            )
          )
        )
      )
    }

    "return 200 when the changefeed has no maxEventTime" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        getR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 0, now, now), 100, None))
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.get("foo")(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      contentAsJson(response) mustEqual Json.obj(
        "data" -> Json.obj(
          "type" -> "changefeed",
          "id" -> "foo",
          "attributes" -> Json.obj(
            "maxAck" -> 0,
            "parentMaxAck" -> 100,
            "typeFilter" -> List("filter"),
            "created" -> "2017-02-06T17:12:48.000Z",
            "lastAck" -> "2017-02-06T17:12:48.000Z"
          ),
          "relationships" -> Json.obj(
            "parent" -> Json.obj(
              "data" -> Json.obj(
                "type" -> "changefeed",
                "id" -> "bar"
              )
            )
          )
        )
      )
    }
  }

  "ChangefeedController#delete" should {
    "return 403 when the auth key isn't valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.delete("foo")(FakeRequest())

      status(response) mustEqual 403
    }

    "return 404 when the id cannot be found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(
        deleteR = Future.successful(0)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.delete("foo")(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 404
    }

    "return 204 when the row is deleted" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(
        deleteR = Future.successful(1)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.delete("foo")(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 204
    }
  }

  "ChangefeedController#ack" should {
    "return 403 when the auth key isn't valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.ack("foo", 1)(FakeRequest())

      status(response) mustEqual 403
    }

    "return 404 when the id cannot be found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(
        getR = Future.successful(None)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.ack("foo", 1)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 404
    }

    "return 400 if the ack'd seqnum is greater than the parent ack sequence number" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        getR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now), 100, Some(now))),
        ackR = Future.successful(1)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.ack("foo", 101)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 400
    }

    "return 204 when the ack is successful" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        getR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now), 100, Some(now))),
        ackR = Future.successful(1)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.ack("foo", 1)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 204
    }
  }

  "ChangefeedController#stream" should {
    "return 403 when the auth key isn't valid" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository()
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.stream("foo", 100, 250)(FakeRequest())

      status(response) mustEqual 403
    }

    "return 404 when the id cannot be found" in {
      implicit val executor = new CallingThreadExecutionContext()
      val repo = TestChangefeedRepository(
        simpleGetR = Future.successful(None)
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.stream("foo", 100, 250)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 404
    }

    "return 200 with json separated by newlines" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        simpleGetR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now)))
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.stream("foo", 100, 250)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      // only works because builder returns a finite list, instead of the infinitely running Fetcher actor
      val actual = contentAsString(response)
      val expected =
        s"""{"eventType":"event","data":{"type":"foo","id":"bar","seq":0,"eventTime":${t0.getMillis}}}
           |{"eventType":"event","data":{"type":"fizz","id":"buzz","seq":1,"eventTime":${t1.getMillis}}}
           |{"eventType":"event","data":{"type":"hello","id":"world","seq":2,"eventTime":${t2.getMillis}}}
           |""".stripMargin

      actual mustEqual expected
    }

    "specify that the connection should be closed" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        simpleGetR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now)))
      )
      val controller = new ChangefeedController(repo, auth, builder)

      val response = controller.stream("foo", 100, 250)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      headers(response).get("Connection") mustEqual Some("close")
    }

    "constrain buffer size to at least 1" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        simpleGetR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now)))
      )
      var actualSize : Option[Int] = None
      val captureBuilder = new ChangeSourceBuilder {
        def buildSource(changefeed: ChangefeedRow, maxUnackedCount: Int, delay: FiniteDuration) = {
          actualSize = Some(maxUnackedCount)
          source
        }
      }

      val controller = new ChangefeedController(repo, auth, captureBuilder)

      val response = controller.stream("foo", -100, 100)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      // only works because builder returns a finite list, instead of the infinitely running Fetcher actor
      val actual = contentAsString(response)
      val expected =
        s"""{"eventType":"event","data":{"type":"foo","id":"bar","seq":0,"eventTime":${t0.getMillis}}}
           |{"eventType":"event","data":{"type":"fizz","id":"buzz","seq":1,"eventTime":${t1.getMillis}}}
           |{"eventType":"event","data":{"type":"hello","id":"world","seq":2,"eventTime":${t2.getMillis}}}
           |""".stripMargin

      actual mustEqual expected


      actualSize mustEqual Some(1)
    }


    "constrain buffer size to at most 10,000" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        simpleGetR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now)))
      )
      var actualSize : Option[Int] = None
      val captureBuilder = new ChangeSourceBuilder {
        def buildSource(changefeed: ChangefeedRow, maxUnackedCount: Int, delay: FiniteDuration) = {
          actualSize = Some(maxUnackedCount)
          source
        }
      }

      val controller = new ChangefeedController(repo, auth, captureBuilder)

      val response = controller.stream("foo", Int.MaxValue, 100)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      // only works because builder returns a finite list, instead of the infinitely running Fetcher actor
      val actual = contentAsString(response)
      val expected =
        s"""{"eventType":"event","data":{"type":"foo","id":"bar","seq":0,"eventTime":${t0.getMillis}}}
           |{"eventType":"event","data":{"type":"fizz","id":"buzz","seq":1,"eventTime":${t1.getMillis}}}
           |{"eventType":"event","data":{"type":"hello","id":"world","seq":2,"eventTime":${t2.getMillis}}}
           |""".stripMargin

      actual mustEqual expected


      actualSize mustEqual Some(10000)
    }


    "constrain delayMS size to at least 0" in {
      implicit val executor = new CallingThreadExecutionContext()
      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      val repo = TestChangefeedRepository(
        simpleGetR = Future.successful(Some(ChangefeedRow("foo", Some("bar"), Some(List("filter")), 1, now, now)))
      )
      var actualDelay : Option[FiniteDuration] = None
      val captureBuilder = new ChangeSourceBuilder {
        def buildSource(changefeed: ChangefeedRow, maxUnackedCount: Int, delay: FiniteDuration) = {
          actualDelay = Some(delay)
          source
        }
      }

      val controller = new ChangefeedController(repo, auth, captureBuilder)

      val response = controller.stream("foo", 100, -100)(FakeRequest().withHeaders("Key" -> "foo"))

      status(response) mustEqual 200
      // only works because builder returns a finite list, instead of the infinitely running Fetcher actor
      val actual = contentAsString(response)
      val expected =
        s"""{"eventType":"event","data":{"type":"foo","id":"bar","seq":0,"eventTime":${t0.getMillis}}}
           |{"eventType":"event","data":{"type":"fizz","id":"buzz","seq":1,"eventTime":${t1.getMillis}}}
           |{"eventType":"event","data":{"type":"hello","id":"world","seq":2,"eventTime":${t2.getMillis}}}
           |""".stripMargin

      actual mustEqual expected


      actualDelay mustEqual Some(0.millis)
    }
  }
}
