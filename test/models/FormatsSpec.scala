package models

import com.rmn.jsonapi.models.ResourceIdentifier
import com.rmn.jsonapi.models.TypeAliases.{GenericResource, GenericRelationshipObject}
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import play.api.libs.json._

@RunWith(classOf[JUnitRunner])
class FormatsSpec extends Specification {
  "Formats#resourceReads" should {
    "allow valid resources" in {
      Json.parse("""
        {
          "type": "mcguffin",
          "id": "MCGUFFINUUID"
        }
                 """).validate[GenericResource] must beEqualTo(JsSuccess(
        GenericResource(
          "mcguffin",
          "MCGUFFINUUID",
          None,
          None
        )
      ))

      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "title": "Rails is Omakase"
          },
          "relationships": {
            "author": {
              "data": {
                "type": "person",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must beEqualTo(JsSuccess(
        GenericResource(
          "article",
          "1",
          Some(JsObject(Seq("title" -> JsString("Rails is Omakase")))),
          Some(Map("author" -> GenericRelationshipObject(data = Some(Left(ResourceIdentifier("person", "9"))))))
        )
      ))

      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "title": "Rails is Omakase"
          },
          "relationships": {
            "editors": {
              "data": [
                { "type": "person", "id": "9" },
                { "type": "person", "id": "10" },
                { "type": "wookiee", "id": "1" }
              ]
            }
          }
        }
                 """).validate[GenericResource] must beEqualTo(JsSuccess(
        GenericResource(
          "article",
          "1",
          Some(JsObject(Seq("title" -> JsString("Rails is Omakase")))),
          Some(
            Map("editors" -> GenericRelationshipObject(data =
              Some(
                Right(List(
                  ResourceIdentifier("person", "9"),
                  ResourceIdentifier("person", "10"),
                  ResourceIdentifier("wookiee", "1")
                ))
              )
            ))
          )
        )
      ))
    }

    "enforce distinct namespaces" in {
      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "title": "Rails is Omakase"
          },
          "relationships": {
            "title": {
              "data": {
                "type": "charSequence",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must haveClass[JsError]

      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "title": "Rails is Omakase"
          },
          "relationships": {
            "type": {
              "data": {
                "type": "universe",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must haveClass[JsError]

      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "id": "Rails is Omakase"
          },
          "relationships": {
            "author": {
              "data": {
                "type": "person",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must haveClass[JsError]
    }

    "disallow 'relationships' or 'links' attributes" in {
      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "links": "Rails is Omakase"
          },
          "relationships": {
            "author": {
              "data": {
                "type": "person",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must haveClass[JsError]

      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "title": "Rails is Omakase",
            "tags": [
              {
                "tagText": "asdf",
                "tagData": null
              },
              {
                "tagText": "asdf2",
                "tagData": {
                  "relationships": "asdf"
                }
              }
            ]
          },
          "relationships": {
            "author": {
              "data": {
                "type": "person",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must haveClass[JsError]

      Json.parse("""
        {
          "type": "article",
          "id": "1",
          "attributes": {
            "title": "Rails is Omakase",
            "metadata": {
              "links": ["http://www.google.com"]
            }
          },
          "relationships": {
            "author": {
              "data": {
                "type": "person",
                "id": "9"
              }
            }
          }
        }
                 """).validate[GenericResource] must haveClass[JsError]
    }
  }
}
