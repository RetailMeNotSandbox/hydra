package core.encoding

import play.api.http.Writeable
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.Codec

trait JsValueUTF8Encoding {
  implicit val jsWriter = new Writeable[JsValue]({ event =>
    Codec.utf_8.encode(Json.stringify(event))
  }, Some("application/json; charset=utf-8"))
}
