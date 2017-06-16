package core.db


import com.github.tminglei.slickpg._
import com.github.tminglei.slickpg.utils.PlainSQLUtils.{mkArrayOptionSetParameter, mkArraySetParameter, mkGetResult}
import play.api.libs.json.{JsValue, Json}
import slick.jdbc.SetParameter


trait PgDriver extends ExPostgresDriver
  with PgArraySupport
  with PgDateSupportJoda
  with PgHStoreSupport
  with PgPlayJsonSupport {
  def pgjson = "jsonb" // jsonb support is in postgres 9.4.0 onward; for 9.3.x use "json"

  override val api = PgAPI

  object PgAPI extends API
    with ArrayImplicits
    with SimpleArrayPlainImplicits
    with DateTimeImplicits
    with JodaDateTimePlainImplicits
    with JsonImplicits
    with PlayJsonPlainImplicits {

    implicit val playJsonArrayTypeMapper =
    new AdvancedArrayJdbcType[JsValue](pgjson,
      (s) => utils.SimpleArrayUtils.fromString[JsValue](Json.parse(_))(s).orNull,
      (v) => utils.SimpleArrayUtils.mkString[JsValue](_.toString())(v)
    ).to(_.toList)

    implicit val getJsonbArray = mkGetResult(_.nextArray[JsValue]())
    implicit val getJsonbArrayOption = mkGetResult(_.nextArrayOption[JsValue]())
    implicit val setJsonbArray = mkArraySetParameter[JsValue]("jsonb")
    implicit val setJsonbArrayOption = mkArrayOptionSetParameter[JsValue]("jsonb")

    // if SetParameter[Option[Seq[JsValue]]] is "setJsonbArrayOption"
    // then SetParameter[List[Option[Long]]] becomes "setLongOptionArray"
    implicit val setLongOptionArray: SetParameter[Seq[Option[Long]]] =
      mkArraySetParameter[Option[Long]]("int8", toStr = _.fold("NULL")(_.toString))
  }
}


object PgDriver extends PgDriver
