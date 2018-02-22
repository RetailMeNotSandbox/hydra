package core.db


import com.github.tminglei.slickpg._
import com.github.tminglei.slickpg.utils.PlainSQLUtils.{mkArrayOptionSetParameter, mkArraySetParameter, mkGetResult}
import play.api.libs.json.{JsValue, Json}
import slick.jdbc.{GetResult, SetParameter}


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

    implicit val playJsonArrayTypeMapper: DriverJdbcType[List[JsValue]] =
      new AdvancedArrayJdbcType[JsValue](
        pgjson,
        (s) => utils.SimpleArrayUtils.fromString[JsValue](Json.parse)(s).orNull,
        (v) => utils.SimpleArrayUtils.mkString[JsValue](_.toString)(v)
      ).to(_.toList)

    implicit val getJsonbArray: GetResult[Seq[JsValue]] =
      mkGetResult(_.nextArray[JsValue]())
    implicit val getJsonbArrayOption: GetResult[Option[Seq[JsValue]]] =
      mkGetResult(_.nextArrayOption[JsValue]())
    implicit val setJsonbArray: SetParameter[Seq[JsValue]] =
      mkArraySetParameter[JsValue]("jsonb")
    implicit val setJsonbArrayOption: SetParameter[Option[Seq[JsValue]]] =
      mkArrayOptionSetParameter[JsValue]("jsonb")

    // Based on the convention established above, these conversion functions should be named "inside-out".
    // if SetParameter[Option[Seq[JsValue]]] is "setJsonbArrayOption"
    // then SetParameter[Seq[Option[Long]]] becomes "setLongOptionArray"
    implicit val getStringListOption: GetResult[Option[List[String]]] =
      mkGetResult(_.nextArrayOption[String]().map(_.toList))
    implicit val setLongOptionArray: SetParameter[Seq[Option[Long]]] =
      mkArraySetParameter[Option[Long]]("int8", toStr = _.fold("NULL")(_.toString))
  }
}


object PgDriver extends PgDriver
