package core.db

import com.typesafe.config.{Config, ConfigFactory}
import slick.profile.SqlProfile.ColumnOption

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object CustomizedCodeGenerator {

  import scala.concurrent.ExecutionContext.Implicits.global

  def main(path: String, outPackage: String): Unit = {
    val config = ConfigFactory.load()
    val dbConfig = config.getConfig("slick.dbs.default.db")

    val db = PgDriver.api.Database.forURL(
      dbConfig.getString("url"),
      dbConfig.getString("user"),
      dbConfig.getString("password"),
      driver = dbConfig.getString("driver")
    )
    // filter out desired tables
    val excluded = Seq("PLAY_EVOLUTIONS")

    val modelWritten = db.run {
      PgDriver.defaultTables.map(_.filter(t => !(excluded contains t.name.name.toUpperCase)))
        .flatMap(PgDriver.createModelBuilder(_, false).buildModel)
    }.map { model =>
      val caseClasses = new mutable.MutableList[String]
      new slick.codegen.SourceCodeGenerator(model) {
        override def Table = new Table(_) { table =>

          // place the case classes outside the Tables trait, so play can use macros to autogenerate json reads/writes
          override def EntityType = new EntityTypeDef {
            override def docWithCode: String = {
              caseClasses += super.docWithCode.toString + "\n"
              ""
            }
          }

          // filter out types for which we supply an implicit GetResult.
          // Otherwise we get an error about "ambiguous implicit values"
          val autoGetterImplicits = Set(
            "play.api.libs.json.JsValue",
            "org.joda.time.Date",
            "org.joda.time.Time",
            "org.joda.time.DateTime",
            "Option[List[String]]"
          )
          override def PlainSqlMapper = new PlainSqlMapperDef {
            override def code = {
              val positional = compoundValue(columnsPositional.map(c => (if(c.fakeNullable || c.model.nullable)s"<<?[${c.rawType}]"else s"<<[${c.rawType}]")))
              val dependencies = columns.map(_.exposedType).distinct.filterNot(autoGetterImplicits).zipWithIndex.map{ case (t,i) => s"""e$i: GR[$t]"""}.mkString(", ")
              val depStr = if (dependencies.nonEmpty) s"implicit $dependencies" else ""
              val rearranged = compoundValue(desiredColumnOrder.map(i => if(hlistEnabled) s"r($i)" else tuple(i)))
              def result(args: String) = if(mappingEnabled) s"$factory($args)" else args
              val body =
                if(autoIncLastAsOption && columns.size > 1){
                  s"""
val r = $positional
import r._
${result(rearranged)} // putting AutoInc last
            """.trim
                } else
                  result(positional)
              s"""
implicit def ${name}($depStr): GR[${TableClass.elementType}] = GR{
  prs => import prs._
  ${indent(body)}
}
        """.trim
            }
          }

          // fix custom types
          override def Column = new Column(_) { column =>
            override def rawType: String = model.tpe match {
              case "java.sql.Date" => "org.joda.time.Date"
              case "java.sql.Time" => "org.joda.time.Time"
              case "java.sql.Timestamp" => "org.joda.time.DateTime"
              // currently, all types that's not built-in support were mapped to `String`
              case "String" => model.options.find(_.isInstanceOf[ColumnOption.SqlType]).map(_.asInstanceOf[ColumnOption.SqlType].typeName).map({
                case "hstore" => "Map[String, String]"
                case "geometry" => "com.vividsolutions.jts.geom.Geometry"
                case "int8[]" => "List[Long]"
                case "_text" => "List[String]"
                case "jsonb" => "play.api.libs.json.JsValue"
                case "_jsonb" => "List[play.api.libs.json.JsValue]"
                case t =>
                  println(s"mapping $t to 'String'")
                  "String"
              }).getOrElse("String")
              case _ => super.rawType
            }
          }
        }

        // ensure to use our customized postgres driver at `import profile.simple._`
        override def packageCode(profile: String, pkg: String, container: String, parentType: Option[String]): String = {
          s"""
package ${pkg}
// AUTO-GENERATED Slick data model
/** Stand-alone Slick data model for immediate use */
object ${container} extends {
  val profile = ${profile}
} with ${container}
/** Slick data model trait for extension, choice of backend or usage in the cake pattern. (Make sure to initialize this late.) */
trait ${container}${parentType.map(t => s" extends $t").getOrElse("")} {
  val profile: $profile
  import profile.api._
  ${indent(code)}
}

${caseClasses.mkString("\n")}
      """.trim()
        }
      }.writeToFile("core.db.PgDriver", path, outPackage)
    }
    Await.result(modelWritten, Duration.Inf)
  }
}
