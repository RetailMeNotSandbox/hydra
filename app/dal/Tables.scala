package dal
// AUTO-GENERATED Slick data model
/** Stand-alone Slick data model for immediate use */
object Tables extends {
  val profile = core.db.PgDriver
} with Tables
/** Slick data model trait for extension, choice of backend or usage in the cake pattern. (Make sure to initialize this late.) */
trait Tables {
  val profile: core.db.PgDriver
  import profile.api._
  import slick.model.ForeignKeyAction
  // NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.
  import slick.jdbc.{GetResult => GR}

  /** DDL for all tables. Call .create to execute. */
  lazy val schema: profile.SchemaDescription = Changefeed.schema ++ ChangeHistory.schema ++ ChangeHistoryToExpand.schema ++ Resource.schema ++ ResourceReferences.schema
  @deprecated("Use .schema instead of .ddl", "3.0")
  def ddl = schema


  /** GetResult implicit for fetching ChangefeedRow objects using plain SQL queries */
  implicit def GetResultChangefeedRow(implicit e0: GR[String], e1: GR[Option[String]], e2: GR[Long]): GR[ChangefeedRow] = GR{
    prs => import prs._
    ChangefeedRow.tupled((<<[String], <<?[String], <<?[List[String]], <<[Long], <<[org.joda.time.DateTime], <<[org.joda.time.DateTime]))
  }
  /** Table description of table changefeed. Objects of this class serve as prototypes for rows in queries. */
  class Changefeed(_tableTag: Tag) extends Table[ChangefeedRow](_tableTag, "changefeed") {
    def * = (id, parentId, typeFilter, maxAck, created, lastAck) <> (ChangefeedRow.tupled, ChangefeedRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), parentId, typeFilter, Rep.Some(maxAck), Rep.Some(created), Rep.Some(lastAck)).shaped.<>({r=>import r._; _1.map(_=> ChangefeedRow.tupled((_1.get, _2, _3, _4.get, _5.get, _6.get)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(text), PrimaryKey */
    val id: Rep[String] = column[String]("id", O.PrimaryKey)
    /** Database column parent_id SqlType(text), Default(None) */
    val parentId: Rep[Option[String]] = column[Option[String]]("parent_id", O.Default(None))
    /** Database column type_filter SqlType(_text), Length(2147483647,false), Default(None) */
    val typeFilter: Rep[Option[List[String]]] = column[Option[List[String]]]("type_filter", O.Length(2147483647,varying=false), O.Default(None))
    /** Database column max_ack SqlType(int8), Default(0) */
    val maxAck: Rep[Long] = column[Long]("max_ack", O.Default(0L))
    /** Database column created SqlType(timestamptz) */
    val created: Rep[org.joda.time.DateTime] = column[org.joda.time.DateTime]("created")
    /** Database column last_ack SqlType(timestamptz) */
    val lastAck: Rep[org.joda.time.DateTime] = column[org.joda.time.DateTime]("last_ack")

    /** Foreign key referencing Changefeed (database name changefeed_parent_id_fkey) */
    lazy val changefeedFk = foreignKey("changefeed_parent_id_fkey", parentId, Changefeed)(r => Rep.Some(r.id), onUpdate=ForeignKeyAction.NoAction, onDelete=ForeignKeyAction.NoAction)
  }
  /** Collection-like TableQuery object for table Changefeed */
  lazy val Changefeed = new TableQuery(tag => new Changefeed(tag))


  /** GetResult implicit for fetching ChangeHistoryRow objects using plain SQL queries */
  implicit def GetResultChangeHistoryRow(implicit e0: GR[String], e1: GR[Long]): GR[ChangeHistoryRow] = GR{
    prs => import prs._
    ChangeHistoryRow.tupled((<<[String], <<[String], <<[Long], <<[org.joda.time.DateTime]))
  }
  /** Table description of table change_history. Objects of this class serve as prototypes for rows in queries.
   *  NOTE: The following names collided with Scala keywords and were escaped: type */
  class ChangeHistory(_tableTag: Tag) extends Table[ChangeHistoryRow](_tableTag, "change_history") {
    def * = (`type`, id, seq, eventTime) <> (ChangeHistoryRow.tupled, ChangeHistoryRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(`type`), Rep.Some(id), Rep.Some(seq), Rep.Some(eventTime)).shaped.<>({r=>import r._; _1.map(_=> ChangeHistoryRow.tupled((_1.get, _2.get, _3.get, _4.get)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column type SqlType(text)
     *  NOTE: The name was escaped because it collided with a Scala keyword. */
    val `type`: Rep[String] = column[String]("type")
    /** Database column id SqlType(text) */
    val id: Rep[String] = column[String]("id")
    /** Database column seq SqlType(bigserial), AutoInc */
    val seq: Rep[Long] = column[Long]("seq", O.AutoInc)
    /** Database column event_time SqlType(timestamptz) */
    val eventTime: Rep[org.joda.time.DateTime] = column[org.joda.time.DateTime]("event_time")

    /** Primary key of ChangeHistory (database name change_history_pkey) */
    val pk = primaryKey("change_history_pkey", (`type`, id))
  }
  /** Collection-like TableQuery object for table ChangeHistory */
  lazy val ChangeHistory = new TableQuery(tag => new ChangeHistory(tag))


  /** GetResult implicit for fetching ChangeHistoryToExpandRow objects using plain SQL queries */
  implicit def GetResultChangeHistoryToExpandRow(implicit e0: GR[Long], e1: GR[String]): GR[ChangeHistoryToExpandRow] = GR{
    prs => import prs._
    ChangeHistoryToExpandRow.tupled((<<[Long], <<[String], <<[String]))
  }
  /** Table description of table change_history_to_expand. Objects of this class serve as prototypes for rows in queries.
   *  NOTE: The following names collided with Scala keywords and were escaped: type */
  class ChangeHistoryToExpand(_tableTag: Tag) extends Table[ChangeHistoryToExpandRow](_tableTag, "change_history_to_expand") {
    def * = (key, `type`, id) <> (ChangeHistoryToExpandRow.tupled, ChangeHistoryToExpandRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(key), Rep.Some(`type`), Rep.Some(id)).shaped.<>({r=>import r._; _1.map(_=> ChangeHistoryToExpandRow.tupled((_1.get, _2.get, _3.get)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column key SqlType(bigserial), AutoInc, PrimaryKey */
    val key: Rep[Long] = column[Long]("key", O.AutoInc, O.PrimaryKey)
    /** Database column type SqlType(text)
     *  NOTE: The name was escaped because it collided with a Scala keyword. */
    val `type`: Rep[String] = column[String]("type")
    /** Database column id SqlType(text) */
    val id: Rep[String] = column[String]("id")
  }
  /** Collection-like TableQuery object for table ChangeHistoryToExpand */
  lazy val ChangeHistoryToExpand = new TableQuery(tag => new ChangeHistoryToExpand(tag))


  /** GetResult implicit for fetching ResourceRow objects using plain SQL queries */
  implicit def GetResultResourceRow(implicit e0: GR[Boolean], e1: GR[Option[Long]], e2: GR[List[play.api.libs.json.JsValue]]): GR[ResourceRow] = GR{
    prs => import prs._
    ResourceRow.tupled((<<[play.api.libs.json.JsValue], <<[Boolean], <<[play.api.libs.json.JsValue], <<?[Long], <<[org.joda.time.DateTime], <<[org.joda.time.DateTime], <<[List[play.api.libs.json.JsValue]]))
  }
  /** Table description of table resource. Objects of this class serve as prototypes for rows in queries. */
  class Resource(_tableTag: Tag) extends Table[ResourceRow](_tableTag, "resource") {
    def * = (id, deleted, resource, sequenceNumber, created, updated, outRefs) <> (ResourceRow.tupled, ResourceRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), Rep.Some(deleted), Rep.Some(resource), sequenceNumber, Rep.Some(created), Rep.Some(updated), Rep.Some(outRefs)).shaped.<>({r=>import r._; _1.map(_=> ResourceRow.tupled((_1.get, _2.get, _3.get, _4, _5.get, _6.get, _7.get)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(jsonb), PrimaryKey, Length(2147483647,false) */
    val id: Rep[play.api.libs.json.JsValue] = column[play.api.libs.json.JsValue]("id", O.PrimaryKey, O.Length(2147483647,varying=false))
    /** Database column deleted SqlType(bool) */
    val deleted: Rep[Boolean] = column[Boolean]("deleted")
    /** Database column resource SqlType(jsonb), Length(2147483647,false) */
    val resource: Rep[play.api.libs.json.JsValue] = column[play.api.libs.json.JsValue]("resource", O.Length(2147483647,varying=false))
    /** Database column sequence_number SqlType(int8), Default(None) */
    val sequenceNumber: Rep[Option[Long]] = column[Option[Long]]("sequence_number", O.Default(None))
    /** Database column created SqlType(timestamptz) */
    val created: Rep[org.joda.time.DateTime] = column[org.joda.time.DateTime]("created")
    /** Database column updated SqlType(timestamptz) */
    val updated: Rep[org.joda.time.DateTime] = column[org.joda.time.DateTime]("updated")
    /** Database column out_refs SqlType(_jsonb), Length(2147483647,false) */
    val outRefs: Rep[List[play.api.libs.json.JsValue]] = column[List[play.api.libs.json.JsValue]]("out_refs", O.Length(2147483647,varying=false))
  }
  /** Collection-like TableQuery object for table Resource */
  lazy val Resource = new TableQuery(tag => new Resource(tag))


  /** GetResult implicit for fetching ResourceReferencesRow objects using plain SQL queries */
  implicit def GetResultResourceReferencesRow(): GR[ResourceReferencesRow] = GR{
    prs => import prs._
    ResourceReferencesRow.tupled((<<[play.api.libs.json.JsValue], <<[play.api.libs.json.JsValue]))
  }
  /** Table description of table resource_references. Objects of this class serve as prototypes for rows in queries. */
  class ResourceReferences(_tableTag: Tag) extends Table[ResourceReferencesRow](_tableTag, "resource_references") {
    def * = (fromId, toId) <> (ResourceReferencesRow.tupled, ResourceReferencesRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(fromId), Rep.Some(toId)).shaped.<>({r=>import r._; _1.map(_=> ResourceReferencesRow.tupled((_1.get, _2.get)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column from_id SqlType(jsonb), Length(2147483647,false) */
    val fromId: Rep[play.api.libs.json.JsValue] = column[play.api.libs.json.JsValue]("from_id", O.Length(2147483647,varying=false))
    /** Database column to_id SqlType(jsonb), Length(2147483647,false) */
    val toId: Rep[play.api.libs.json.JsValue] = column[play.api.libs.json.JsValue]("to_id", O.Length(2147483647,varying=false))

    /** Primary key of ResourceReferences (database name resource_references_pkey) */
    val pk = primaryKey("resource_references_pkey", (fromId, toId))
  }
  /** Collection-like TableQuery object for table ResourceReferences */
  lazy val ResourceReferences = new TableQuery(tag => new ResourceReferences(tag))
}

/** Entity class storing rows of table Changefeed
 *  @param id Database column id SqlType(text), PrimaryKey
 *  @param parentId Database column parent_id SqlType(text), Default(None)
 *  @param typeFilter Database column type_filter SqlType(_text), Length(2147483647,false), Default(None)
 *  @param maxAck Database column max_ack SqlType(int8), Default(0)
 *  @param created Database column created SqlType(timestamptz)
 *  @param lastAck Database column last_ack SqlType(timestamptz) */
case class ChangefeedRow(id: String, parentId: Option[String] = None, typeFilter: Option[List[String]] = None, maxAck: Long = 0L, created: org.joda.time.DateTime, lastAck: org.joda.time.DateTime)

/** Entity class storing rows of table ChangeHistory
 *  @param `type` Database column type SqlType(text)
 *  @param id Database column id SqlType(text)
 *  @param seq Database column seq SqlType(bigserial), AutoInc
 *  @param eventTime Database column event_time SqlType(timestamptz) */
case class ChangeHistoryRow(`type`: String, id: String, seq: Long, eventTime: org.joda.time.DateTime)

/** Entity class storing rows of table ChangeHistoryToExpand
 *  @param key Database column key SqlType(bigserial), AutoInc, PrimaryKey
 *  @param `type` Database column type SqlType(text)
 *  @param id Database column id SqlType(text) */
case class ChangeHistoryToExpandRow(key: Long, `type`: String, id: String)

/** Entity class storing rows of table Resource
 *  @param id Database column id SqlType(jsonb), PrimaryKey, Length(2147483647,false)
 *  @param deleted Database column deleted SqlType(bool)
 *  @param resource Database column resource SqlType(jsonb), Length(2147483647,false)
 *  @param sequenceNumber Database column sequence_number SqlType(int8), Default(None)
 *  @param created Database column created SqlType(timestamptz)
 *  @param updated Database column updated SqlType(timestamptz)
 *  @param outRefs Database column out_refs SqlType(_jsonb), Length(2147483647,false) */
case class ResourceRow(id: play.api.libs.json.JsValue, deleted: Boolean, resource: play.api.libs.json.JsValue, sequenceNumber: Option[Long] = None, created: org.joda.time.DateTime, updated: org.joda.time.DateTime, outRefs: List[play.api.libs.json.JsValue])

/** Entity class storing rows of table ResourceReferences
 *  @param fromId Database column from_id SqlType(jsonb), Length(2147483647,false)
 *  @param toId Database column to_id SqlType(jsonb), Length(2147483647,false) */
case class ResourceReferencesRow(fromId: play.api.libs.json.JsValue, toId: play.api.libs.json.JsValue)
