// http://danielwestheide.com/blog/2015/06/28/put-your-writes-where-your-master-is-compile-time-restriction-of-slick-effect-types.html

package core

import slick.dbio.Effect
import slick.dbio.Effect.{Transactional, Write, Read}

import scala.annotation.implicitNotFound

package object db {
  sealed trait Role
  trait CanWriteRole extends Role
  trait CanReadRole extends Role

  @implicitNotFound("'${R}' database is not privileged to to perform effect '${E}'.")
  trait HasPrivilege[R <: Role, E <: Effect]
  // You may get this error with the type 'Effect' (i.e. "... is not privileged to to perform effect
  // 'slick.dbio.Effect'"). When this happens, it's because Slick can't infer the effect type of the action. This is
  // always the case with plain SQL queries. The fix is to annotate the action with an appropriate type.

  type ReadWriteTransaction = Read with Write with Transactional
  type ReadTransaction = Read with Transactional
  type WriteTransaction = Write with Transactional

  implicit val replicaCanRead: CanReadRole HasPrivilege Read = null
  implicit val replicaCanReadTransact: CanReadRole HasPrivilege ReadTransaction = null
  implicit val primaryCanRead: CanWriteRole HasPrivilege Read = null
  implicit val primaryCanWrite: CanWriteRole HasPrivilege Write = null
  implicit val primaryCanReadTransact: CanWriteRole HasPrivilege ReadTransaction = null
  implicit val primaryCanWriteTransact: CanWriteRole HasPrivilege WriteTransaction = null
  implicit val primaryCanPerformTransactions: CanWriteRole HasPrivilege ReadWriteTransaction = null
}
