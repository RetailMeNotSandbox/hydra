package core.json

import play.api.libs.json.{JsValue, Reads, Writes}

object EitherImplicits {

  implicit def EitherReads[A, B](implicit fmtA: Reads[A], fmtB: Reads[B]): Reads[Either[A, B]] = new Reads[Either[A, B]] {
    def reads(json: JsValue) = fmtA.reads(json).map(l => Left[A, B](l)).orElse(fmtB.reads(json).map(r => Right[A, B](r)))
  }

  implicit def EitherWrites[A, B](implicit fmtA: Writes[A], fmtB: Writes[B]): Writes[Either[A, B]] = new Writes[Either[A, B]] {
    def writes(item: Either[A, B]) = item.fold(fmtA.writes, fmtB.writes)
  }
}
