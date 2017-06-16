package core.modules

import javax.inject._

import play.api.http.HttpFilters
import play.api.mvc._
import play.filters.gzip.GzipFilter

class Filters @Inject() (metric: MetricsFilter, gzipFilter: GzipFilter) extends HttpFilters {
  override def filters: Seq[EssentialFilter] = Seq(
    metric,
    gzipFilter
  )
}
