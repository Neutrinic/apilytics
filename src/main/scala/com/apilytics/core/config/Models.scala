package com.apilytics.core.config

import scala.concurrent.duration.FiniteDuration

sealed trait AuthType
object AuthType {
  case object Bearer extends AuthType
  case object Basic extends AuthType
  case object Header extends AuthType
  case object OAuth2Client extends AuthType
}

sealed trait PaginationStyle
object PaginationStyle {
  case object Cursor extends PaginationStyle
  case object Offset extends PaginationStyle
  case object LinkHeader extends PaginationStyle
  case object None extends PaginationStyle
}

sealed trait ArrayHandling
object ArrayHandling {
  case object KeepArray extends ArrayHandling
  case object ExplodeView extends ArrayHandling
  case object Both extends ArrayHandling
}

final case class AuthConfig(
    authType: AuthType,
    token: Option[String] = None,
    username: Option[String] = None,
    password: Option[String] = None,
    headerName: Option[String] = None,
    headerValue: Option[String] = None,
    clientId: Option[String] = None,
    clientSecret: Option[String] = None,
    tokenUrl: Option[String] = None
)

final case class PaginationConfig(
    style: PaginationStyle = PaginationStyle.None,
    cursorPath: Option[String] = None,
    cursorParam: Option[String] = None,
    offsetParam: Option[String] = None,
    pageSizeParam: Option[String] = None,
    maxPageSize: Int = 100
)

final case class SchemaConfig(
    flattenDepth: Int = 2,
    arrayHandling: ArrayHandling = ArrayHandling.KeepArray,
    arrowBatchSize: Int = 4096
)

final case class HttpConfig(
    maxRetries: Int = 5,
    maxBackoff: FiniteDuration,
    timeout: FiniteDuration
)

final case class FilterConfig(
    param: String,
    column: String,
    operators: List[String]
)

final case class TableConfig(
    endpoint: String,
    dataPath: Option[String] = None,
    filters: List[FilterConfig] = Nil
)

final case class SourceConfig(
    openapi: String,
    auth: AuthConfig,
    pagination: PaginationConfig = PaginationConfig(),
    schema: SchemaConfig = SchemaConfig(),
    http: HttpConfig,
    tables: Map[String, TableConfig] = Map.empty
)
