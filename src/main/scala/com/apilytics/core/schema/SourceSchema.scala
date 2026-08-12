package com.apilytics.core.schema

/** Protocol-neutral description of a value returned by a data source.
  *
  * Deliberately not tied to OpenAPI: the shape here is what every protocol we plan
  * to support can express (see #191). OpenAPI, GraphQL introspection and protobuf
  * descriptors all get parsed *into* this by their own protocol module, and
  * everything downstream — schema flattening, Arrow conversion, the Spark layer —
  * only ever sees this.
  */
@SerialVersionUID(1L)
sealed trait SourceSchema extends Serializable

object SourceSchema {
  @SerialVersionUID(1L) final case class StringType(format: Option[String] = None) extends SourceSchema
  @SerialVersionUID(1L) final case class IntegerType(format: Option[String] = None) extends SourceSchema
  @SerialVersionUID(1L) final case class NumberType(format: Option[String] = None) extends SourceSchema
  @SerialVersionUID(1L) case object BooleanType extends SourceSchema
  @SerialVersionUID(1L) final case class ArrayType(items: SourceSchema) extends SourceSchema
  @SerialVersionUID(1L) final case class ObjectType(
      properties: Map[String, SourceSchema],
      required: Set[String] = Set.empty
  ) extends SourceSchema

  /** Value whose shape cannot be pinned down by the source's own schema language.
    *
    * From OpenAPI that means additionalProperties, an empty object, a missing type,
    * or anyOf/oneOf. In strict mode this becomes a JSON string column; only in
    * variant mode does it reach Spark's native VARIANT.
    */
  @SerialVersionUID(1L) case object VariantType extends SourceSchema

  @SerialVersionUID(1L) case object UnknownType extends SourceSchema
}
