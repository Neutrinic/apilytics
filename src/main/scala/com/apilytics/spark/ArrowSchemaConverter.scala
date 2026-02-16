package com.apilytics.spark

import com.apilytics.core.schema.SchemaMapper
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema => ArrowSchema}
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

/** Converts Arrow Schema to Spark StructType. */
object ArrowSchemaConverter {

  def toSparkSchema(arrowSchema: ArrowSchema): StructType = {
    val fields = arrowSchema.getFields.asScala.map(toSparkField).toArray
    StructType(fields)
  }

  /** Prune Arrow schema to only include columns in the required Spark schema.
    * Preserves field metadata (like json.path) from the original Arrow fields.
    */
  def pruneSchema(arrowSchema: ArrowSchema, required: StructType): ArrowSchema = {
    val requiredNames = required.fieldNames.toSet
    val prunedFields = arrowSchema.getFields.asScala
      .filter(f => requiredNames.contains(f.getName))
      .asJava
    new ArrowSchema(prunedFields)
  }

  private def toSparkField(field: Field): StructField = {
    // Check if this is a variant field - return native VariantType
    val isVariant = Option(field.getMetadata)
      .flatMap(m => Option(m.get(SchemaMapper.VariantKey)))
      .contains("true")

    if (isVariant) {
      StructField(field.getName, VariantType, field.isNullable)
    } else {
      val sparkType = toSparkType(field.getType)
      StructField(field.getName, sparkType, field.isNullable)
    }
  }

  private def toSparkType(arrowType: ArrowType): DataType = arrowType match {
    case _: ArrowType.Utf8 => StringType
    case i: ArrowType.Int =>
      i.getBitWidth match {
        case 32 => IntegerType
        case 64 => LongType
        case _  => IntegerType
      }
    case _: ArrowType.FloatingPoint => DoubleType
    case _: ArrowType.Bool          => BooleanType
    case _: ArrowType.Date          => DateType
    case _: ArrowType.Timestamp     => TimestampType
    case _                          => StringType // fallback
  }
}
