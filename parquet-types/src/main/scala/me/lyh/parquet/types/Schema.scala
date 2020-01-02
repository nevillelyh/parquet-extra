package me.lyh.parquet.types

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Type, Types}

import scala.collection.JavaConverters._

private object Schema {
  def rename(schema: Type, name: String): Type = {
    if (schema.isPrimitive) {
      Types
        .primitive(schema.asPrimitiveType().getPrimitiveTypeName, schema.getRepetition)
        .as(schema.getLogicalTypeAnnotation)
        .named(name)
    } else {
      schema
        .asGroupType()
        .getFields
        .asScala
        .foldLeft(Types.buildGroup(schema.getRepetition))(_.addField(_))
        .named(name)
    }
  }

  def setRepetition(schema: Type, repetition: Repetition): Type = {
    require(schema.isRepetition(Repetition.REQUIRED))
    if (schema.isPrimitive) {
      Types
        .primitive(schema.asPrimitiveType().getPrimitiveTypeName, repetition)
        .as(schema.getLogicalTypeAnnotation)
        .named(schema.getName)
    } else {
      schema
        .asGroupType()
        .getFields
        .asScala
        .foldLeft(Types.buildGroup(repetition))(_.addField(_))
        .named(schema.getName)
    }
  }

  def primitive(ptn: PrimitiveTypeName, lta: LogicalTypeAnnotation = null): Type =
    Types.required(ptn).as(lta).named(ptn.name())

  def message(schema: Type): MessageType = {
    val builder = Types.buildMessage()
    schema.asGroupType().getFields.asScala.foreach(builder.addField)
    builder.named(schema.getName)
  }
}
