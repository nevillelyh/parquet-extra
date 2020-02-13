package me.lyh.parquet.tensorflow

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaTest extends AnyFlatSpec with Matchers {

  private val schema = Schema
    .newBuilder()
    .required("i", Schema.Type.INT64)
    .required("f", Schema.Type.FLOAT)
    .required("b", Schema.Type.BYTES)
    .optional("oi", Schema.Type.INT64)
    .optional("of", Schema.Type.FLOAT)
    .optional("ob", Schema.Type.BYTES)
    .repeated("ri", Schema.Type.INT64)
    .repeated("rf", Schema.Type.FLOAT)
    .repeated("rb", Schema.Type.BYTES)
    .named("Schema")

  "Schema" should "round-trip Parquet" in {
    val parquet = Types
      .buildMessage()
      .addField(
        Types
          .primitive(PrimitiveTypeName.INT64, Repetition.REQUIRED)
          .as(LogicalTypeAnnotation.intType(64, true))
          .named("i")
      )
      .addField(Types.primitive(PrimitiveTypeName.FLOAT, Repetition.REQUIRED).named("f"))
      .addField(Types.primitive(PrimitiveTypeName.BINARY, Repetition.REQUIRED).named("b"))
      .addField(
        Types
          .primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
          .as(LogicalTypeAnnotation.intType(64, true))
          .named("oi")
      )
      .addField(Types.primitive(PrimitiveTypeName.FLOAT, Repetition.OPTIONAL).named("of"))
      .addField(Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).named("ob"))
      .addField(
        Types
          .primitive(PrimitiveTypeName.INT64, Repetition.REPEATED)
          .as(LogicalTypeAnnotation.intType(64, true))
          .named("ri")
      )
      .addField(Types.primitive(PrimitiveTypeName.FLOAT, Repetition.REPEATED).named("rf"))
      .addField(Types.primitive(PrimitiveTypeName.BINARY, Repetition.REPEATED).named("rb"))
      .named("Schema")

    schema.toParquet shouldBe parquet
    Schema.fromParquet(schema.toParquet) shouldBe schema
  }

  it should "round-trip JSON" in {
    Schema.fromJson(schema.toJson) shouldBe schema
  }
}
