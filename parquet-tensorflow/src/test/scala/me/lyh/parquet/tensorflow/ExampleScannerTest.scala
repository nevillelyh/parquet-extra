package me.lyh.parquet.tensorflow

import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}

import scala.collection.JavaConverters._

class ExampleScannerTest extends AnyFlatSpec with Matchers {
  private def longs(xs: Long*): Feature =
    Feature
      .newBuilder()
      .setInt64List(Int64List.newBuilder().addAllValue(xs.asInstanceOf[Seq[java.lang.Long]].asJava))
      .build()

  private def floats(xs: Float*): Feature =
    Feature
      .newBuilder()
      .setFloatList(
        FloatList.newBuilder().addAllValue(xs.asInstanceOf[Seq[java.lang.Float]].asJava)
      )
      .build()

  private def bytes(xs: String*): Feature =
    Feature
      .newBuilder()
      .setBytesList(BytesList.newBuilder().addAllValue(xs.map(ByteString.copyFromUtf8).asJava))
      .build()

  private def example(xs: (String, Feature)*): Example =
    Example
      .newBuilder()
      .setFeatures(Features.newBuilder().putAllFeature(xs.toMap.asJava))
      .build()

  private def getSchema(xs: Seq[Example]): Schema =
    xs.foldLeft(new ExampleScanner("Schema"))(_.scan(_)).getSchema()

  "ExampleScanner" should "support required" in {
    val examples = Seq(
      example("a" -> longs(1L), "b" -> floats(1.0f), "c" -> bytes("x")),
      example("a" -> longs(2L), "b" -> floats(2.0f), "c" -> bytes("y")),
      example("a" -> longs(3L), "b" -> floats(3.0f), "c" -> bytes("z"))
    )

    val schema = Schema
      .newBuilder()
      .required("a", Schema.Type.INT64)
      .required("b", Schema.Type.FLOAT)
      .required("c", Schema.Type.BYTES)
      .named("Schema")

    getSchema(examples.take(1)) shouldBe schema
    getSchema(examples.take(2)) shouldBe schema
    getSchema(examples) shouldBe schema
  }

  it should "support optional" in {
    val a = example("a" -> longs(10L))
    val b = example("b" -> floats(10.0f))
    val c = example("c" -> bytes("z"))
    val examples = Seq(a, b, c)

    val schema = Schema
      .newBuilder()
      .optional("a", Schema.Type.INT64)
      .optional("b", Schema.Type.FLOAT)
      .optional("c", Schema.Type.BYTES)
      .named("Schema")

    getSchema(examples) shouldBe schema
    getSchema(examples ++ Seq(example())) shouldBe schema
    getSchema(examples ++ Seq(Example.getDefaultInstance)) shouldBe schema
    getSchema(examples ++ Seq(example("a" -> longs()))) shouldBe schema
    getSchema(examples ++ Seq(example("a" -> Feature.getDefaultInstance))) shouldBe schema
    getSchema(examples ++ Seq(example("b" -> floats()))) shouldBe schema
    getSchema(examples ++ Seq(example("b" -> Feature.getDefaultInstance))) shouldBe schema
    getSchema(examples ++ Seq(example("c" -> bytes()))) shouldBe schema
    getSchema(examples ++ Seq(example("c" -> Feature.getDefaultInstance))) shouldBe schema
  }

  it should "support repeated" in {
    val a = example("a" -> longs(10L, 100L))
    val b = example("b" -> floats(10.0f, 100.0f))
    val c = example("c" -> bytes("z", "Z"))
    val examples = Seq(a, b, c)

    val schema = Schema
      .newBuilder()
      .repeated("a", Schema.Type.INT64)
      .repeated("b", Schema.Type.FLOAT)
      .repeated("c", Schema.Type.BYTES)
      .named("Schema")

    getSchema(examples) shouldBe schema
    getSchema(examples ++ Seq(example())) shouldBe schema
    getSchema(examples ++ Seq(Example.getDefaultInstance)) shouldBe schema
    getSchema(examples ++ Seq(example("a" -> longs()))) shouldBe schema
    getSchema(examples ++ Seq(example("a" -> longs(1000L)))) shouldBe schema
    getSchema(examples ++ Seq(example("a" -> Feature.getDefaultInstance))) shouldBe schema
    getSchema(examples ++ Seq(example("b" -> floats()))) shouldBe schema
    getSchema(examples ++ Seq(example("b" -> floats(1000.0f)))) shouldBe schema
    getSchema(examples ++ Seq(example("b" -> Feature.getDefaultInstance))) shouldBe schema
    getSchema(examples ++ Seq(example("c" -> bytes()))) shouldBe schema
    getSchema(examples ++ Seq(example("c" -> bytes("ZZZ")))) shouldBe schema
    getSchema(examples ++ Seq(example("c" -> Feature.getDefaultInstance))) shouldBe schema
  }
}
