package me.lyh.parquet.tensorflow

import com.google.protobuf.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID}
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetReader}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ParquetDecodingException
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ParquetExampleTest extends AnyFlatSpec with Matchers {
  private val fs = FileSystem.getLocal(new Configuration())

  private def makeTemp: Path = {
    val tmp = sys.props("java.io.tmpdir")
    val ts = System.currentTimeMillis()
    val p = new Path(s"$tmp/parquet-tensorflow-$ts.parquet")
    fs.deleteOnExit(p)
    p
  }

  private def write(path: Path, schema: Schema, xs: Seq[Example]): Unit = {
    val writer = ExampleParquetWriter.builder(path).withSchema(schema).build()
    xs.foreach(writer.write)
    writer.close()
  }

  private def read(path: Path, schema: Schema): Seq[Example] =
    read(ExampleParquetReader.builder(path).withSchema(schema).build())

  private def read(path: Path, fields: Seq[String]): Seq[Example] =
    read(ExampleParquetReader.builder(path).withFields(fields.asJava).build())

  private def read(reader: ParquetReader[Example]): Seq[Example] = {
    val b = Seq.newBuilder[Example]
    var r = reader.read()
    while (r != null) {
      b += r
      r = reader.read()
    }
    b.result()
  }

  private def roundTrip(schema: Schema, xs: Seq[Example]): Seq[Example] = {
    val temp = makeTemp
    write(temp, schema, xs)
    read(temp, schema)
  }

  private def longs(xs: Long*) =
    Feature
      .newBuilder()
      .setInt64List(
        xs.foldLeft(Int64List.newBuilder())(_.addValue(_))
      )
      .build()

  private def floats(xs: Float*) =
    Feature
      .newBuilder()
      .setFloatList(
        xs.foldLeft(FloatList.newBuilder())(_.addValue(_))
      )
      .build()

  private def bytes(xs: String*) =
    Feature
      .newBuilder()
      .setBytesList(
        xs.map(ByteString.copyFromUtf8).foldLeft(BytesList.newBuilder())(_.addValue(_))
      )
      .build()

  private def getFeatures(keys: String*): Example => Example = {
    val keySet = keys.toSet
    (e: Example) => {
      val features = e.getFeatures.getFeatureMap.asScala
        .foldLeft(Features.newBuilder()) {
          case (b, (k, v)) =>
            if (keySet.contains(k)) {
              b.putFeature(k, v)
            } else {
              b
            }
        }
        .build()
      Example.newBuilder().setFeatures(features).build()
    }
  }

  private def testException(fun: => Any, msgs: String*) = {
    val e = the[ParquetDecodingException] thrownBy fun
    msgs.foreach(m => e.getCause.getMessage should include(m))
  }

  "ParquetExample" should "work with Hadoop" in {
    val temp = makeTemp
    val schema = Schema
      .newBuilder()
      .required("long", Schema.Type.INT64)
      .required("float", Schema.Type.FLOAT)
      .required("bytes", Schema.Type.BYTES)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      Example
        .newBuilder()
        .setFeatures(
          Features
            .newBuilder()
            .putFeature("long", longs(i))
            .putFeature("float", floats(i))
            .putFeature("bytes", bytes(i.toString))
        )
        .build()
    }
    val job = Job.getInstance()

    job.setOutputFormatClass(classOf[ExampleParquetOutputFormat])
    ExampleParquetOutputFormat.setSchema(job, schema)
    val outputFormat = new ExampleParquetOutputFormat()
    val writer = outputFormat.getRecordWriter(job.getConfiguration, temp, CompressionCodecName.GZIP)
    xs.foreach(writer.write(null, _))
    writer.close(null)

    job.setInputFormatClass(classOf[ExampleParquetInputFormat])

    def read(): Seq[Example] = {
      val inputFormat = new ExampleParquetInputFormat()
      val context = new TaskAttemptContextImpl(job.getConfiguration, new TaskAttemptID())
      FileInputFormat.setInputPaths(job, temp)
      val split = inputFormat.getSplits(job).get(0)
      val reader = inputFormat.createRecordReader(split, context)
      reader.initialize(split, context)
      val b = Seq.newBuilder[Example]
      while (reader.nextKeyValue()) {
        b += reader.getCurrentValue
      }
      reader.close()
      b.result()
    }

    // no schema or fields
    ParquetInputFormat.setReadSupportClass(job, classOf[ExampleReadSupport])
    read() shouldEqual xs

    // writer schema
    ExampleParquetInputFormat.setSchema(job, schema)
    read() shouldEqual xs
    job.getConfiguration.unset(ExampleParquetInputFormat.SCHEMA_KEY)

    // projected schema
    ExampleParquetInputFormat.setSchema(
      job,
      Schema.newBuilder().required("long", Schema.Type.INT64).named("Schema")
    )
    read() shouldEqual xs.map(getFeatures("long"))
    job.getConfiguration.unset(ExampleParquetInputFormat.SCHEMA_KEY)

    // all fields
    ExampleParquetInputFormat.setFields(job, Seq("long", "float", "bytes").asJava)
    read() shouldEqual xs
    job.getConfiguration.unset(ExampleParquetInputFormat.FIELDS_KEY)

    // projected fields
    ExampleParquetInputFormat.setFields(job, Seq("long").asJava)
    read() shouldEqual xs.map(getFeatures("long"))
    job.getConfiguration.unset(ExampleParquetInputFormat.FIELDS_KEY)
  }

  it should "round trip primitives" in {
    val schema = Schema
      .newBuilder()
      .required("long", Schema.Type.INT64)
      .required("float", Schema.Type.FLOAT)
      .required("bytes", Schema.Type.BYTES)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      Example
        .newBuilder()
        .setFeatures(
          Features
            .newBuilder()
            .putFeature("long", longs(i))
            .putFeature("float", floats(i))
            .putFeature("bytes", bytes(i.toString))
        )
        .build()
    }
    roundTrip(schema, xs) shouldEqual xs
  }

  it should "round trip repetition" in {
    val schema = Schema
      .newBuilder()
      .optional("o", Schema.Type.INT64)
      .repeated("l", Schema.Type.INT64)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      val b = Features.newBuilder()
      if (i % 2 == 0) b.putFeature("o", longs(i))
      if (i > 0) b.putFeature("l", longs((0 until i).map(_.toLong): _*))
      Example.newBuilder().setFeatures(b).build()
    }
    roundTrip(schema, xs) shouldEqual xs
  }

  it should "round trip primitives projection" in {
    val schema = Schema
      .newBuilder()
      .required("long", Schema.Type.INT64)
      .required("float", Schema.Type.FLOAT)
      .required("bytes", Schema.Type.BYTES)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      Example
        .newBuilder()
        .setFeatures(
          Features
            .newBuilder()
            .putFeature("long", longs(i))
            .putFeature("float", floats(i))
            .putFeature("bytes", bytes(i.toString))
        )
        .build()
    }
    val temp = makeTemp
    write(temp, schema, xs)

    val reader1 = Schema
      .newBuilder()
      .required("bytes", Schema.Type.BYTES)
      .required("float", Schema.Type.FLOAT)
      .required("long", Schema.Type.INT64)
      .named("Reader1")
    read(temp, reader1) shouldEqual xs
    read(temp, Seq("bytes", "float", "long")) shouldEqual xs

    val reader2 = Schema
      .newBuilder()
      .required("long", Schema.Type.INT64)
      .named("Reader2")
    read(temp, reader2) shouldEqual xs.map(getFeatures("long"))
    read(temp, Seq("long")) shouldEqual xs.map(getFeatures("long"))

    val reader3 = Schema
      .newBuilder()
      .required("float", Schema.Type.FLOAT)
      .named("Reader3")
    read(temp, reader3) shouldEqual xs.map(getFeatures("float"))
    read(temp, Seq("float")) shouldEqual xs.map(getFeatures("float"))
  }

  it should "round trip repetition projection" in {
    val schema = Schema
      .newBuilder()
      .optional("o", Schema.Type.INT64)
      .repeated("l", Schema.Type.INT64)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      val b = Features.newBuilder()
      if (i % 2 == 0) b.putFeature("o", longs(i))
      if (i > 0) b.putFeature("l", longs((0 until i).map(_.toLong): _*))
      Example.newBuilder().setFeatures(b).build()
    }
    val temp = makeTemp
    write(temp, schema, xs)

    val reader1 = Schema
      .newBuilder()
      .repeated("l", Schema.Type.INT64)
      .optional("o", Schema.Type.INT64)
      .named("Reader1")
    read(temp, reader1) shouldEqual xs
    read(temp, Seq("l", "o")) shouldEqual xs

    val reader2 = Schema
      .newBuilder()
      .repeated("l", Schema.Type.INT64)
      .named("Reader2")
    read(temp, reader2) shouldEqual xs.map(getFeatures("l"))
    read(temp, Seq("l")) shouldEqual xs.map(getFeatures("l"))

    val reader3 = Schema
      .newBuilder()
      .optional("o", Schema.Type.INT64)
      .named("Reader3")
    read(temp, reader3) shouldEqual xs.map(getFeatures("o"))
    read(temp, Seq("o")) shouldEqual xs.map(getFeatures("o"))
  }

  it should "support schema evolution" in {
    val schema = Schema
      .newBuilder()
      .required("r", Schema.Type.INT64)
      .optional("o", Schema.Type.INT64)
      .repeated("l", Schema.Type.INT64)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      val b = Features.newBuilder().putFeature("r", longs(i))
      if (i % 2 == 0) b.putFeature("o", longs(i))
      if (i > 0) b.putFeature("l", longs((0 until i).map(_.toLong): _*))
      Example.newBuilder().setFeatures(b).build()
    }
    val temp = makeTemp
    write(temp, schema, xs)

    // narrowing repetition
    val r1 = Schema.newBuilder().required("o", Schema.Type.INT64).named("R1")
    testException(
      read(temp, r1),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: required int64 o (INTEGER(64,true)) != optional int64 o (INTEGER(64,true))"
    )
    val r2 = Schema.newBuilder().optional("l", Schema.Type.INT64).named("R2")
    testException(
      read(temp, r2),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: optional int64 l (INTEGER(64,true)) != repeated int64 l (INTEGER(64,true))"
    )

    // widening repetition
    val r3 = Schema.newBuilder().optional("r", Schema.Type.INT64).named("R3")
    read(temp, r3) shouldEqual xs.map(getFeatures("r"))
    val r4 = Schema.newBuilder().repeated("o", Schema.Type.INT64).named("R4")
    read(temp, r4) shouldEqual xs.map(getFeatures("o"))

    // new fields
    val r5 = Schema
      .newBuilder()
      .required("r", Schema.Type.INT64)
      .required("x", Schema.Type.INT64)
      .named("R5")
    testException(read(temp, r5), "Failed to decode R5#x: Required field size != 1: 0")
    val getRX = getFeatures("r", "x")
    val r6 = Schema
      .newBuilder()
      .required("r", Schema.Type.INT64)
      .optional("x", Schema.Type.INT64)
      .named("R6")
    read(temp, r6) shouldEqual xs.map(getRX)
    val r7 = Schema
      .newBuilder()
      .required("r", Schema.Type.INT64)
      .repeated("x", Schema.Type.INT64)
      .named("R7")
    read(temp, r7) shouldEqual xs.map(getRX)

    val r8 = Schema.newBuilder().required("r", Schema.Type.FLOAT).named("R8")
    testException(
      read(temp, r8),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: required float r != required int64 r (INTEGER(64,true))"
    )
  }

  it should "fail unmatched fields" in {
    val schema = Schema
      .newBuilder()
      .required("long", Schema.Type.INT64)
      .required("float", Schema.Type.FLOAT)
      .required("bytes", Schema.Type.BYTES)
      .named("Schema")
    val xs = (0 until 10).map { i =>
      Example
        .newBuilder()
        .setFeatures(
          Features
            .newBuilder()
            .putFeature("long", longs(i))
            .putFeature("float", floats(i))
            .putFeature("bytes", bytes(i.toString))
        )
        .build()
    }
    val temp = makeTemp
    write(temp, schema, xs)

    val reader = ExampleParquetReader
      .builder(temp)
      .withFields(Seq("long", "float", "bytes", "x", "y", "z").asJava)
      .build()
    val msg = "Invalid fields: [x, y, z]"
    the[IllegalStateException] thrownBy reader.read() should have message msg
  }
}
