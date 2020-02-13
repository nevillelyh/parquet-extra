package me.lyh.parquet.tensorflow

import com.google.protobuf.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ParquetDecodingException
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  private def read(path: Path, schema: Schema): Seq[Example] = {
    val reader = ExampleParquetReader.builder(path).withSchema(schema).build();
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

  private def bytes(xs: ByteString*) =
    Feature
      .newBuilder()
      .setBytesList(
        xs.foldLeft(BytesList.newBuilder())(_.addValue(_))
      )
      .build()

  private def getFeatures(keys: String*): Example => Seq[(String, Option[Feature])] =
    (e: Example) => {
      keys.map(k => (k, Option(e.getFeatures.getFeatureOrDefault(k, null))))
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
            .putFeature("bytes", bytes(ByteString.copyFromUtf8(i.toString)))
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
    ExampleParquetInputFormat.setSchema(job, schema)
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
    b.result() shouldEqual xs
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
            .putFeature("bytes", bytes(ByteString.copyFromUtf8(i.toString)))
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
            .putFeature("bytes", bytes(ByteString.copyFromUtf8(i.toString)))
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

    val reader2 = Schema
      .newBuilder()
      .required("long", Schema.Type.INT64)
      .named("Reader2")
    val f = getFeatures("long")
    read(temp, reader2).map(f) shouldEqual xs.map(f)

    val reader3 = Schema
      .newBuilder()
      .required("float", Schema.Type.FLOAT)
      .named("Reader3")
    val g = getFeatures("float")
    read(temp, reader3).map(g) shouldEqual xs.map(g)
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

    val reader2 = Schema
      .newBuilder()
      .repeated("l", Schema.Type.INT64)
      .named("Reader2")
    val f = getFeatures("l")
    read(temp, reader2).map(f) shouldEqual xs.map(f)

    val reader3 = Schema
      .newBuilder()
      .optional("o", Schema.Type.INT64)
      .named("Reader3")
    val g = getFeatures("o")
    read(temp, reader3).map(g) shouldEqual xs.map(g)
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

    val getR = getFeatures("r")
    val getO = getFeatures("o")

    // widening repetition
    val r3 = Schema.newBuilder().optional("r", Schema.Type.INT64).named("R3")
    read(temp, r3).map(getR) shouldEqual xs.map(getR)
    val r4 = Schema.newBuilder().repeated("o", Schema.Type.INT64).named("R4")
    read(temp, r4).map(getO) shouldEqual xs.map(getO)

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
    read(temp, r6).map(getRX) shouldEqual xs.map(getRX)
    val r7 = Schema
      .newBuilder()
      .required("r", Schema.Type.INT64)
      .repeated("x", Schema.Type.INT64)
      .named("R7")
    read(temp, r7).map(getRX) shouldEqual xs.map(getRX)

    val r8 = Schema.newBuilder().required("r", Schema.Type.FLOAT).named("R8")
    testException(
      read(temp, r8),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: required float r != required int64 r (INTEGER(64,true))"
    )
  }
}
