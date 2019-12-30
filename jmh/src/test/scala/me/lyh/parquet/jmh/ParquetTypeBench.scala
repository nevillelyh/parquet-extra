package me.lyh.parquet.jmh

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import org.apache.parquet.io._
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class ParquetAvroBench {
  import org.apache.avro.Schema
  import org.apache.avro.generic.{GenericData, GenericRecord}
  import org.apache.parquet.avro._

  private val schema = Schema.createRecord("Record", "me.lyh", null, false)
  schema.setFields(
    List(
      new Schema.Field("long", Schema.createArray(Schema.create(Schema.Type.LONG))),
      new Schema.Field("float", Schema.createArray(Schema.create(Schema.Type.FLOAT))),
      new Schema.Field("bytes", Schema.createArray(Schema.create(Schema.Type.BYTES)))
    ).asJava
  )
  private val record = new GenericData.Record(schema)
  record.put("long", (1 to 10).map(_.toLong).asJava)
  record.put("float", (1 to 10).map(_.toFloat).asJava)
  record.put("bytes", (1 to 10).map(_.toString.getBytes).asJava)
  private val xs = Array.fill(1000)(record)
  private val bytes = write.toByteArray

  @Benchmark def write: ByteArrayOutputStream = {
    val file = new TestOutputFile
    val writer = AvroParquetWriter.builder[GenericRecord](file).withSchema(schema).build()
    xs.foreach(writer.write)
    writer.close()
    file.stream
  }

  @Benchmark def read: Int = {
    val file = new TestInputFile(bytes)
    val reader = AvroParquetReader.builder[GenericRecord](file).build()
    var r = reader.read()
    var c = 0
    while (r != null) {
      c += 1
      r = reader.read()
    }
    c
  }
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class ParquetTypeBench {
  import me.lyh.parquet.types._

  private val record = Record(
    (1 to 10).map(_.toLong),
    (1 to 10).map(_.toFloat),
    (1 to 10).map(_.toString)
  )
  private val xs = Array.fill(1000)(record)
  private val bytes = write.toByteArray

  @Benchmark def write: ByteArrayOutputStream = {
    val file = new TestOutputFile
    val writer = TypeParquetWriter.builder[Record](file).build()
    xs.foreach(writer.write)
    writer.close()
    file.stream
  }

  @Benchmark def read: Int = {
    val file = new TestInputFile(bytes)
    val reader = TypeParquetReader.builder[Record](file).build()
    var r = reader.read()
    var c = 0
    while (r != null) {
      c += 1
      r = reader.read()
    }
    c
  }
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class ParquetTensorFlowBench {
  import me.lyh.parquet.tensorflow._
  import org.tensorflow.example._

  private val schema = Schema
    .newBuilder()
    .repeated("long", Schema.Type.INT64)
    .repeated("float", Schema.Type.FLOAT)
    .repeated("bytes", Schema.Type.BYTES)
    .named("Schema")
  private val record = Example
    .newBuilder()
    .setFeatures(
      Features
        .newBuilder()
        .putFeature(
          "long",
          Feature
            .newBuilder()
            .setInt64List(
              Int64List
                .newBuilder()
                .addAllValue((1 to 10).map(_.toLong.asInstanceOf[java.lang.Long]).asJava)
                .build()
            )
            .build()
        )
        .putFeature(
          "float",
          Feature
            .newBuilder()
            .setFloatList(
              FloatList
                .newBuilder()
                .addAllValue((1 to 10).map(_.toFloat.asInstanceOf[java.lang.Float]).asJava)
                .build()
            )
            .build()
        )
        .putFeature(
          "bytes",
          Feature
            .newBuilder()
            .setBytesList(
              BytesList
                .newBuilder()
                .addAllValue((1 to 10).map(x => ByteString.copyFromUtf8(x.toString)).asJava)
                .build()
            )
            .build()
        )
        .build()
    )
    .build()
  private val xs = Array.fill(1000)(record)
  private val bytes = write.toByteArray

  @Benchmark def write: ByteArrayOutputStream = {
    val file = new TestOutputFile
    val writer = ExampleParquetWriter.builder(file).withSchema(schema).build()
    xs.foreach(writer.write)
    writer.close()
    file.stream
  }

  @Benchmark def read: Int = {
    val file = new TestInputFile(bytes)
    val reader = ExampleParquetReader.builder(file).withSchema(schema).build()
    var r = reader.read()
    var c = 0
    while (r != null) {
      c += 1
      r = reader.read()
    }
    c
  }
}

case class Record(long: Seq[Long], float: Seq[Float], bytes: Seq[String])

class TestOutputFile extends OutputFile {
  private val baos = new ByteArrayOutputStream()

  override def create(blockSizeHint: Long): PositionOutputStream = {
    new DelegatingPositionOutputStream(baos) {
      override def getPos: Long = baos.size()
    }
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = create(blockSizeHint)
  override def supportsBlockSize(): Boolean = false
  override def defaultBlockSize(): Long = 0

  def stream: ByteArrayOutputStream = baos
}

class TestInputFile(bytes: Array[Byte]) extends InputFile {
  private val bais = new ByteArrayInputStream(bytes)

  override def getLength: Long = bytes.length

  override def newStream(): SeekableInputStream = new DelegatingSeekableInputStream(bais) {
    override def getPos: Long = bytes.length - bais.available()
    override def seek(newPos: Long): Unit = {
      bais.reset()
      bais.skip(newPos)
    }
  }
}
