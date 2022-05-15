package me.lyh.parquet.avro

import com.spotify.scio.parquet._
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{
  AvroParquetReader,
  AvroParquetWriter,
  AvroReadSupport,
  AvroWriteSupport
}
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import scala.reflect.{classTag, ClassTag}

class ProjectionReaderTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val tmpDir = sys.props("java.io.tmpdir")
  private val parquetPath: Path = new Path(tmpDir, "test-parquet-" + UUID.randomUUID())
  private val avroPath: Path = new Path(tmpDir, "test-avro-" + UUID.randomUUID())

  override def beforeAll(): Unit = {
    val record = ProjectionTestRecord
      .newBuilder()
      .setField1(1)
      .setField2(2)
      .setField3(3)
      .setField4(4)
      .setField5(5)
      .build()

    val avroWriter = {
      val datumWriter = new SpecificDatumWriter[ProjectionTestRecord]
      val writer = new DataFileWriter[ProjectionTestRecord](datumWriter)
      writer.create(ProjectionTestRecord.getClassSchema, new File(avroPath.toString))
    }
    avroWriter.append(record)
    avroWriter.close()

    val conf = new Configuration
    AvroWriteSupport.setSchema(conf, ProjectionTestRecord.getClassSchema)

    val parquetWriter: ParquetWriter[ProjectionTestRecord] = AvroParquetWriter
      .builder[ProjectionTestRecord](parquetPath)
      .withSchema(ProjectionTestRecord.getClassSchema)
      .withDataModel(GenericData.get())
      .build()
    parquetWriter.write(record)
    parquetWriter.close()
  }

  override def afterAll(): Unit = {
    FileSystem.get(new Configuration).delete(parquetPath, false)
    FileSystem.get(new Configuration).delete(avroPath, false)
  }
  private def readAvro(projection: Schema)(f: GenericRecord => Unit): Unit = {
    val file = new File(avroPath.toString)
    val datumReader: DatumReader[GenericRecord] = new GenericDatumReader(projection)
    val reader = DataFileReader.openReader[GenericRecord](file, datumReader)
    val record: GenericRecord = reader.next(null.asInstanceOf[GenericRecord])
    reader.close()

    f(record)
  }

  private def readParquet[T: ClassTag](projection: Schema = null)(f: T => Unit): Unit = {
    val conf = new Configuration
    val schema = classTag[T].runtimeClass
      .getMethod("getClassSchema")
      .invoke(null)
      .asInstanceOf[Schema]
    AvroReadSupport.setAvroReadSchema(conf, schema)
    if (projection != null) {
      AvroReadSupport.setRequestedProjection(conf, projection)
    }
    val reader = AvroParquetReader.builder[T](parquetPath).withConf(conf).build()
    val record: T = reader.read()
    reader.close()

    f(record)
  }

  "Avro reader" should "support projection" in {
    // Read as incomplete original record
    val projection = Projection[ProjectionTestRecord](_.getField3, _.getField1)
    readAvro(projection) { r =>
      r.get("field1") shouldBe 1
      r.get("field3") shouldBe 3
    }

    // Read as slim specific record, same field order
    readAvro(ProjectionTestRecord1.getClassSchema) { r =>
      r.get("field1") shouldBe 1
      r.get("field3") shouldBe 3
    }

    // Read as slim specific record, different field order
    readAvro(ProjectionTestRecord2.getClassSchema) { r =>
      r.get("field1") shouldBe 1
      r.get("field3") shouldBe 3
    }
  }

  "Parquet reader" should "support projection" in {
    // Read as incomplete original record
    val projection = Projection[ProjectionTestRecord](_.getField3, _.getField1)
    readParquet[ProjectionTestRecord](projection) { r =>
      r.getField1 shouldBe 1
      r.getField3 shouldBe 3
    }

    // Read as slim specific record, same field order
    readParquet[ProjectionTestRecord1](ProjectionTestRecord1.getClassSchema) { r =>
      r.getField1 shouldBe 1
      r.getField3 shouldBe 3
    }

    // Read as slim specific record, different field order
    readParquet[ProjectionTestRecord2](ProjectionTestRecord2.getClassSchema) { r =>
      r.getField1 shouldBe 1
      r.getField3 shouldBe 3
    }
  }
}
