package me.lyh.parquet.avro

import com.spotify.scio.parquet.ParquetTestRecord
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport, AvroWriteSupport}
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class SchemaEvolutionTest extends AnyFlatSpec with Matchers {
  def writeRecordsParquet(): Path = {
    val conf = new Configuration()
    AvroWriteSupport.setSchema(conf, ParquetTestRecord.SCHEMA$)
    val path: Path = new Path("parquet-extra", "test-parquet-" + UUID.randomUUID())
    new File(path.toString).deleteOnExit()

    val record = ParquetTestRecord.newBuilder()
      .setField1(1)
      .setField2(2)
      .setField3(3)
      .setField4(4)
      .setField5(5)
      .build()

    val parquetWriter: ParquetWriter[ParquetTestRecord] = AvroParquetWriter
      .builder[ParquetTestRecord](path)
      .withSchema(ParquetTestRecord.SCHEMA$)
      .withDataModel(GenericData.get())
      .build()

    parquetWriter.write(record)
    parquetWriter.close()
    path
  }

  def writeRecordsAvro(): File = {
    val file = new File("parquet-extra/test-avro-" + UUID.randomUUID())
    file.deleteOnExit()

    val record = ParquetTestRecord.newBuilder()
      .setField1(1)
      .setField2(2)
      .setField3(3)
      .setField4(4)
      .setField5(5)
      .build()

    val avroWriter = {
      val reflectDatumWriter = new SpecificDatumWriter[ParquetTestRecord]()
      val writer = new DataFileWriter[ParquetTestRecord](reflectDatumWriter)
      writer.create(ParquetTestRecord.SCHEMA$, file)
    }
    avroWriter.append(record)
    avroWriter.close()

    file
  }

  // Test reading Parquet-Avro records using a projection of the ORIGINAL schema
  "ParquetAvroReader" should "support re-ordered fields" in {
    val inputPath = writeRecordsParquet()

    val conf = new Configuration()
    AvroReadSupport.setAvroReadSchema(conf, ParquetTestRecord.SCHEMA$)
    AvroReadSupport.setRequestedProjection(conf, Projection[ParquetTestRecord](_.getField3, _.getField1))

    val parquetReader = AvroParquetReader
      .builder[ParquetTestRecord](inputPath)
      .withConf(conf)
      .build()

    val readRecord = parquetReader.read()
    readRecord.getField3 shouldBe 3
  }

  "Vanilla Avro reader" should "support re-ordered fields" in {
    val projection = Projection[ParquetTestRecord](_.getField3, _.getField1)
    val inputFile = writeRecordsAvro()
    val avroReader = {
      val datumReader: DatumReader[GenericRecord] = new GenericDatumReader(projection)
      DataFileReader.openReader[GenericRecord](inputFile, datumReader)
    }

    val readRecord = avroReader.next()
    readRecord.get("field3") shouldBe 3
  }
}
