package me.lyh.parquet.types

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetWriter}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.api.RecordConsumer

class TypeWriteSupport[T] extends WriteSupport[T] {
  private var parquetType: ParquetType[T] = null
  private var recordConsumer: RecordConsumer = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    if (parquetType == null) {
      parquetType =
        SerializationUtils.fromBase64(configuration.get(TypeParquetOutputFormat.ParquetTypeKey))
    }
    val schema = Schema.message(parquetType.schema)
    new WriteSupport.WriteContext(schema, java.util.Collections.emptyMap())
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit =
    this.recordConsumer = recordConsumer

  override def write(record: T): Unit = {
    recordConsumer.startMessage()
    parquetType.write(recordConsumer, record)
    recordConsumer.endMessage()
  }
}

object TypeWriteSupport {
  def apply[T](implicit t: ParquetType[T]): TypeWriteSupport[T] = {
    val s = new TypeWriteSupport[T]
    s.parquetType = t
    s
  }
}

object TypeParquetWriter {
  def builder[T: ParquetType](file: OutputFile): Builder[T] =
    new ParquetWriter.Builder[T, Builder[T]](file) with Builder[T] {
      override protected val writeSupport: TypeWriteSupport[T] = TypeWriteSupport[T]
    }

  def builder[T: ParquetType](path: Path): Builder[T] =
    new ParquetWriter.Builder[T, Builder[T]](path) with Builder[T] {
      override protected val writeSupport: TypeWriteSupport[T] = TypeWriteSupport[T]
    }

  trait Builder[T] extends ParquetWriter.Builder[T, Builder[T]] {
    protected val writeSupport: TypeWriteSupport[T]
    override def self(): Builder[T] = this
    override protected def getWriteSupport(conf: Configuration): WriteSupport[T] = writeSupport
  }
}

class TypeParquetOutputFormat[T] extends ParquetOutputFormat[T](new TypeWriteSupport[T])

object TypeParquetOutputFormat {
  val ParquetTypeKey = "parquet.type.write.parquet.type"

  def setParquetType[T](job: Job, t: ParquetType[T]): Unit = {
    ParquetOutputFormat.setWriteSupportClass(job, classOf[TypeWriteSupport[T]])
    job.getConfiguration.set(ParquetTypeKey, SerializationUtils.toBase64(t))
  }
}
