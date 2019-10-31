package me.lyh.parquet.types

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetReader}
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.api.{GroupConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType

class TypeReadSupport[T] extends ReadSupport[T] {
  private var parquetType: ParquetType[T] = null

  override def init(context: InitContext): ReadSupport.ReadContext = {
    if (parquetType == null) {
      parquetType = SerializationUtils.fromBase64(
        context.getConfiguration.get(TypeParquetInputFormat.ParquetTypeKey)
      )
    }
    val requestedSchema = Schema.message(parquetType.schema)
    new ReadSupport.ReadContext(requestedSchema, java.util.Collections.emptyMap())
  }

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: java.util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadSupport.ReadContext): RecordMaterializer[T] = {
    parquetType.schema
    new RecordMaterializer[T] {
      private val root = parquetType.newConverter
      override def getCurrentRecord: T = root.get
      override def getRootConverter: GroupConverter = root.asGroupConverter()
    }
  }
}

object TypeReadSupport {
  def apply[T](implicit t: ParquetType[T]): TypeReadSupport[T] = {
    val s = new TypeReadSupport[T]
    s.parquetType = t
    s
  }
}

object TypeParquetReader {
  def builder[T: ParquetType](file: InputFile): Builder[T] =
    new ParquetReader.Builder[T](file) with Builder[T] {
      override protected val readSupport: TypeReadSupport[T] = TypeReadSupport[T]
    }

  def builder[T: ParquetType](path: Path): ParquetReader.Builder[T] =
    new ParquetReader.Builder[T](path) with Builder[T] {
      override protected val readSupport: TypeReadSupport[T] = TypeReadSupport[T]
    }

  trait Builder[T] extends ParquetReader.Builder[T] {
    protected val readSupport: TypeReadSupport[T]
    override protected def getReadSupport: ReadSupport[T] = readSupport
  }
}


class TypeParquetInputFormat[T] extends ParquetInputFormat[T](classOf[TypeReadSupport[T]])

object TypeParquetInputFormat {
  val ParquetTypeKey = "parquet.type.read.parquet.type"

  def setParquetType[T](job: Job, t: ParquetType[T]): Unit = {
    ParquetInputFormat.setReadSupportClass(job, classOf[TypeReadSupport[T]])
    job.getConfiguration.set(ParquetTypeKey, SerializationUtils.toBase64(t))
  }
}
