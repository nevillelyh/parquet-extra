package me.lyh.parquet.types

import magnolia._
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, RecordConsumer}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}

import scala.annotation.implicitNotFound
import scala.language.experimental.macros

sealed trait ParquetType[V] extends Serializable {
  def schema: Type
  protected val isGroup: Boolean = false
  protected def isEmpty(v: V): Boolean
  private[types] def write(c: RecordConsumer, v: V): Unit
  private[types] def newConverter: TypeConverter[V]

  protected def writeGroup(c: RecordConsumer, v: V): Unit = {
    if (isGroup) {
      c.startGroup()
    }
    write(c, v)
    if (isGroup) {
      c.endGroup()
    }
  }
}

object ParquetType {
  type Typeclass[T] = ParquetType[T]

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = new Typeclass[T] {
    override def schema: Type =
      caseClass.parameters
        .foldLeft(Types.requiredGroup()) { (g, p) =>
          g.addField(Schema.rename(p.typeclass.schema, p.label))
        }
        .named(caseClass.typeName.full)

    override protected val isGroup: Boolean = true
    override protected def isEmpty(v: T): Boolean = false

    override private[types] def write(c: RecordConsumer, v: T): Unit = {
      caseClass.parameters.foreach { p =>
        val x = p.dereference(v)
        if (!p.typeclass.isEmpty(x)) {
          c.startField(p.label, p.index)
          p.typeclass.writeGroup(c, x)
          c.endField(p.label, p.index)
        }
      }
    }

    override private[types] def newConverter: TypeConverter[T] =
      new GroupConverter with TypeConverter.Buffered[T] {
        private val fieldConverters = caseClass.parameters.map(_.typeclass.newConverter)
        override def isPrimitive: Boolean = false
        override def getConverter(fieldIndex: Int): Converter = fieldConverters(fieldIndex)
        override def start(): Unit = ()
        override def end(): Unit = buffer += caseClass.construct { p =>
          try {
            fieldConverters(p.index).get
          } catch {
            case e: IllegalArgumentException =>
              val field = s"${caseClass.typeName.full}#${p.label}"
              throw new ParquetDecodingException(s"Failed to decode $field: ${e.getMessage}", e)
          }
        }
      }
  }

  @implicitNotFound("Cannot derive ParquetType for sealed trait")
  private sealed trait Dispatchable[T]
  def dispatch[T: Dispatchable](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = ???

  implicit def apply[T]: Typeclass[T] = macro Magnolia.gen[T]

  private trait PrimitiveType[V] extends ParquetType[V] {
    override protected def isEmpty(v: V): Boolean = false
  }
  implicit val booleanType: ParquetType[Boolean] = new PrimitiveType[Boolean] {
    override def schema: Type = Schema.primitive(PrimitiveTypeName.BOOLEAN)
    override private[types] def write(c: RecordConsumer, v: Boolean): Unit = c.addBoolean(v)
    override private[types] def newConverter: TypeConverter[Boolean] = TypeConverter.newBoolean
  }
  implicit val intType: ParquetType[Int] = new PrimitiveType[Int] {
    override def schema: Type =
      Schema.primitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true))
    override private[types] def write(c: RecordConsumer, v: Int): Unit = c.addInteger(v)
    override private[types] def newConverter: TypeConverter[Int] = TypeConverter.newInt
  }
  implicit val longType: ParquetType[Long] = new PrimitiveType[Long] {
    override def schema: Type =
      Schema.primitive(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true))
    override private[types] def write(c: RecordConsumer, v: Long): Unit = c.addLong(v)
    override private[types] def newConverter: TypeConverter[Long] = TypeConverter.newLong
  }
  implicit val floatType: ParquetType[Float] = new PrimitiveType[Float] {
    override def schema: Type = Schema.primitive(PrimitiveTypeName.FLOAT)
    override private[types] def write(c: RecordConsumer, v: Float): Unit = c.addFloat(v)
    override private[types] def newConverter: TypeConverter[Float] = TypeConverter.newFloat
  }
  implicit val doubleType: ParquetType[Double] = new PrimitiveType[Double] {
    override def schema: Type = Schema.primitive(PrimitiveTypeName.DOUBLE)
    override private[types] def write(c: RecordConsumer, v: Double): Unit = c.addDouble(v)
    override private[types] def newConverter: TypeConverter[Double] = TypeConverter.newDouble
  }
  implicit val byteArrayType: ParquetType[Array[Byte]] = new PrimitiveType[Array[Byte]] {
    override def schema: Type = Schema.primitive(PrimitiveTypeName.BINARY)
    override private[types] def write(c: RecordConsumer, v: Array[Byte]): Unit =
      c.addBinary(Binary.fromConstantByteArray(v))
    override private[types] def newConverter: TypeConverter[Array[Byte]] =
      TypeConverter.newByteArray
  }
  implicit val charSequenceType: ParquetType[CharSequence] = new PrimitiveType[CharSequence] {
    override def schema: Type =
      Schema.primitive(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())
    override private[types] def write(c: RecordConsumer, v: CharSequence): Unit =
      c.addBinary(Binary.fromCharSequence(v))
    override private[types] def newConverter: TypeConverter[CharSequence] =
      TypeConverter.newCharSequence
  }
  implicit val stringType: ParquetType[String] = new PrimitiveType[String] {
    override def schema: Type =
      Schema.primitive(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())
    override private[types] def write(c: RecordConsumer, v: String): Unit =
      c.addBinary(Binary.fromString(v))
    override private[types] def newConverter: TypeConverter[String] = TypeConverter.newString
  }

  implicit def optionalType[V](implicit t: Typeclass[V]): Typeclass[Option[V]] =
    new Typeclass[Option[V]] {
      override def schema: Type = Schema.setRepetition(t.schema, Repetition.OPTIONAL)
      override protected def isEmpty(v: Option[V]): Boolean = v.isEmpty

      override private[types] def write(c: RecordConsumer, v: Option[V]): Unit =
        v.foreach(t.writeGroup(c, _))

      override private[types] def newConverter: TypeConverter[Option[V]] = {
        val buffered = t.newConverter.asInstanceOf[TypeConverter.Buffered[V]]
        new TypeConverter.Delegate[V, Option[V]](buffered) {
          override def get: Option[V] = {
            require(inner.buffer.size <= 1, "Optional field size > 1: " + inner.buffer.size)
            val v = inner.buffer.headOption
            inner.buffer.clear()
            v
          }
        }
      }
    }

  implicit def repeatedType[V, C[V]](
    implicit t: Typeclass[V],
    ti: C[V] => Iterable[V],
    fc: FactoryCompat[V, C[V]]
  ): Typeclass[C[V]] =
    new Typeclass[C[V]] {
      override def schema: Type = Schema.setRepetition(t.schema, Repetition.REPEATED)
      override protected def isEmpty(v: C[V]): Boolean = v.isEmpty

      override private[types] def write(c: RecordConsumer, v: C[V]): Unit =
        v.foreach(t.writeGroup(c, _))

      override private[types] def newConverter: TypeConverter[C[V]] = {
        val buffered = t.newConverter.asInstanceOf[TypeConverter.Buffered[V]]
        new TypeConverter.Delegate[V, C[V]](buffered) {
          override def get: C[V] = {
            val v = fc.build(inner.buffer)
            inner.buffer.clear()
            v
          }
        }
      }
    }
}
