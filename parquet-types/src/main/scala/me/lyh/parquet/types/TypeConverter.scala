package me.lyh.parquet.types

import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}

import scala.collection.mutable

sealed trait TypeConverter[V] extends Converter {
  def get: V
}

private object TypeConverter {
  trait Buffered[V] extends TypeConverter[V] {
    val buffer: mutable.Buffer[V] = mutable.Buffer.empty
    override def get: V = {
      require(buffer.size == 1, "Required field size != 1: " + buffer.size)
      val v = buffer.head
      buffer.clear()
      v
    }
  }

  abstract class Primitive[V] extends PrimitiveConverter with Buffered[V]

  abstract class Delegate[V, U](val inner: Buffered[V]) extends TypeConverter[U] {
    override def isPrimitive: Boolean = inner.isPrimitive
    override def asPrimitiveConverter(): PrimitiveConverter = {
      require(isPrimitive)
      inner.asPrimitiveConverter()
    }
    override def asGroupConverter(): GroupConverter = {
      require(!isPrimitive)
      inner.asGroupConverter()
    }
  }

  def newBoolean: TypeConverter[Boolean] = new Primitive[Boolean] {
    override def addBoolean(value: Boolean): Unit = buffer += value
  }
  def newInt: TypeConverter[Int] = new Primitive[Int] {
    override def addInt(value: Int): Unit = buffer += value
  }
  def newLong: TypeConverter[Long] = new Primitive[Long] {
    override def addLong(value: Long): Unit = buffer += value
  }
  def newFloat: TypeConverter[Float] = new Primitive[Float] {
    override def addFloat(value: Float): Unit = buffer += value
  }
  def newDouble: TypeConverter[Double] = new Primitive[Double] {
    override def addDouble(value: Double): Unit = buffer += value
  }
  def newByteArray: TypeConverter[Array[Byte]] = new Primitive[Array[Byte]] {
    override def addBinary(value: Binary): Unit = buffer += value.getBytes
  }
  def newCharSequence: TypeConverter[CharSequence] = new Primitive[CharSequence] {
    override def addBinary(value: Binary): Unit = buffer += value.toStringUsingUTF8
  }
  def newString: TypeConverter[String] = new Primitive[String] {
    override def addBinary(value: Binary): Unit = buffer += value.toStringUsingUTF8
  }
}
