package me.lyh.parquet.types

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.google.common.io.BaseEncoding

private object SerializationUtils {
  def toBase64[T <: Serializable](obj: T): String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    BaseEncoding.base64().encode(baos.toByteArray)
  }

  def fromBase64[T](b64: String): T = {
    val bytes = BaseEncoding.base64().decode(b64)
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    ois.readObject().asInstanceOf[T]
  }
}
