package me.lyh.parquet.types

import scala.collection.{mutable, Factory}

trait FactoryCompat[-A, +C] extends Serializable {
  def newBuilder: mutable.Builder[A, C]
  def build(xs: IterableOnce[A]): C = newBuilder.addAll(xs).result()
}

object FactoryCompat {
  implicit def fromFactory[A, C](implicit f: Factory[A, C]): FactoryCompat[A, C] =
    new FactoryCompat[A, C] {
      override def newBuilder: mutable.Builder[A, C] = f.newBuilder
    }
}
