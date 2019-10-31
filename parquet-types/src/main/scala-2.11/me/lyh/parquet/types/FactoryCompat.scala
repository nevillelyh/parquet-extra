package me.lyh.parquet.types

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

trait FactoryCompat[-A, +C] extends Serializable { self =>
  def newBuilder: mutable.Builder[A, C]
  def build(xs: TraversableOnce[A]): C = (newBuilder ++= xs).result()
}

object FactoryCompat {
  private type FC[A, C] = FactoryCompat[A, C]

  def apply[A, C](f: () => mutable.Builder[A, C]): FC[A, C] =
    new FactoryCompat[A, C] {
      override def newBuilder: mutable.Builder[A, C] = f()
    }

  implicit def arrayFC[A: ClassTag] = FactoryCompat(() => Array.newBuilder[A])
  implicit def traversableFC[A] = FactoryCompat(() => Traversable.newBuilder[A])
  implicit def iterableFC[A] = FactoryCompat(() => Iterable.newBuilder[A])
  implicit def seqFC[A] = FactoryCompat(() => Seq.newBuilder[A])
  implicit def indexedSeqFC[A] = FactoryCompat(() => IndexedSeq.newBuilder[A])
  implicit def listFC[A] = FactoryCompat(() => List.newBuilder[A])
  implicit def vectorFC[A] = FactoryCompat(() => Vector.newBuilder[A])
  implicit def streamFC[A] = FactoryCompat(() => Stream.newBuilder[A])
}
