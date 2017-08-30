package me.lyh.parquet.avro

import org.apache.avro.Schema
import org.apache.avro.compiler.specific.SpecificCompiler._
import org.apache.avro.specific.{SpecificRecord => SR}

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.macros._

object Common {

  def treeToField[T <: SR : c.WeakTypeTag](c: blackbox.Context)
                                          (schema: Schema,
                                           getter: c.Expr[T => Any]): (String, Schema.Type) = {
    import c.universe._

    def extractGetters(t: Tree): Seq[String] = {
      def extract(t: Tree, s: Seq[String]): (Tree, Seq[String]) = t match {
        case Select(sel, tn) => extract(sel, tn.toString +: s)
        case Apply(sel, _)   => extract(sel, s)
        case x => (null, s)
      }
      extract(t, Seq())._2
    }

    def gettersToField(getters: Seq[String]): (String, Schema.Type) = {
      var node = schema
      var fieldType: Schema.Type = Schema.Type.NULL

      val gt = getters.filter(s => s != "get" && s != "toString")

      def fromNullable(s: Schema): Schema = s.getTypes.asScala.find(_.getType != Schema.Type.NULL).get

      val fields = gt.zipWithIndex.map { case (g, i) =>
        val field = node.getFields.asScala.find(f => generateGetMethod(schema, f) == g).get
        val next = field.schema()
        fieldType = if (next.getType == Schema.Type.UNION) fromNullable(next).getType else next.getType

        if (i < gt.size - 1) {
          node = next.getType match {
            case Schema.Type.RECORD => next
            case Schema.Type.UNION => fromNullable(next)
            case Schema.Type.ARRAY => next.getElementType
            case t => throw new RuntimeException(s"Unsupported type: $t")
          }
        }
        field.name()
      }
      (fields.mkString("."), fieldType)
    }

    try {
      val Function(_, body) = getter.tree
      gettersToField(extractGetters(body))
    } catch {
      case e: Exception => throw new IllegalArgumentException("Invalid getter expression: " + getter.tree + " " + e)
    }
  }

  def isNull[@specialized (Boolean, Int, Long, Float, Double) T](x: T): Boolean = x == null

}
