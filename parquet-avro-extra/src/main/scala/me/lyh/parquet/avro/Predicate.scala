package me.lyh.parquet.avro

import org.apache.avro.Schema
import org.apache.avro.specific.{SpecificRecord => SR}
import org.apache.parquet.filter2.predicate.FilterPredicate

import scala.language.experimental.macros
import scala.reflect.macros.Context

case class Predicates[T](native: T => Boolean, parquet: FilterPredicate)

object Predicate {

  def apply[T <: SR](p: T => Boolean): FilterPredicate = macro applyImpl[T]

  def applyImpl[T <: SR : c.WeakTypeTag](c: Context)
                                        (p: c.Expr[T => Boolean]): c.Expr[FilterPredicate] = buildFilterPredicate(c)(p)

  def build[T <: SR](p: T => Boolean): Predicates[T] = macro buildImpl[T]

  def buildImpl[T <: SR : c.WeakTypeTag](c: Context)
                                        (p: c.Expr[T => Boolean]): c.Expr[Predicates[T]] = {
    import c.universe._
    val f = buildFilterPredicate(c)(p)
    c.Expr(q"_root_.me.lyh.parquet.avro.Predicates($p, $f)").asInstanceOf[c.Expr[Predicates[T]]]

  }

  private val logicalOps = Map("$amp$amp" -> "and", "$bar$bar" -> "or")

  private val compareOps = Map(
    "$greater"    -> "gt",
    "$less"       -> "lt",
    "$greater$eq" -> "gtEq",
    "$less$eq"    -> "ltEq",
    "$eq$eq"      -> "eq",
    "$bang$eq"    -> "notEq")

  private val numericTypes = Map(
    Schema.Type.INT    -> ("Integer", "intColumn", "toInt"),
    Schema.Type.LONG   -> ("Long", "longColumn", "toLong"),
    Schema.Type.FLOAT  -> ("Float", "floatColumn", "toFloat"),
    Schema.Type.DOUBLE -> ("Double", "doubleColumn", "toDouble"))

  private def buildFilterPredicate[T <: SR : c.WeakTypeTag](c: Context)
                                             (p: c.Expr[T => Boolean]): c.Expr[FilterPredicate] = {
    import c.universe._
    val ns = q"_root_.org.apache.parquet.filter2.predicate"
    val nsApi = q"$ns.FilterApi"
    val nsOp = q"$ns.Operators"

    val schema = Class
      .forName(implicitly[WeakTypeTag[T]].tpe.typeSymbol.fullName)
      .getMethod("getClassSchema")
      .invoke(null)
      .asInstanceOf[Schema]

    def mkPredicateFn(columnType: Tree, columnFn: String, value: Tree): (String, String) => Tree = {
      val vt = tq"$columnType with Comparable[$columnType]"
      val supportsLtGt = tq"$nsOp.SupportsLtGt"
      val ct = tq"$nsOp.Column[$columnType] with $supportsLtGt"

      (columnPath: String, operator: String) => {
        val opFn = newTermName(operator)
        val cFn = newTermName(columnFn)
        val colVal = q"$nsApi.$cFn($columnPath)"
        q"$nsApi.$opFn($colVal.asInstanceOf[$ct], $value.asInstanceOf[$vt])"
      }
    }

    def applyToPredicate(tree: Tree): c.Expr[FilterPredicate] = {
      val Apply(Select(lExpr, operator), List(rExpr)) = tree

      def extractGetter(expr: Tree): Option[(String, Schema.Type)] = try {
        val getter = expr match {
          case Apply(_, List(g)) => g
          case t => t
        }
        val (fieldName, fieldType) = Common.treeToField(c)(schema, c.Expr[T => Any](q"(x: Any) => $getter"))
        if (fieldName != "" && fieldType != Schema.Type.NULL) Some((fieldName, fieldType)) else None
      } catch {
        case _: Exception => None
      }

      val logicalOp = logicalOps.get(operator.toString)
      if (logicalOp.isDefined) {
        // expr1 AND|OR expr2
        val (op, l, r) = (newTermName(logicalOp.get), parse(lExpr), parse(rExpr))
        c.Expr(q"$ns.FilterApi.$op($l, $r)").asInstanceOf[c.Expr[FilterPredicate]]
      } else {
        // expr1 COMP expr2
        val (flipped, (fieldName, fieldType), valueExpr) = (extractGetter(lExpr), extractGetter(rExpr)) match {
          case (Some(g), None) => (false, g, rExpr)  // getter COMP value
          case (None, Some(g)) => (true, g, lExpr)   // value COMP getter
          case _ => throw new RuntimeException("Invalid expression: " + tree)
        }

        def flip(op: String) = if (op.startsWith("gt")) {
          "lt" + op.substring(2)
        } else if (op.startsWith("lt")) {
          "gt" + op.substring(2)
        } else {
          op
        }

        if (operator.toString == "Boolean2boolean") {
          // implicit boolean predicate, e.g. (_.isValid)
          val predicateFn = mkPredicateFn(tq"java.lang.Boolean","booleanColumn", q"true")
          c.Expr(predicateFn(fieldName, "eq")).asInstanceOf[c.Expr[FilterPredicate]]
        } else {
          // value can be a null literal with no {toInt, toLong, ...} methods
          val isNullLiteral = try {
            val Literal(Constant(null)) = valueExpr
            true
          } catch {
            case _: Exception => false
          }

          val predicateFn = fieldType match {
            case t if numericTypes.contains(t) =>
              val (cType, cFn, vFn) = numericTypes(t)
              val cTypeName = tq"java.lang.${newTypeName(cType)}"
              val vFnName = newTermName(vFn)
              val nullCase = cq"_: NullPointerException => null"
              val value = if (isNullLiteral) q"null" else q"(if ($valueExpr == null) null else try { $valueExpr.$vFnName } catch { case $nullCase })"
              mkPredicateFn(cTypeName, cFn, value)

            case Schema.Type.BOOLEAN =>
              val value = if (isNullLiteral) q"null" else valueExpr
              mkPredicateFn(tq"java.lang.Boolean","booleanColumn", value)

            case Schema.Type.STRING =>
              val ns = q"_root_.org.apache.parquet.io.api"
              val value = if (isNullLiteral) q"null" else q"$ns.Binary.fromString($valueExpr)"
              mkPredicateFn(tq"$ns.Binary","binaryColumn", value)

            case _ => throw new RuntimeException("Unsupported value type: " + fieldType)
          }

          if (!compareOps.contains(operator.toString)) {
            throw new RuntimeException("Unsupported operator type: " + operator)
          }
          val op = compareOps(operator.toString)
          val realOp = if (flipped) flip(op) else op
          c.Expr(predicateFn(fieldName, realOp)).asInstanceOf[c.Expr[FilterPredicate]]
        }
      }
    }

    def selectToPredicate(tree: Tree): c.Expr[FilterPredicate] = {
      val Select(expr, operator) = tree
      if (operator.toString == "unary_$bang") {
        val p = parse(expr)
        c.Expr(q"$nsApi.not($p)").asInstanceOf[c.Expr[FilterPredicate]]
      } else {
        throw new RuntimeException("Unknown unary operator: " + operator)
      }
    }

    def parse(tree: Tree): c.Expr[FilterPredicate] = {
      tree match {
        case Apply(_, _) => applyToPredicate(tree)
        case Select(_, _) => selectToPredicate(tree)
        case _ => throw new RuntimeException("Invalid expression: " + tree)
      }
    }

    val Function(_, body) = p.tree
    parse(body)
  }

}
