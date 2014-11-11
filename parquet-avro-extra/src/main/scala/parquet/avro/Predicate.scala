package parquet.avro

import org.apache.avro.Schema
import org.apache.avro.specific.{ SpecificRecord => SR }
import _root_.parquet.filter2.predicate.FilterPredicate

import scala.language.experimental.macros
import scala.reflect.macros.Context

object Predicate {

  def apply[T <: SR](p: T => Boolean): FilterPredicate = macro applyImpl[T]

  def applyImpl[T <: SR : c.WeakTypeTag](c: Context)
                                        (p: c.Expr[T => Boolean]): c.Expr[FilterPredicate] = {
    import c.universe._
    val namespace = q"_root_.parquet.filter2.predicate"

    val schema = Class
      .forName(implicitly[WeakTypeTag[T]].tpe.typeSymbol.fullName)
      .getMethod("getClassSchema")
      .invoke(null)
      .asInstanceOf[Schema]

    def mkPredicateFn(columnType: Tree, columnFn: String, value: Tree): (String, String) => Tree = {
      val vt = tq"$columnType with Comparable[$columnType]"
      val supportsLtGt = tq"$namespace.Operators.SupportsLtGt"
      val ct = tq"$namespace.Operators.Column[$columnType] with $supportsLtGt"

      (columnPath: String, operator: String) => {
        val opFn = newTermName(operator)
        val cFn = newTermName(columnFn)
        val colVal = q"$namespace.FilterApi.$cFn($columnPath)"
        q"$namespace.FilterApi.$opFn($colVal.asInstanceOf[$ct], $value.asInstanceOf[$vt])"
      }
    }

    def applyToPredicate(tree: Tree): c.Expr[FilterPredicate] = {
      val Apply(Select(lExpr, operator), List(rExpr)) = tree

      def getValue(v: Any): Option[Any] = try {
        Some(v.asInstanceOf[Literal].value.value)
      } catch {
        case _: Exception => None
      }

      val boolOp = Map("$amp$amp" -> "and", "$bar$bar" -> "or").get(operator.toString)
      if (boolOp.isDefined) {
        // expr1 AND|OR expr2
        val op = newTermName(boolOp.get)
        val l = treeToPredicate(lExpr)
        val r = treeToPredicate(rExpr.asInstanceOf[Tree])
        c.Expr(q"$namespace.FilterApi.$op($l, $r)").asInstanceOf[c.Expr[FilterPredicate]]
      } else {
        // expr1 COMP expr2
        val (flipped, constant) = (getValue(lExpr), getValue(rExpr)) match {
          case (None, Some(v)) => (false, v)  // term COMP constant
          case (Some(v), None) => (true, v)   // constant COMP term
          case _ => throw new RuntimeException("Invalid expression: " + tree)
        }

        val op = if (!flipped) {
          operator.toString match {
            case "$greater"    => "gt"
            case "$less"       => "lt"
            case "$greater$eq" => "gtEq"
            case "$less$eq"    => "ltEq"
            case "$eq$eq"      => "eq"
            case "$bang$eq"    => "notEq"
          }
        } else {
          operator.toString match {
            case "$greater"    => "lt"
            case "$less"       => "gt"
            case "$greater$eq" => "ltEq"
            case "$less$eq"    => "gtEq"
            case "$eq$eq"      => "eq"
            case "$bang$eq"    => "notEq"
          }
        }

        val getter = (if (!flipped) lExpr else rExpr) match {
          case Apply(_, List(g)) => g
          case t => t
        }

        val (fieldName, fieldType) = Common.treeToField(c)(schema, c.Expr[T => Any](q"(x: Any) => $getter"))
        val predicateFn = fieldType match {
          case Schema.Type.INT =>
            mkPredicateFn(tq"java.lang.Integer", "intColumn", q"${constant.asInstanceOf[Int]}")
          case Schema.Type.LONG =>
            mkPredicateFn(tq"java.lang.Long", "longColumn", q"${constant.asInstanceOf[Long]}")
          case Schema.Type.FLOAT =>
            mkPredicateFn(tq"java.lang.Float", "floatColumn", q"${constant.asInstanceOf[Float]}")
          case Schema.Type.DOUBLE =>
            mkPredicateFn(tq"java.lang.Double", "doubleColumn", q"${constant.asInstanceOf[Double]}")
          case Schema.Type.BOOLEAN =>
            mkPredicateFn(tq"java.lang.Boolean","booleanColumn", q"${constant.asInstanceOf[Boolean]}")
          case Schema.Type.STRING =>
            val binary = q"_root_.parquet.io.api.Binary.fromString(${constant.asInstanceOf[String]})"
            mkPredicateFn(tq"_root_.parquet.io.api.Binary","binaryColumn", binary)
          case _ => throw new RuntimeException("Unsupported value type: " + fieldType)
        }
        c.Expr(predicateFn(fieldName, op)).asInstanceOf[c.Expr[FilterPredicate]]
      }
    }

    def selectToPredicate(tree: Tree): c.Expr[FilterPredicate] = {
      val Select(expr, operator) = tree
      if (operator.toString == "unary_$bang") {
        val p = treeToPredicate(expr)
        c.Expr(q"$namespace.FilterApi.not($p)").asInstanceOf[c.Expr[FilterPredicate]]
      } else {
        throw new RuntimeException("Unknown unary operator: " + operator)
      }
    }

    def treeToPredicate(tree: Tree): c.Expr[FilterPredicate] = {
      tree match {
        case Apply(_, _) => applyToPredicate(tree)
        case Select(_, _) => selectToPredicate(tree)
        case _ => throw new RuntimeException("Invalid expression: " + tree)
      }
    }

    val Function(_, body) = p.tree
    treeToPredicate(body)
  }

}
