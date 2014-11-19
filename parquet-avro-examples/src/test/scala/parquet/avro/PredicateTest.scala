package parquet.avro

import org.scalatest._
import _root_.parquet.avro.schema.TestRecord
import _root_.parquet.filter2.predicate.{ FilterApi => F }
import _root_.parquet.io.api.Binary

import java.lang.{ Integer => JInt, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBoolean }

class PredicateTest extends FlatSpec with Matchers {

  val intCol = F.intColumn("int_field")
  val longCol = F.longColumn("long_field")
  val floatCol = F.floatColumn("float_field")
  val doubleCol = F.doubleColumn("double_field")
  val boolCol = F.booleanColumn("boolean_field")
  val strCol = F.binaryColumn("string_field")

  "Predicate" should "support all primitive types" in {
    Predicate[TestRecord](_.getIntField > 10) should equal (F.gt(intCol, JInt.valueOf(10)))
    Predicate[TestRecord](_.getLongField > 10l) should equal (F.gt(longCol, JLong.valueOf(10)))
    Predicate[TestRecord](_.getFloatField > 10f) should equal (F.gt(floatCol, JFloat.valueOf(10)))
    Predicate[TestRecord](_.getDoubleField > 10.0) should equal (F.gt(doubleCol, JDouble.valueOf(10.0)))
    Predicate[TestRecord](_.getBooleanField == true) should equal (F.eq(boolCol, JBoolean.valueOf(true)))

    Predicate[TestRecord](_.getStringField.toString > "abc") should equal (F.gt(strCol, Binary.fromString("abc")))
  }

  "Predicate" should "support all primitive values" in {
    val intVal = 10
    Predicate[TestRecord](_.getIntField > intVal) should equal (F.gt(intCol, JInt.valueOf(10)))
    Predicate[TestRecord](_.getIntField > intVal + 1) should equal (F.gt(intCol, JInt.valueOf(10 + 1)))

    val longVal = 10l
    Predicate[TestRecord](_.getLongField > longVal) should equal (F.gt(longCol, JLong.valueOf(10)))
    Predicate[TestRecord](_.getLongField > longVal + 1) should equal (F.gt(longCol, JLong.valueOf(10 + 1)))

    val floatVal = 10f
    Predicate[TestRecord](_.getFloatField > floatVal) should equal (F.gt(floatCol, JFloat.valueOf(10)))
    Predicate[TestRecord](_.getFloatField > floatVal + 1) should equal (F.gt(floatCol, JFloat.valueOf(10 + 1)))

    val doubleVal = 10.0
    Predicate[TestRecord](_.getDoubleField > doubleVal) should equal (F.gt(doubleCol, JDouble.valueOf(10)))
    Predicate[TestRecord](_.getDoubleField > doubleVal + 1) should equal (F.gt(doubleCol, JDouble.valueOf(10 + 1)))

    val booleanVal = true
    Predicate[TestRecord](_.getBooleanField == booleanVal) should equal (F.eq(boolCol, JBoolean.valueOf(true)))
    Predicate[TestRecord](_.getBooleanField == !booleanVal) should equal (F.eq(boolCol, JBoolean.valueOf(!true)))

    val stringVal = "abc"
    Predicate[TestRecord](_.getStringField.toString > stringVal) should equal (F.gt(strCol, Binary.fromString("abc")))
    Predicate[TestRecord] {
      _.getStringField.toString > stringVal + "x"
    } should equal (F.gt(strCol, Binary.fromString("abc" + "x")))
  }

  "Predicate" should "support all predicates" in {
    val int10 = JInt.valueOf(10)

    Predicate[TestRecord](_.getIntField > 10) should equal (F.gt(intCol, int10))
    Predicate[TestRecord](_.getIntField < 10) should equal (F.lt(intCol, int10))
    Predicate[TestRecord](_.getIntField >= 10) should equal (F.gtEq(intCol, int10))
    Predicate[TestRecord](_.getIntField <= 10) should equal (F.ltEq(intCol, int10))
    Predicate[TestRecord](_.getIntField == 10) should equal (F.eq(intCol, int10))
    Predicate[TestRecord](_.getIntField != 10) should equal (F.notEq(intCol, int10))
  }

  "Predicate" should "support flipped operands" in {
    val int10 = JInt.valueOf(10)

    Predicate[TestRecord](10 < _.getIntField) should equal (F.gt(intCol, int10))
    Predicate[TestRecord](10 >_.getIntField) should equal (F.lt(intCol, int10))
    Predicate[TestRecord](10 <= _.getIntField) should equal (F.gtEq(intCol, int10))
    Predicate[TestRecord](10 >= _.getIntField) should equal (F.ltEq(intCol, int10))
    Predicate[TestRecord](10 == _.getIntField) should equal (F.eq(intCol, int10))
    Predicate[TestRecord](10 != _.getIntField) should equal (F.notEq(intCol, int10))
  }

  "Predicate" should "support not operator" in {
    val int10 = JInt.valueOf(10)

    Predicate[TestRecord](r => !(r.getIntField > 10)) should equal (F.not(F.gt(intCol, int10)))
    Predicate[TestRecord](r => !(r.getIntField < 10)) should equal (F.not(F.lt(intCol, int10)))
    Predicate[TestRecord](r => !(r.getIntField >= 10)) should equal (F.not(F.gtEq(intCol, int10)))
    Predicate[TestRecord](r => !(r.getIntField <= 10)) should equal (F.not(F.ltEq(intCol, int10)))
    Predicate[TestRecord](r => !(r.getIntField == 10)) should equal (F.not(F.eq(intCol, int10)))
    Predicate[TestRecord](r => !(r.getIntField != 10)) should equal (F.not(F.notEq(intCol, int10)))
  }

  "Predicate" should "support binary boolean operators" in {
    val intGt = F.gt(intCol, JInt.valueOf(10))
    val longLt = F.lt(longCol, JLong.valueOf(20))

    Predicate[TestRecord] { r =>
      r.getIntField > 10 && r.getLongField < 20l
    } should equal (F.and(intGt, longLt))

    Predicate[TestRecord] { r =>
      r.getIntField > 10 || r.getLongField < 20l
    } should equal (F.or(intGt, longLt))

    Predicate[TestRecord]{ r =>
      !(r.getIntField > 10 && r.getLongField < 20l)
    } should equal (F.not(F.and(intGt, longLt)))

    Predicate[TestRecord]{ r =>
      !(r.getIntField > 10) || !(r.getLongField < 20l)
    } should equal (F.or(F.not(intGt), F.not(longLt)))
  }

}
