package me.lyh.parquet.avro

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}

import _root_.parquet.filter2.predicate.{FilterApi => F}
import _root_.parquet.io.api.Binary
import me.lyh.parquet.avro.schema.{TestRecord => TR}
import org.scalatest._

class PredicateTest extends FlatSpec with Matchers {

  val intCol = F.intColumn("int_field")
  val longCol = F.longColumn("long_field")
  val floatCol = F.floatColumn("float_field")
  val doubleCol = F.doubleColumn("double_field")
  val boolCol = F.booleanColumn("boolean_field")
  val strCol = F.binaryColumn("string_field")

  "Predicate" should "support all primitive types" in {
    Predicate[TR](_.getIntField > 10) shouldEqual F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getIntField > 10) shouldEqual F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getLongField > 10l) shouldEqual F.gt(longCol, JLong.valueOf(10))
    Predicate[TR](_.getFloatField > 10f) shouldEqual F.gt(floatCol, JFloat.valueOf(10))
    Predicate[TR](_.getDoubleField > 10.0) shouldEqual F.gt(doubleCol, JDouble.valueOf(10.0))
    Predicate[TR](_.getBooleanField == true) shouldEqual F.eq(boolCol, JBoolean.valueOf(true))

    Predicate[TR](_.getStringField.toString > "abc") shouldEqual F.gt(strCol, Binary.fromString("abc"))
  }

  "Predicate" should "support all primitive values" in {
    val intVal = 10
    Predicate[TR](_.getIntField > intVal) shouldEqual F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getIntField > intVal + 1) shouldEqual F.gt(intCol, JInt.valueOf(10 + 1))

    val longVal = 10l
    Predicate[TR](_.getLongField > longVal) shouldEqual F.gt(longCol, JLong.valueOf(10))
    Predicate[TR](_.getLongField > longVal + 1) shouldEqual F.gt(longCol, JLong.valueOf(10 + 1))

    val floatVal = 10f
    Predicate[TR](_.getFloatField > floatVal) shouldEqual F.gt(floatCol, JFloat.valueOf(10))
    Predicate[TR](_.getFloatField > floatVal + 1) shouldEqual F.gt(floatCol, JFloat.valueOf(10 + 1))

    val doubleVal = 10.0
    Predicate[TR](_.getDoubleField > doubleVal) shouldEqual F.gt(doubleCol, JDouble.valueOf(10))
    Predicate[TR](_.getDoubleField > doubleVal + 1) shouldEqual F.gt(doubleCol, JDouble.valueOf(10 + 1))

    val booleanVal = true
    Predicate[TR](_.getBooleanField == booleanVal) shouldEqual F.eq(boolCol, JBoolean.valueOf(true))
    Predicate[TR](_.getBooleanField == !booleanVal) shouldEqual F.eq(boolCol, JBoolean.valueOf(false))

    val stringVal = "abc"
    Predicate[TR](_.getStringField.toString > stringVal) shouldEqual F.gt(strCol, Binary.fromString("abc"))
    Predicate[TR] {
      _.getStringField.toString > stringVal + "x"
    } shouldEqual F.gt(strCol, Binary.fromString("abc" + "x"))
  }

  "Predicate" should "support implicit boolean predicate" in {
    val trueVal = JBoolean.valueOf(true)
    val intGt = F.gt(intCol, JInt.valueOf(10))

    Predicate[TR](_.getBooleanField) shouldEqual F.eq(boolCol, trueVal)
    Predicate[TR](!_.getBooleanField) shouldEqual F.not(F.eq(boolCol, trueVal))

    Predicate[TR] { r =>
      r.getBooleanField && r.getIntField > 10
    } shouldEqual F.and(F.eq(boolCol, trueVal), intGt)

    Predicate[TR] { r =>
      !r.getBooleanField && r.getIntField > 10
    } shouldEqual F.and(F.not(F.eq(boolCol, trueVal)), intGt)
  }

  "Predicate" should "support all predicates" in {
    val int10 = JInt.valueOf(10)

    Predicate[TR](_.getIntField > 10) shouldEqual F.gt(intCol, int10)
    Predicate[TR](_.getIntField < 10) shouldEqual F.lt(intCol, int10)
    Predicate[TR](_.getIntField >= 10) shouldEqual F.gtEq(intCol, int10)
    Predicate[TR](_.getIntField <= 10) shouldEqual F.ltEq(intCol, int10)
    Predicate[TR](_.getIntField == 10) shouldEqual F.eq(intCol, int10)
    Predicate[TR](_.getIntField != 10) shouldEqual F.notEq(intCol, int10)
  }

  "Predicate" should "support flipped operands" in {
    val int10 = JInt.valueOf(10)

    Predicate[TR](10 < _.getIntField) shouldEqual F.gt(intCol, int10)
    Predicate[TR](10 > _.getIntField) shouldEqual F.lt(intCol, int10)
    Predicate[TR](10 <= _.getIntField) shouldEqual F.gtEq(intCol, int10)
    Predicate[TR](10 >= _.getIntField) shouldEqual F.ltEq(intCol, int10)
    Predicate[TR](10 == _.getIntField) shouldEqual F.eq(intCol, int10)
    Predicate[TR](10 != _.getIntField) shouldEqual F.notEq(intCol, int10)
  }

  "Predicate" should "support logical not operator" in {
    val int10 = JInt.valueOf(10)

    Predicate[TR](r => !(r.getIntField > 10)) shouldEqual F.not(F.gt(intCol, int10))
    Predicate[TR](r => !(r.getIntField < 10)) shouldEqual F.not(F.lt(intCol, int10))
    Predicate[TR](r => !(r.getIntField >= 10)) shouldEqual F.not(F.gtEq(intCol, int10))
    Predicate[TR](r => !(r.getIntField <= 10)) shouldEqual F.not(F.ltEq(intCol, int10))
    Predicate[TR](r => !(r.getIntField == 10)) shouldEqual F.not(F.eq(intCol, int10))
    Predicate[TR](r => !(r.getIntField != 10)) shouldEqual F.not(F.notEq(intCol, int10))
  }

  "Predicate" should "support binary logical operators" in {
    val intGt = F.gt(intCol, JInt.valueOf(10))
    val longLt = F.lt(longCol, JLong.valueOf(20))

    Predicate[TR] { r =>
      r.getIntField > 10 && r.getLongField < 20l
    } shouldEqual F.and(intGt, longLt)

    Predicate[TR] { r =>
      r.getIntField > 10 || r.getLongField < 20l
    } shouldEqual F.or(intGt, longLt)

    Predicate[TR]{ r =>
      !(r.getIntField > 10 && r.getLongField < 20l)
    } shouldEqual F.not(F.and(intGt, longLt))

    Predicate[TR]{ r =>
      !(r.getIntField > 10) || !(r.getLongField < 20l)
    } shouldEqual F.or(F.not(intGt), F.not(longLt))
  }

  "Predicate" should "build Scala lambda" in {
    val record = new TR(10, 20l, 30.0f, 40.0, true, "test")

    val t1 = Predicate.build[TR](r => r.getIntField > 0 && r.getLongField > 0l)
    t1.native(record) shouldBe true
    t1.parquet shouldEqual F.and(F.gt(intCol, JInt.valueOf(0)), F.gt(longCol, JLong.valueOf(0l)))

    val t2 = Predicate.build[TR](r => r.getIntField > 100 && r.getLongField > 100l)
    t2.native(record) shouldBe false
    t2.parquet shouldEqual F.and(F.gt(intCol, JInt.valueOf(100)), F.gt(longCol, JLong.valueOf(100l)))
  }

}
