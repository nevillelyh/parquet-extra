package me.lyh.parquet.avro

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}

import me.lyh.parquet.avro.schema.{TestRecord => TR}
import org.apache.parquet.filter2.predicate.{FilterApi => F}
import org.apache.parquet.io.api.Binary
import org.scalatest._

class PredicateTest extends FlatSpec with Matchers {

  val intCol = F.intColumn("int_field")
  val longCol = F.longColumn("long_field")
  val floatCol = F.floatColumn("float_field")
  val doubleCol = F.doubleColumn("double_field")
  val boolCol = F.booleanColumn("boolean_field")
  val strCol = F.binaryColumn("string_field")

  "Predicate.apply(p: T => Boolean)" should "support all primitive types" in {
    Predicate[TR](_.getIntField > 10) shouldEqual F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getIntField > 10) shouldEqual F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getLongField > 10L) shouldEqual F.gt(longCol, JLong.valueOf(10))
    Predicate[TR](_.getFloatField > 10f) shouldEqual F.gt(floatCol, JFloat.valueOf(10))
    Predicate[TR](_.getDoubleField > 10.0) shouldEqual F.gt(doubleCol, JDouble.valueOf(10))
    Predicate[TR](_.getBooleanField == true) shouldEqual F.eq(boolCol, JBoolean.valueOf(true))

    Predicate[TR](_.getStringField.toString > "abc") shouldEqual F.gt(strCol, Binary.fromString("abc"))
  }

  it should "support all primitive values" in {
    val intVal = 10
    Predicate[TR](_.getIntField > intVal) shouldEqual F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getIntField > intVal + 1) shouldEqual F.gt(intCol, JInt.valueOf(10 + 1))

    val longVal = 10L
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

  it should "support null literals" in {
    Predicate[TR](_.getIntField == null) shouldEqual F.eq(intCol, null.asInstanceOf[JInt])
    Predicate[TR](_.getLongField == null) shouldEqual F.eq(longCol, null.asInstanceOf[JLong])
    Predicate[TR](_.getFloatField == null) shouldEqual F.eq(floatCol, null.asInstanceOf[JFloat])
    Predicate[TR](_.getDoubleField == null) shouldEqual F.eq(doubleCol, null.asInstanceOf[JDouble])
    Predicate[TR](_.getBooleanField == null) shouldEqual F.eq(boolCol, null.asInstanceOf[JBoolean])
    Predicate[TR](_.getStringField == null) shouldEqual F.eq(strCol, null.asInstanceOf[Binary])
  }

  it should "support null boxed values" in {
    val i: JInt = null
    val l: JLong = null
    val f: JFloat = null
    val d: JDouble = null
    Predicate[TR](_.getIntField == i) shouldEqual F.eq(intCol, null.asInstanceOf[JInt])
    Predicate[TR](_.getLongField == l) shouldEqual F.eq(longCol, null.asInstanceOf[JLong])
    Predicate[TR](_.getFloatField == f) shouldEqual F.eq(floatCol, null.asInstanceOf[JFloat])
    Predicate[TR](_.getDoubleField  == d) shouldEqual F.eq(doubleCol, null.asInstanceOf[JDouble])
  }

  it should "support type coercion" in {
    val intP = F.gt(intCol, JInt.valueOf(10))
    Predicate[TR](_.getIntField > 10L) shouldEqual intP
    Predicate[TR](10L < _.getIntField) shouldEqual intP
    Predicate[TR](_.getIntField > 10.0f) shouldEqual intP
    Predicate[TR](10.0f < _.getIntField) shouldEqual intP
    Predicate[TR](_.getIntField > 10.0) shouldEqual intP
    Predicate[TR](10.0 < _.getIntField) shouldEqual intP

    val longP = F.gt(longCol, JLong.valueOf(10))
    Predicate[TR](_.getLongField > 10) shouldEqual longP
    Predicate[TR](10 < _.getLongField) shouldEqual longP
    Predicate[TR](_.getLongField > 10.0f) shouldEqual longP
    Predicate[TR](10.0f < _.getLongField) shouldEqual longP
    Predicate[TR](_.getLongField > 10.0) shouldEqual longP
    Predicate[TR](10.0 < _.getLongField) shouldEqual longP

    val floatP = F.gt(floatCol, JFloat.valueOf(10))
    Predicate[TR](_.getFloatField > 10) shouldEqual floatP
    Predicate[TR](10 < _.getFloatField) shouldEqual floatP
    Predicate[TR](_.getFloatField > 10L) shouldEqual floatP
    Predicate[TR](10L < _.getFloatField) shouldEqual floatP
    Predicate[TR](_.getFloatField > 10.0) shouldEqual floatP
    Predicate[TR](10.0 < _.getFloatField) shouldEqual floatP

    val doubleP = F.gt(doubleCol, JDouble.valueOf(10))
    Predicate[TR](_.getDoubleField > 10) shouldEqual doubleP
    Predicate[TR](10 < _.getDoubleField) shouldEqual doubleP
    Predicate[TR](_.getDoubleField > 10L) shouldEqual doubleP
    Predicate[TR](10L < _.getDoubleField) shouldEqual doubleP
    Predicate[TR](_.getDoubleField > 10.0f) shouldEqual doubleP
    Predicate[TR](10.0f < _.getDoubleField) shouldEqual doubleP
  }

  it should "support all comparators" in {
    val int10 = JInt.valueOf(10)

    Predicate[TR](_.getIntField > 10) shouldEqual F.gt(intCol, int10)
    Predicate[TR](_.getIntField < 10) shouldEqual F.lt(intCol, int10)
    Predicate[TR](_.getIntField >= 10) shouldEqual F.gtEq(intCol, int10)
    Predicate[TR](_.getIntField <= 10) shouldEqual F.ltEq(intCol, int10)
    Predicate[TR](_.getIntField == 10) shouldEqual F.eq(intCol, int10)
    Predicate[TR](_.getIntField != 10) shouldEqual F.notEq(intCol, int10)
  }

  it should "support flipped operands" in {
    val int10 = JInt.valueOf(10)

    Predicate[TR](10 < _.getIntField) shouldEqual F.gt(intCol, int10)
    Predicate[TR](10 > _.getIntField) shouldEqual F.lt(intCol, int10)
    Predicate[TR](10 <= _.getIntField) shouldEqual F.gtEq(intCol, int10)
    Predicate[TR](10 >= _.getIntField) shouldEqual F.ltEq(intCol, int10)
    Predicate[TR](10 == _.getIntField) shouldEqual F.eq(intCol, int10)
    Predicate[TR](10 != _.getIntField) shouldEqual F.notEq(intCol, int10)
  }

  it should "support logical not operator" in {
    val int10 = JInt.valueOf(10)

    Predicate[TR](r => !(r.getIntField > 10)) shouldEqual F.not(F.gt(intCol, int10))
    Predicate[TR](r => !(r.getIntField < 10)) shouldEqual F.not(F.lt(intCol, int10))
    Predicate[TR](r => !(r.getIntField >= 10)) shouldEqual F.not(F.gtEq(intCol, int10))
    Predicate[TR](r => !(r.getIntField <= 10)) shouldEqual F.not(F.ltEq(intCol, int10))
    Predicate[TR](r => !(r.getIntField == 10)) shouldEqual F.not(F.eq(intCol, int10))
    Predicate[TR](r => !(r.getIntField != 10)) shouldEqual F.not(F.notEq(intCol, int10))
  }

  it should "support binary logical operators" in {
    val intGt = F.gt(intCol, JInt.valueOf(10))
    val longLt = F.lt(longCol, JLong.valueOf(20))

    Predicate[TR] { r =>
      r.getIntField > 10 && r.getLongField < 20L
    } shouldEqual F.and(intGt, longLt)

    Predicate[TR] { r =>
      r.getIntField > 10 || r.getLongField < 20L
    } shouldEqual F.or(intGt, longLt)

    Predicate[TR]{ r =>
      !(r.getIntField > 10 && r.getLongField < 20L)
    } shouldEqual F.not(F.and(intGt, longLt))

    Predicate[TR]{ r =>
      !(r.getIntField > 10) || !(r.getLongField < 20L)
    } shouldEqual F.or(F.not(intGt), F.not(longLt))
  }

  it should "support implicit boolean predicate" in {
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

  "Predicate.build(p: T => Boolean)" should "build Scala lambda and FilterPredicate" in {
    val record = new TR(10, 20L, 30.0f, 40.0, true, "test")

    val t1 = Predicate.build[TR](r => r.getIntField > 0 && r.getLongField > 0L)
    t1.native(record) shouldBe true
    t1.parquet shouldEqual F.and(F.gt(intCol, JInt.valueOf(0)), F.gt(longCol, JLong.valueOf(0)))

    val t2 = Predicate.build[TR](r => r.getIntField > 100 && r.getLongField > 100L)
    t2.native(record) shouldBe false
    t2.parquet shouldEqual F.and(F.gt(intCol, JInt.valueOf(100)), F.gt(longCol, JLong.valueOf(100)))
  }

}
