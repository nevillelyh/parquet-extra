package parquet.avro

import org.scalatest._
import parquet.avro.schema.User

class ProjectionTest extends FlatSpec with Matchers {

  val schema = User.getClassSchema

  "Projection" should "work on top-level field" in {
    val s = Projection[User](_.getEmail, _.getAddress)
    s.getFields.size() should be (2)
    s.getField("email") should equal (schema.getField("email"))
    s.getField("address") should equal (schema.getField("address"))
  }

  "Projection" should "work on nested field" in {
    val s1 = Projection[User](_.getAddress.getZip)
    s1.getFields.size() should be (1)

    val s2 = s1.getField("address").schema()
    s2.getFields.size() should be (1)
    s2.getField("zip") should equal(schema.getField("address").schema().getField("zip"))
  }

  "Projection" should "work on array field" in {
    val s = Projection[User](_.getAccounts)
    s.getFields.size() should be (1)
    s.getField("accounts").schema().getElementType should equal(schema.getField("accounts").schema().getElementType)
  }

  "Projection" should "work on nested array field" in {
    val s1 = Projection[User](_.getAccounts.get(0).getAmount)
    s1.getFields.size() should be (1)

    val s2 = s1.getField("accounts").schema().getElementType
    s2.getFields.size() should be (1)
    s2.getField("amount") should equal(schema.getField("accounts").schema().getElementType.getField("amount"))
  }

}
