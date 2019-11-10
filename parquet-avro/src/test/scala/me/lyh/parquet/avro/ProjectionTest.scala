package me.lyh.parquet.avro

import me.lyh.parquet.avro.schema.User
import org.scalatest._

class ProjectionTest extends FlatSpec with Matchers {
  val schema = User.getClassSchema

  "Projection.apply(g: (T => Any)*)" should "work on top-level field" in {
    val s = Projection[User](_.getEmail, _.getAddress)
    s.getFields.size() shouldBe 2
    s.getField("email") shouldEqual schema.getField("email")
    s.getField("address") shouldEqual schema.getField("address")
  }

  it should "work on nested field" in {
    val s1 = Projection[User](_.getAddress.getZip)
    s1.getFields.size() shouldBe 1

    val s2 = s1.getField("address").schema()
    s2.getFields.size() shouldBe 1
    s2.getField("zip") shouldEqual schema.getField("address").schema().getField("zip")
  }

  it should "work on array field" in {
    val s = Projection[User](_.getAccounts)
    s.getFields.size() shouldBe 1
    s.getField("accounts") shouldEqual schema.getField("accounts")
  }

  it should "work on nested array field" in {
    val s1 = Projection[User](_.getAccounts.get(0).getAmount)
    s1.getFields.size() shouldBe 1

    val s2 = s1.getField("accounts").schema().getElementType
    s2.getFields.size() shouldBe 1
    s2.getField("amount") shouldEqual schema
      .getField("accounts")
      .schema()
      .getElementType
      .getField("amount")
  }
}
