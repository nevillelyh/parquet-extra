package parquet.avro.examples

import parquet.avro.Projection
import parquet.avro.schema.User

object ProjectionExample extends App {
  val s = Projection[User](
    _.getEmail,
    _.getAddress.getZip,
    _.getAccounts.get(0).getAmount
  )
  println(s.toString(true))
}
