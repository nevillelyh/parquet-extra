package parquet.avro.examples

import parquet.avro.Predicate
import parquet.avro.Projection
import parquet.avro.schema.User

object ExampleApp extends App {
  val s = Projection[User](
    _.getEmail,
    _.getAddress.getZip,
    _.getAccounts.get(0).getAmount
  )
  println(s.toString(true))

  println(Predicate[User](_.getAccounts.get(0).getAmount > 10))
  println(Predicate[User](10 < _.getAccounts.get(0).getAmount))
  println(Predicate[User](_.getAccounts.get(0).getAmount >= 10))
  println(Predicate[User](10 <= _.getAccounts.get(0).getAmount))

  println(Predicate[User](_.getAccounts.get(0).getAmount == 10))
  println(Predicate[User](_.getAccounts.get(0).getAmount != 10))

  println(Predicate[User](_.getEmail == "neville@spotify.com"))

  println(Predicate[User](x => !(x.getAccounts.get(0).getAmount > 10)))
  println(Predicate[User](x => x.getId > 10 && x.getId < 100))
  println(Predicate[User](x => !(x.getAccounts.get(0).getAmount > 10) && !(x.getId > 100)))
  println(Predicate[User](x => !(x.getAccounts.get(0).getAmount > 10 || x.getId > 100)))
}
