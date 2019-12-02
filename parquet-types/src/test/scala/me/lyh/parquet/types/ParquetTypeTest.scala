package me.lyh.parquet.types

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ParquetDecodingException
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetTypeTest extends AnyFlatSpec with Matchers {
  private val fs = FileSystem.getLocal(new Configuration())

  private def makeTemp: Path = {
    val tmp = sys.props("java.io.tmpdir")
    val ts = System.currentTimeMillis()
    val p = new Path(s"$tmp/parquet-type-$ts.parquet")
    fs.deleteOnExit(p)
    p
  }

  private def write[T: ParquetType](path: Path, xs: Seq[T]): Unit = {
    val writer = TypeParquetWriter.builder[T](path).build()
    xs.foreach(writer.write)
    writer.close()
  }

  private def read[T: ParquetType](path: Path): Seq[T] = {
    val reader = TypeParquetReader.builder[T](path).build()
    val b = Seq.newBuilder[T]
    var r = reader.read()
    while (r != null) {
      b += r
      r = reader.read()
    }
    b.result()
  }

  private def roundTrip[T: ParquetType](xs: Seq[T]): Seq[T] = {
    val temp = makeTemp
    write(temp, xs)
    read(temp)
  }

  private def testException(fun: => Any, msgs: String*) = {
    val e = the[ParquetDecodingException] thrownBy fun
    msgs.foreach(m => e.getCause.getMessage should include(m))
  }

  private def optInt(i: Int) = if (i % 2 == 0) Some(i) else None

  "ParuqetType" should "work with Hadoop" in {
    val temp = makeTemp
    val xs = (0 until 10)
      .map(i => Primitives(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble, i.toString))
    val job = Job.getInstance()

    job.setOutputFormatClass(classOf[TypeParquetOutputFormat[Primitives]])
    TypeParquetOutputFormat.setParquetType(job, ParquetType[Primitives])
    val outputFormat = new TypeParquetOutputFormat[Primitives]()
    val writer = outputFormat.getRecordWriter(job.getConfiguration, temp, CompressionCodecName.GZIP)
    xs.foreach(writer.write(null, _))
    writer.close(null)

    job.setInputFormatClass(classOf[TypeParquetInputFormat[Primitives]])
    TypeParquetInputFormat.setParquetType(job, ParquetType[Primitives])
    val inputFormat = new TypeParquetInputFormat[Primitives]()
    val context = new TaskAttemptContextImpl(job.getConfiguration, new TaskAttemptID())
    FileInputFormat.setInputPaths(job, temp)
    val split = inputFormat.getSplits(job).get(0)
    val reader = inputFormat.createRecordReader(split, context)
    reader.initialize(split, context)
    val b = Seq.newBuilder[Primitives]
    while (reader.nextKeyValue()) {
      b += reader.getCurrentValue
    }
    reader.close()
    b.result() shouldEqual xs
  }

  it should "round trip primitives" in {
    val xs = (0 until 10)
      .map(i => Primitives(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble, i.toString))
    roundTrip(xs) shouldEqual xs
  }

  it should "round trip primitives without equals" in {
    val xs = (0 until 10).map(i => NonEqs(i.toString.getBytes(), i.toString))
    roundTrip(xs).map(t => (t.ba.toList, t.cs.toString)) shouldEqual
      xs.map(t => (t.ba.toList, t.cs.toString))
  }

  it should "round trip repetition" in {
    val xs = (0 until 10).map(i => Repetition(optInt(i), List.fill(i)(i)))
    roundTrip(xs) shouldEqual xs
  }

  it should "support collections" in {
    val xs = (0 until 10).map(
      i =>
        Collections(
          Array.fill(i)(i),
          Iterable.fill(i)(i),
          Seq.fill(i)(i),
          IndexedSeq.fill(i)(i),
          List.fill(i)(i),
          Vector.fill(i)(i)
        )
    )
    val copy = roundTrip(xs)
    copy.map(_.copy(a = null)) shouldEqual xs.map(_.copy(a = null))
    copy.map(_.a.toList) shouldEqual xs.map(_.a.toList)
  }

  it should "round trip nested" in {
    val xs = (0 until 10).map { i =>
      val i1 = Inner(i, optInt(i), List.fill(i)(i))
      val i2 = if (i % 3 == 0) Some(i1) else None
      val i3 = List.fill(i)(i1)
      Outer(i1, i2, i3)
    }
    roundTrip(xs) shouldEqual xs
  }

  it should "round trip primitive projection" in {
    val xs = (0 until 10)
      .map(i => Primitives(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble, i.toString))
    val temp = makeTemp
    write(temp, xs)
    read[Primitives2](temp).map { x =>
      Primitives(x.b, x.i, x.l, x.f, x.d, x.s)
    } shouldEqual xs
    read[Primitives3](temp) shouldEqual xs.map(x => Primitives3(x.s, x.i))
  }

  it should "round trip repetition projection" in {
    val xs = (0 until 10).map(i => Repetition(optInt(i), List.fill(i)(i)))
    val temp = makeTemp
    write(temp, xs)
    read[Repetition2](temp).map(x => Repetition(x.o, x.l)) shouldEqual xs
    read[Repetition3](temp).map(_.o) shouldEqual xs.map(_.o)
    read[Repetition4](temp).map(_.l) shouldEqual xs.map(_.l)
  }

  it should "round trip nested projection" in {
    val xs = (0 until 10).map { i =>
      val i1 = Inner(i, optInt(i), List.fill(i)(i))
      val i2 = if (i % 3 == 0) Some(i1) else None
      val i3 = List.fill(i)(i1)
      Outer(i1, i2, i3)
    }
    val temp = makeTemp
    write(temp, xs)
    read[Outer1](temp) shouldEqual xs.map(x => Outer1(Inner1(x.r.r), x.o, x.l))
    read[Outer2](temp) shouldEqual xs.map(x => Outer2(x.r, x.o.map(i => Inner2(i.o)), x.l))
    read[Outer3](temp) shouldEqual xs.map(x => Outer3(x.r, x.o, x.l.map(i => Inner3(i.l))))
  }

  it should "support schema evolution" in {
    val xs = (0 until 10).map(i => W(i, optInt(i), List.fill(i)(i)))
    val temp = makeTemp
    write(temp, xs)

    // narrowing repetition
    testException(
      read[R1](temp),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: required int32 o (INT_32) != optional int32 o (INT_32)"
    )
    testException(
      read[R2](temp),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: optional int32 l (INT_32) != repeated int32 l (INT_32)"
    )

    // widening repetition
    read[R3](temp) shouldEqual xs.map(x => R3(Some(x.r)))
    read[R4](temp) shouldEqual xs.map(x => R4(x.o.toList))

    // new fields
    testException(
      read[R5](temp),
      "Failed to decode me.lyh.parquet.types.R5#x:",
      "requirement failed: Required field size != 1: 0"
    )
    read[R6](temp) shouldEqual xs.map(x => R6(x.r, None))
    read[R7](temp) shouldEqual xs.map(x => R7(x.r, Nil))

    // incompatible fields
    testException(
      read[R8](temp),
      "The requested schema is not compatible with the file schema.",
      "incompatible types: required binary r (UTF8) != required int32 r (INT_32)"
    )
    val schema = Schema.rename(ParquetType[Inner].schema, "r")
    testException(
      read[R9](temp),
      "The requested schema is not compatible with the file schema.",
      s"incompatible types: $schema != required int32 r (INT_32)"
    )
  }
}

case class Primitives(b: Boolean, i: Int, l: Long, f: Float, d: Double, s: String)
case class NonEqs(ba: Array[Byte], cs: CharSequence)
case class Repetition(o: Option[Int], l: List[Int])
case class Collections(
  a: Array[Int],
  i: Iterable[Int],
  s: Seq[Int],
  is: IndexedSeq[Int],
  l: List[Int],
  v: Vector[Int]
)

case class Inner(r: Int, o: Option[Int], l: List[Int])
case class Outer(r: Inner, o: Option[Inner], l: List[Inner])

case class Primitives2(s: String, b: Boolean, f: Float, d: Double, i: Int, l: Long)
case class Primitives3(s: String, i: Int)
case class Repetition2(l: List[Int], o: Option[Int])
case class Repetition3(o: Option[Int])
case class Repetition4(l: List[Int])

case class Inner1(r: Int)
case class Inner2(o: Option[Int])
case class Inner3(l: List[Int])
case class Outer1(r: Inner1, o: Option[Inner], l: List[Inner])
case class Outer2(r: Inner, o: Option[Inner2], l: List[Inner])
case class Outer3(r: Inner, o: Option[Inner], l: List[Inner3])

case class W(r: Int, o: Option[Int], l: List[Int])
case class R1(o: Int)
case class R2(l: Option[Int])
case class R3(r: Option[Int])
case class R4(o: List[Int])
case class R5(r: Int, x: Int)
case class R6(r: Int, x: Option[Int])
case class R7(r: Int, x: List[Int])
case class R8(r: String)
case class R9(r: Inner)
