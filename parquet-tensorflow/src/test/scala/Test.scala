import com.google.protobuf.ByteString
import me.lyh.parquet.tensorflow.{ExampleParquetOutputFormat, ExampleParquetWriter, Schema}
import org.apache.beam.sdk.extensions.smb.TensorFlowFileOperations
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.example.{BytesList, Example, Feature, Features, FloatList, Int64List}

object Test {

  private def longs(xs: Long*) =
    Feature
      .newBuilder()
      .setInt64List(
        xs.foldLeft(Int64List.newBuilder())(_.addValue(_))
      )
      .build()

  private def floats(xs: Float*) =
    Feature
      .newBuilder()
      .setFloatList(
        xs.foldLeft(FloatList.newBuilder())(_.addValue(_))
      )
      .build()

  private def bytes(xs: String*) =
    Feature
      .newBuilder()
      .setBytesList(
        xs.foldLeft(BytesList.newBuilder())((b, x) => b.addValue(ByteString.copyFromUtf8(x)))
      )
      .build()

  private val n = 50

  private val schema = {
    //    val n = 5
    var b = Schema.newBuilder()
    (1 to n).foreach(i => b = b.required(s"long_req_0$i", Schema.Type.INT64))
    (1 to n).foreach(i => b = b.optional(s"long_opt_0$i", Schema.Type.INT64))
//    (1 to n).foreach(i => b = b.repeated(s"long_rep_0$i", Schema.Type.INT64))
    (1 to n).foreach(i => b = b.required(s"float_req_0$i", Schema.Type.FLOAT))
    (1 to n).foreach(i => b = b.optional(s"float_opt_0$i", Schema.Type.FLOAT))
//    (1 to n).foreach(i => b = b.repeated(s"float_rep_0$i", Schema.Type.FLOAT))
    (1 to n).foreach(i => b = b.required(s"bytes_req_0$i", Schema.Type.BYTES))
    (1 to n).foreach(i => b = b.optional(s"bytes_opt_0$i", Schema.Type.BYTES))
//    (1 to n).foreach(i => b = b.repeated(s"bytes_rep_0$i", Schema.Type.BYTES))
    b.named("Example")
  }

  private def record(x: Int) = {
    var b = Features.newBuilder()
    (1 to n).foreach(i => b = b.putFeature(s"long_req_0$i", longs(x * i)))
    if (x % 2 == 0) {
      (1 to n).foreach(i => b = b.putFeature(s"long_opt_0$i", longs(x * i)))
    }
//    (1 to n).foreach(i => b = b.putFeature(s"long_rep_0$i", longs((1 to 100).map(_ + x.toLong): _*)))

    (1 to n).foreach(i => b = b.putFeature(s"float_req_0$i", floats(x * i)))
    if (x % 2 == 0) {
      (1 to n).foreach(i => b = b.putFeature(s"float_opt_0$i", floats(x * i)))
    }
//    (1 to n).foreach(i => b = b.putFeature(s"float_rep_0$i", floats((1 to 100).map(_ + x.toFloat): _*)))

    (1 to n).foreach(i => b = b.putFeature(s"bytes_req_0$i", bytes("req%012d".format(x * i))))
    if (x % 2 == 0) {
      (1 to n).foreach(i => b = b.putFeature(s"bytes_opt_0$i", bytes("opt%012d".format(x * i))))
    }
//    (1 to n).foreach(i => b = b.putFeature(
//      s"bytes_rep_0$i", bytes((1 to i * 10).map(j => "rep%012d".format(j + x)): _*)))

    Example.newBuilder().setFeatures(b).build()
  }

  def main(args: Array[String]): Unit = {
//    writePq("examples2.1m.parquet", CompressionCodecName.UNCOMPRESSED)
    writePq("examples2.1m.snappy.parquet", CompressionCodecName.SNAPPY)
    writePq("examples2.1m.gzip.parquet", CompressionCodecName.GZIP)
    writeTf("examples2.1m.tfrecord", Compression.UNCOMPRESSED)
    writeTf("examples2.1m.tfrecord.deflate", Compression.DEFLATE)
    writeTf("examples2.1m.tfrecord.gzip", Compression.GZIP)
  }

  private def writePq(path: String, codec: CompressionCodecName): Unit = {
    val outputPath = new Path(path)
    val writer = ExampleParquetWriter.builder(outputPath)
      .withSchema(schema)
      .withCompressionCodec(codec)
      .build()

    val m = 1000000
    var i = 0
    while (i < 1 * m) {
      writer.write(record(i))
      i += 1
      if (i % 100000 == 0) {
        println(i)
      }
    }
    writer.close()
  }

  private def writeTf(path: String, compression: Compression): Unit = {
    val writer = TensorFlowFileOperations
      .of(compression)
      .createWriter(FileSystems.matchNewResource(path, false))

    val m = 1000000
    var i = 0
    while (i < 1 * m) {
      writer.write(record(i))
      i += 1
      if (i % 100000 == 0) {
        println(i)
      }
    }
    writer.close()
  }
}
