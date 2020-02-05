import me.lyh.parquet.tensorflow.{ExampleParquetReader, ExampleParquetWriter, Schema}
import org.apache.beam.sdk.extensions.smb.TensorFlowFileOperations
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.example.Example

import scala.collection.JavaConverters._

object Test {
  private val schema = {
    val b = Schema.newBuilder()
    val n = 2
    (1 to n).foreach(i => b.required("long_req_%02d".format(i), Schema.Type.INT64))
    (1 to n).foreach(i => b.optional("long_opt_%02d".format(i), Schema.Type.INT64))
    (1 to n).foreach(i => b.repeated("long_rep_%02d".format(i), Schema.Type.INT64))
    (1 to n).foreach(i => b.required("float_req_%02d".format(i), Schema.Type.FLOAT))
    (1 to n).foreach(i => b.optional("float_opt_%02d".format(i), Schema.Type.FLOAT))
    (1 to n).foreach(i => b.repeated("float_rep_%02d".format(i), Schema.Type.FLOAT))
    (1 to n).foreach(i => b.required("bytes_req_%02d".format(i), Schema.Type.BYTES))
    (1 to n).foreach(i => b.optional("bytes_opt_%02d".format(i), Schema.Type.BYTES))
    (1 to n).foreach(i => b.repeated("bytes_rep_%02d".format(i), Schema.Type.BYTES))
    b.named("Example")
  }

  def main(args: Array[String]): Unit = {
//    pq2tf(in, "examples1m.tfrecords", Compression.UNCOMPRESSED)
//    pq2pq(in, "examples1m.parquet", CompressionCodecName.UNCOMPRESSED)
//    pq2tf(in, "examples1m.gzip.tfrecords", Compression.GZIP)
//    pq2pq(in, "examples1m.gzip.parquet", CompressionCodecName.GZIP)
    readPq("examples1m.parquet")
    readPq("examples1m.snappy.parquet")
    readPq("examples1m.gzip.parquet")

    readPq("examples1m.parquet")
    readPq("examples1m.snappy.parquet")
    readPq("examples1m.gzip.parquet")

    //readTf("examples1m.tfrecords")
    //readTf("examples1m.tfrecords.deflate")
    //readTf("examples1m.tfrecords.gz")

    //readTf("examples1m.tfrecords")
    //readTf("examples1m.tfrecords.deflate")
    //readTf("examples1m.tfrecords.gz")
  }

  private def readPq(in: String): Unit = {
    val reader = ExampleParquetReader.builder(new Path(in)).withSchema(schema).build()
    val start = System.currentTimeMillis()
    var e = reader.read()
    var n = 0
    while (e != null) {
      n += 1
      e = reader.read()
    }
    val t = System.currentTimeMillis() - start
    val rps = n.toDouble * 1000/ t
    println(s"$in\t$n\t$t\t$rps")
  }

  private def readTf(in: String): Unit = {
    val iterator = TensorFlowFileOperations.of(Compression.AUTO)
      .iterator(FileSystems.matchSingleFileSpec(in).resourceId())
    val start = System.currentTimeMillis()
    var n = 0
    while (iterator.hasNext) {
      iterator.next()
      n += 1
    }
    val t = System.currentTimeMillis() - start
    val rps = n.toDouble * 1000/ t
    println(s"$in\t$n\t$t\t$rps")
  }


  private def tf2pq(): Unit = {
    val in = "data_tfrecord-00001-of-02263.gz"
    val out = "data_tfrecord-00001-of-02263.snappy.parquet"
    val comp = CompressionCodecName.SNAPPY
    val iterator = TensorFlowFileOperations.of(Compression.GZIP)
      .iterator(FileSystems.matchSingleFileSpec(in).resourceId())

    val schema = Schema.newBuilder()
      .optional("track_browse_genre", Schema.Type.BYTES)
      .required("isPremium", Schema.Type.BYTES)
      .optional("mode", Schema.Type.BYTES)
      .required("top_type", Schema.Type.BYTES)
      .optional("genre", Schema.Type.BYTES)
      .required("isIncognito", Schema.Type.BYTES)
      .required("platform_type", Schema.Type.BYTES)
      .required("sub_type", Schema.Type.BYTES)
      .required("dayPart", Schema.Type.BYTES)
      .optional("energy", Schema.Type.FLOAT)
      .optional("audio_dis", Schema.Type.FLOAT)
      .optional("loudness", Schema.Type.FLOAT)
      .optional("danceability", Schema.Type.FLOAT)
      .required("rank", Schema.Type.INT64)
      .required("msPlayed", Schema.Type.INT64)
      .optional("organism", Schema.Type.FLOAT)
      .optional("runnability", Schema.Type.FLOAT)
      .optional("duration", Schema.Type.FLOAT)
      .optional("liveness", Schema.Type.FLOAT)
      .optional("key", Schema.Type.INT64)
      .optional("time_signature", Schema.Type.INT64)
      .optional("first_downbeat", Schema.Type.FLOAT)
      .optional("bounciness", Schema.Type.FLOAT)
      .optional("tempo", Schema.Type.FLOAT)
      .optional("dyn_range_mean", Schema.Type.FLOAT)
      .required("total_plays", Schema.Type.INT64)
      .required("cos_sim", Schema.Type.FLOAT)
      .optional("speechiness", Schema.Type.FLOAT)
      .optional("beat_strength", Schema.Type.FLOAT)
      .required("percentile", Schema.Type.FLOAT)
      .required("dis_l2_norm", Schema.Type.FLOAT)
      .optional("user_track_genre_affinity", Schema.Type.FLOAT)
      .optional("acousticness", Schema.Type.FLOAT)
      .optional("mechanism", Schema.Type.FLOAT)
      .required("dis_l1_norm", Schema.Type.FLOAT)
      .required("popularity_normalized", Schema.Type.FLOAT)
      .optional("instrumentalness", Schema.Type.FLOAT)
      .optional("intro_score", Schema.Type.FLOAT)
      .optional("flatness", Schema.Type.FLOAT)
      .optional("valence", Schema.Type.FLOAT)
      .named("Data")

    val writer = ExampleParquetWriter.builder(new Path(out))
      .withSchema(schema)
      .withCompressionCodec(comp)
      .build()

    var i = 0
    iterator.forEachRemaining { e =>
      writer.write(e)
      i += 1
      if (i % 1000 == 0) {
        println(i)
      }
    }
    writer.close()
  }

  private def pq2pq(in: String, out: String, comp: CompressionCodecName): Unit = {
    val reader = ExampleParquetReader.builder(new Path(in)).withSchema(schema).build()
    val writer = ExampleParquetWriter.builder(new Path(out))
      .withSchema(schema)
      .withCompressionCodec(comp)
      .build()
    var e = reader.read()
    var i = 0
    while (e != null) {
      writer.write(e)
      i += 1
      if (i % 100000 == 0) {
        println(i)
      }
      e = reader.read()
    }
    reader.close()
    writer.close()
  }

  private def pq2tf(in: String, out: String, comp: Compression): Unit = {
    val reader = ExampleParquetReader.builder(new Path(in)).withSchema(schema).build()
    val writer = TensorFlowFileOperations.of(comp)
      .createWriter(FileSystems.matchNewResource(out, false))
    var e = reader.read()
    var i = 0
    while (e != null) {
      writer.write(e)
      i += 1
      if (i % 100000 == 0) {
        println(i)
      }
      e = reader.read()
    }
    reader.close()
    writer.close()
  }
}
