package me.lyh.tfpq

import java.net.URI
import java.nio.channels.Channels

import caseapp._
import me.lyh.parquet.tensorflow.{ExampleParquetWriter, Schema}
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory
import org.tensorflow.example.{Example, Feature}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.control.NonFatal

@ProgName("tfpq")
case class Options(
  @ExtraName("i")
  @HelpMessage("input file")
  input: String,

  @ExtraName("o")
  @HelpMessage("output file")
  output: String,

  @ExtraName("l")
  @HelpMessage("number of records to convert, default is all")
  limit: Option[Long],

  @ExtraName("s")
  @HelpMessage(
    """schema file, a CSV in the form of [feature name,type,repetition],
      |        where type is one of [INT64,FLOAT,BYTES]
      |        and repetition is one of [REQUIRED,OPTIONAL,REPEATED].
      |""".stripMargin)
  schema: Option[String],

  @HelpMessage(
    """number of samples when inferring schema, if schema file is not specified
      |        default is all
      |""".stripMargin)
  samples: Option[Long],

  @ExtraName("C")
  @HelpMessage("Compression of input file, one of [AUTO,UNCOMPRESSED,DEFLATE,GZIP], default is AUTO")
  inputCompression: String = "AUTO",

  @ExtraName("c")
  @HelpMessage("Compression of output file, one of [UNCOMPRESSED,SNAPPY,GZIP], default is SNAPPY")
  outputCompression: String = "SNAPPY")

object Main extends CaseApp[Options] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(opts: Options, args: RemainingArgs): Unit = {
    val schema = getSchema(opts)
    val reader = new Reader(opts.input, opts.inputCompression)
    val writer = new Writer(opts.output, opts.outputCompression, schema)

    val limit = opts.limit match {
      case None => Long.MaxValue
      case Some(l) => l
    }
    require(limit > 0, s"limit must be > 0, got $limit")

    val start = System.currentTimeMillis()
    var n = 0
    var example = reader.read()
    while (example != null && n < limit) {
      writer.write(example)
      n += 1
      if (n % 100000 == 0) {
        val t = System.currentTimeMillis() - start
        val rps = n / (t / 1000.0)
        logger.info("{} records converted at {} record/sec", n, rps)
      }
      example = reader.read()
    }
    reader.close()
    writer.close()
    val duration = (System.currentTimeMillis() - start) / 1000.0
    val rps = n / duration
    logger.info("{} records converted in {} sec at {} record/sec", n, duration, rps)
  }

  private def getSchema(opts: Options): Schema = (opts.schema, opts.samples) match {
    case (None, None) => inferSchema(opts.input, opts.inputCompression, Long.MaxValue)
    case (Some(schema), None) => parseSchema(schema)
    case (None, Some(n)) =>inferSchema(opts.input, opts.inputCompression, n)
    case (Some(_), Some(_)) =>
      throw new IllegalArgumentException(s"Only one of schema and samples can be specified")
  }

  private def inferSchema(path: String, compression: String, samples: Long): Schema = {
    require(samples > 0, s"samples must be > 0, got $samples")
    logger.info("Inferring schema from {}", path)

    def getTypeAndCount(feature: Feature): Option[(Schema.Type, Int)] =
      (feature.hasInt64List, feature.hasFloatList, feature.hasBytesList) match {
        case (true, false, false) =>
          Some((Schema.Type.INT64, feature.getInt64List.getValueCount))
        case (false, true, false) =>
          Some((Schema.Type.FLOAT, feature.getFloatList.getValueCount))
        case (false, false, true) =>
          Some((Schema.Type.BYTES, feature.getBytesList.getValueCount))
        case (false, false, false) => None
        case _ => throw new IllegalStateException(s"Malformed Feature $feature")
      }

    val m = collection.mutable.Map.empty[String, (Long, Schema.Type, Int)]
    var n = 0L
    val reader = new Reader(path, compression)
    var example = reader.read()
    while (example != null && n < samples) {
      example.getFeatures.getFeatureMap.asScala.foreach { case (name, feature) =>
        getTypeAndCount(feature) match {
          case None =>
          case Some((tpe, count)) =>
            m.get(name) match {
              case None => m.put(name, (1, tpe, count))
              case Some((freq, t, max)) =>
                require(t == tpe, s"Conflicting types for feature $name: $t, $tpe")
                m(name) = (freq + 1, t, math.max(max, count))
            }
        }
      }
      n += 1
      example = reader.read()
    }
    reader.close()

    var builder = Schema.newBuilder()
    m.foreach { case (name, (freq, tpe, max)) =>
      val min = if (freq == n) 1 else 0
      builder = if (min == 1 && max == 1) {
        builder.required(name, tpe)
      } else if (min == 0 && max == 1) {
        builder.optional(name, tpe)
      } else {
        builder.repeated(name, tpe)
      }
    }
    val schema = builder.named("Examples")

    logger.info(s"Inferred schema: {}", schema.toParquet)
    schema
  }

  private def parseSchema(path: String): Schema = Source
    .fromInputStream(FileUtils.open(path))
    .getLines()
    .foldLeft(Schema.newBuilder()) { (builder, line) =>
      val t = line.split(',')
      try {
        val name = t(0).trim
        val tpe = t(1).trim.toUpperCase match {
          case "INT64" => Schema.Type.INT64
          case "FLOAT" => Schema.Type.FLOAT
          case "BYTES" => Schema.Type.BYTES
        }
        t(2).trim.toUpperCase match {
          case "REQUIRED" => builder.required(name, tpe)
          case "OPTIONAL" => builder.optional(name, tpe)
          case "REPEATED" => builder.repeated(name, tpe)
        }
      } catch {
        case NonFatal(_) =>
          throw new IllegalArgumentException(s"Malformed schema $line")
      }
    }
    .named("Example")
}

private class Reader(path: String, compression: String) {
  private def normalizeCompression(path: String, compression: String): String =
    compression.toUpperCase match {
      case "AUTO" =>
        path.split('.').last.toLowerCase match {
          case "deflate" => "DEFLATE"
          case "gz" | "gzip" => "GZIP"
          case _ =>
            throw new IllegalArgumentException(s"Failed to detect input compression for file $path")
        }
      case c => c
    }

  private val channel = {
    val raw = FileUtils.open(path)
    val in = normalizeCompression(path, compression) match {
      case "UNCOMPRESSED" => raw
      case "DEFLATE" => new DeflateCompressorInputStream(raw)
      case "GZIP" => new GzipCompressorInputStream(raw)
      case _ => throw new IllegalArgumentException(s"Unsupported input compression $compression")
    }
    Channels.newChannel(in)
  }

  private val codec = new TFRecordCodec

  def read(): Example = codec.read(channel) match {
    case null => null
    case b => Example.parseFrom(b)
  }

  def close(): Unit = channel.close()
}

private class Writer(path: String, compression: String, schema: Schema) {
  private val writer = ExampleParquetWriter.builder(new Path(path))
    .withSchema(schema)
    .withCompressionCodec(normalizeCompression(compression))
    .build()

  private def normalizeCompression(compression: String) = compression.toUpperCase match {
    case "UNCOMPRESSED" => CompressionCodecName.UNCOMPRESSED
    case "SNAPPY" => CompressionCodecName.SNAPPY
    case "GZIP" => CompressionCodecName.GZIP
    case _ => throw new IllegalArgumentException(s"Unsupported output compression $compression")
  }

  def write(example: Example): Unit = writer.write(example)

  def close(): Unit = writer.close()
}

private object FileUtils {
  def open(path: String): FSDataInputStream =
    FileSystem.get(new URI(path), Job.getInstance().getConfiguration).open(new Path(path))
}
