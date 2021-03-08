package me.lyh.parquet.carreleur

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page.{DataPage, DataPageV1, DataPageV2}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetOutputFormat, ParquetWriter}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}

import scala.jdk.CollectionConverters._

object Test {
  def main(args: Array[String]): Unit = {
    val DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;

    val inPath = new Path(args(0))
    val outPath = new Path("temp.parquet")
    val conf = new Configuration()

    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(inPath, conf))
    val fileMeta = reader.getFooter.getFileMetaData
    println("# CREATED BY: " + fileMeta.getCreatedBy)
    val schema = fileMeta.getSchema
    println("# SCHEMA: " + schema.getName)
    println("# METADATA: " + fileMeta.getKeyValueMetaData.asScala.keys)

    val rowGroupSize = conf.getInt(ParquetOutputFormat.BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
    val writer = new ParquetFileWriter(
      HadoopOutputFile.fromPath(outPath, conf), schema, Mode.OVERWRITE, rowGroupSize,
      ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
      ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
      ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH,
      ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED)
    writer.start()

    val blocks = reader.getFooter.getBlocks.asScala
    blocks.foreach { block =>
      writer.startBlock(block.getRowCount)
      println("=" * 40)
      println("# BLOCK COMPRESSED SIZE: " + block.getCompressedSize)
      println("# BLOCK STARTING POS: " + block.getStartingPos)
      println("# BLOCK TOTAL BYTE SIZE: " + block.getTotalByteSize)
      println("# BLOCK ROW COUNT: " + block.getRowCount)
      val rowGroup = reader.readNextRowGroup()
      val pages = Array.fill[DataPage](block.getColumns.size())(null)
      block.getColumns.asScala.zipWithIndex.foreach { case (column, i) =>
        println("# COLUMN: " + column.getPath.toDotString)
        val columnDescriptor = schema.getColumnDescription(column.getPath.toArray)
        val pageReader = rowGroup.getPageReader(columnDescriptor)


        pageReader.readPage().accept(new Visitor[Unit] {
          override def visit(dataPageV1: DataPageV1): Unit = {
            println("V1")
//            println(dataPageV1.getBytes.size())
//            println(dataPageV1.getDlEncoding)
//            println(dataPageV1.getRlEncoding)
//            println(dataPageV1.getStatistics)
//            println(dataPageV1.getCompressedSize)
            pages(i) = dataPageV1
            ()
          }

          override def visit(dataPageV2: DataPageV2): Unit = {
            println("V2")
            pages(i) = dataPageV2
            ()
          }
        })
      }

      block.getColumns.asScala.zip(pages).foreach { case (column, page) =>
        val columnDescriptor = schema.getColumnDescription(column.getPath.toArray)
        writer.startColumn(columnDescriptor, column.getValueCount, column.getCodec)
        page match {
          case p: DataPageV1 =>
            writer.writeDataPage(
              column.getValueCount.toInt,
              column.getTotalUncompressedSize.toInt,
              p.getBytes,
              p.getStatistics,
              block.getRowCount,
              p.getRlEncoding,
              p.getDlEncoding,
              p.getValueEncoding
            )
          case v: DataPageV2 =>
          case _ =>
        }
        writer.endColumn()
      }


      writer.endBlock()
    }

    writer.end(fileMeta.getKeyValueMetaData)
  }
}
