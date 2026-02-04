package me.lyh.parquet.tensorflow;

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.tensorflow.example.Example;

public class ExampleParquetOutputFormat extends ParquetOutputFormat<Example> {
  public static final String SCHEMA_KEY = "parquet.tensorflow.example.output.schema";

  public static void setSchema(Job job, Schema schema) {
    setWriteSupportClass(job, ExampleWriteSupport.class);
    job.getConfiguration().set(SCHEMA_KEY, schema.toParquet().toString());
  }
}
