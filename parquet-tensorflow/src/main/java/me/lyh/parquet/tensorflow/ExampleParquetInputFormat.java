package me.lyh.parquet.tensorflow;

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.tensorflow.example.Example;

public class ExampleParquetInputFormat extends ParquetInputFormat<Example> {
  public static final String SCHEMA_KEY = "parquet.tensorflow.example.input.schema";

  public static void setSchema(Job job, Schema schema) {
    setReadSupportClass(job, ExampleReadSupport.class);
    job.getConfiguration().set(SCHEMA_KEY, schema.toParquet().toString());
  }
}
