package me.lyh.parquet.tensorflow;

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.tensorflow.proto.example.Example;

import java.util.Collection;

public class ExampleParquetInputFormat extends ParquetInputFormat<Example> {
  public static final String SCHEMA_KEY = "parquet.tensorflow.example.input.schema";
  public static final String FIELDS_KEY = "parquet.tensorflow.example.input.fields";

  public static void setSchema(Job job, Schema schema) {
    Preconditions.checkState(
        job.getConfiguration().get(FIELDS_KEY) == null,
        "Only one of [%s, %s] can be set", SCHEMA_KEY, FIELDS_KEY
    );
    setReadSupportClass(job, ExampleReadSupport.class);
    job.getConfiguration().set(SCHEMA_KEY, schema.toParquet().toString());
  }

  public static void setFields(Job job, Collection<String> fields) {
    Preconditions.checkState(
        job.getConfiguration().get(SCHEMA_KEY) == null,
        "Only one of [%s, %s] can be set", SCHEMA_KEY, FIELDS_KEY
    );
    setReadSupportClass(job, ExampleReadSupport.class);
    job.getConfiguration().set(FIELDS_KEY, String.join(",", fields));
  }
}
