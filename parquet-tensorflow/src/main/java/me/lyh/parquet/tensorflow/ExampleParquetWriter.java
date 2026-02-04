package me.lyh.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.tensorflow.example.Example;

public class ExampleParquetWriter {
  private ExampleParquetWriter() {}

  public static Builder builder(Path path) {
    return new Builder(path);
  }

  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  public static class Builder extends ParquetWriter.Builder<Example, Builder> {
    private Schema schema;

    protected Builder(Path path) {
      super(path);
    }

    protected Builder(OutputFile file) {
      super(file);
    }

    @Override
    protected Builder self() {
      return this;
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    @Override
    protected WriteSupport<Example> getWriteSupport(Configuration conf) {
      if (schema == null) {
        String schemaString = conf.get(ExampleParquetOutputFormat.SCHEMA_KEY);
        MessageType parquet = MessageTypeParser.parseMessageType(schemaString);
        schema = Schema.fromParquet(parquet);
      }
      return new ExampleWriteSupport(schema);
    }
  }
}
