package me.lyh.parquet.tensorflow;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.tensorflow.example.Example;

public class ExampleParquetReader {
  private ExampleParquetReader() {}

  public static Builder builder(Path path) {
    return new Builder(path);
  }

  public static Builder builder(InputFile file) {
    return new Builder(file);
  }

  public static class Builder extends ParquetReader.Builder<Example> {
    private Schema schema;

    protected Builder(Path path) {
      super(path);
    }

    protected Builder(InputFile file) {
      super(file);
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    @Override
    protected ReadSupport<Example> getReadSupport() {
      Preconditions.checkNotNull(schema, "schema");
      return new ExampleReadSupport(schema);
    }
  }
}
